//! Provides the server and handles the incoming requests
//! All ports are handled by the same function

// External modules:
use mysql;
use mysql::{OptsBuilder, Pool, Value, QueryResult};
use chrono::Local;

// System modules:
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::spawn;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::io;
use time;
use std::process;
use std::fs::File;

// Internal modules:
use configuration::{Configuration, HEADER_LENGTH};
use data_parser::{parse_text_data, parse_binary_data, StationDataType};

quick_error! {
    #[derive(Debug)]
    pub enum StoreDataError {
        IOError(error: io::Error) { from() }
        MySQLError(error: mysql::Error) { from() }
        TimeParseError(error: time::ParseError) { from() }
    }
}

pub fn init_db(config: &Configuration) -> Pool {
    let mut db_builder = OptsBuilder::new();
    db_builder.ip_or_hostname(Some(config.hostname.as_ref()))
           .db_name(Some(config.db_name.as_ref()))
           .user(Some(config.username.as_ref()))
           .pass(Some(config.password.as_ref()));
    match Pool::new(db_builder) {
        Ok(db_pool) => db_pool,
        Err(e) => {
            info!("Database error: {}", e);
            process::exit(1);
        }
    }
}

pub fn store_to_db<'a>(db_pool: &Pool, station_name: &str, data: &StationDataType) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    let datetime_format = "%Y-%m-%d %H:%M:%S";

    match data {
        &StationDataType::SimpleData(timestamp_tm, voltage1, voltage2, wind_diag) => {
            let timestamp = timestamp_tm.strftime(&datetime_format)?;
            let result = db_pool.prep_exec("INSERT INTO battery_data (
                      timestamp,
                      station,
                      battery_voltage,
                      li_battery_voltage,
                      wind_dir
                   ) VALUES (
                      :timestamp,
                      :station,
                      :battery_voltage,
                      :li_battery_voltage,
                      :wind_dir
                   )", (
                   Value::from(timestamp.to_string()),
                   Value::from(station_name),
                   Value::from(voltage1),
                   Value::from(voltage2),
                   Value::from(wind_diag)
               ))?;
            return Ok(Some(result));
        },
        &StationDataType::MultipleData(ref full_data_set) => {
            let timestamp = full_data_set.timestamp.strftime(&datetime_format)?;
            let result = db_pool.prep_exec("INSERT INTO multiple_data (
                    timestamp,
                    station,
                    air_temperature,
                    air_relative_humidity,
                    solar_radiation,
                    soil_water_content,
                    soil_temperature,
                    wind_speed,
                    wind_max,
                    wind_direction,
                    precipitation,
                    air_pressure
                ) VALUES (
                    :timestamp,
                    :station,
                    :air_temperature,
                    :air_relative_humidity,
                    :solar_radiation,
                    :soil_water_content,
                    :soil_temperature,
                    :wind_speed,
                    :wind_max,
                    :wind_direction,
                    :precipitation,
                    :air_pressure
                )", (
                    Value::from(timestamp.to_string()),
                    Value::from(station_name),
                    Value::from(full_data_set.air_temperature),
                    Value::from(full_data_set.air_relative_humidity),
                    Value::from(full_data_set.solar_radiation),
                    Value::from(full_data_set.soil_water_content),
                    Value::from(full_data_set.soil_temperature),
                    Value::from(full_data_set.wind_speed),
                    Value::from(full_data_set.wind_max),
                    Value::from(full_data_set.wind_direction),
                    Value::from(full_data_set.precipitation),
                    Value::from(full_data_set.air_pressure)
                ))?;
            return Ok(Some(result));
        }
    }
}

fn port_to_station(port: u16) -> String{
    match port {
        2100 => "Nahuelbuta".to_string(),
        2101 => "Santa_Gracia".to_string(),
        2102 => "Pan_de_Azucar".to_string(),
        2103 => "La_Campana".to_string(),
        2104 => "Wanne_Tuebingen".to_string(),
        2001 => "test1".to_string(),
        2200 => "test2".to_string(),
        _ => "unknown".to_string()
    }
}

fn handle_client<'a>(stream: &mut TcpStream, remote_addr: &SocketAddr,
    db_pool: &Arc<Mutex<Pool>>) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    let date_today = Local::now().format("%Y_%m_%d").to_string();
    info!("Date: {}", date_today);
    info!("Client socket address: {}", remote_addr);

    let local_addr = stream.local_addr()?;

    let local_port = match local_addr {
        SocketAddr::V4(local_addr) => local_addr.port(),
        SocketAddr::V6(local_addr) => local_addr.port()
    };

    info!("Port: {}", local_port);

    let mut tcp_buffer = Vec::new();

    let len = stream.read_to_end(&mut tcp_buffer)?;
    info!("[{}] Number of bytes received: {}", local_port, len);

    // Write received binary data to disk
    let station_name = port_to_station(local_port);
    let binary_filename = if len < 100 {
        format!("old/binary/{}_small_{}.dat", station_name, date_today)
    } else {
        format!("old/binary/{}_full_{}.dat", station_name, date_today)
    };

    info!("write binary file to: {}", binary_filename);

    {
        // Close file after this block
        let mut binary_file = File::create(binary_filename)?;
        binary_file.write(&tcp_buffer)?;
    }

    if tcp_buffer.len() > HEADER_LENGTH {

        let (_, buffer_right) = tcp_buffer.split_at(HEADER_LENGTH);

        // let str_header = String::from_utf8_lossy(buffer_left);
        // let str_data = String::from_utf8_lossy(buffer_right);

        // info!("Header: {:?}", buffer_left);
        info!("[{}] Data: {:?}", local_port, buffer_right);

        // info!("Header (ASCII) ({}): '{}'", &station_name, str_header);
        // info!("Data (ASCII) ({}): '{}'", &station_name, str_data);

        // Quick hack for now, remove later when everything is binary
        // For the test case "test_server1"
        if local_port == 2001 {
            info!("Parse text data for {}", &station_name);

            match parse_text_data(&buffer_right) {
                Ok(parsed_data) => {
                    info!("Data parsed correctly");
                    match db_pool.lock() {
                        Ok(db_pool) => {
                            store_to_db(&db_pool, &station_name, &parsed_data)?;
                        },
                        Err(e) => info!("Mutex (poison) error (db_pool): {}", e)
                    }
                },
                Err(e) => {
                    info!("Could not parse data: {}", e);
                }
            }
        } else {
            info!("Parse binary data for {}", &station_name);

            for (counter, parsed_data) in parse_binary_data(&buffer_right).iter().enumerate() {
                match *parsed_data {
                    Ok(ref parsed_data) => {
                        info!("Data parsed correctly ({})", counter + 1);
                        match db_pool.lock() {
                            Ok(db_pool) => {
                                store_to_db(&db_pool, &station_name, &parsed_data)?;
                            },
                            Err(e) => info!("Mutex (poison) error (db_pool): {}", e)
                        }
                    },
                    Err(ref e) => {
                        info!("Could not parse data: {}", e);
                    }
                }
            }
        }
    } else if tcp_buffer.len() < HEADER_LENGTH {
        info!("[{}] Invalid header (less than {} bytes received)!", local_port, HEADER_LENGTH);
        info!("[{}] Bytes: {:?}", local_port, tcp_buffer);
        // info!("Bytes (ASCII): '{}'", String::from_utf8_lossy(&tcp_buffer));
    } else { // tcp_buffer.len() == HEADER_LENGTH -> no data, only header
        info!("[{}] No data received, just header.", local_port);
        info!("[{}] Bytes: {:?}", local_port, tcp_buffer);
        // info!("Bytes (ASCII): '{}'", String::from_utf8_lossy(&tcp_buffer));
    }

    info!("handle_client finished");

    Ok(None)
}

pub fn start_service(config: &Configuration) {
    let mut listeners = Vec::new();

    for port in &config.ports {
        match TcpListener::bind(("0.0.0.0", *port)) {
            Ok(listener) => {
                info!("Create listener for port {}", port);
                listeners.push(listener);
            },
            Err(e) => {
                info!("Network error: {}", e);
                process::exit(1);
            }
        }
    }

    let db_pool = Arc::new(Mutex::new(init_db(&config)));

    for listener in listeners {
        let cloned_pool = db_pool.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
                    let (mut stream, addr) = result;
                    match handle_client(&mut stream, &addr, &cloned_pool) {
                        Ok(None) => {},
                        Ok(Some(query_result)) => { info!("Database insert successful: {}, {}",
                            query_result.affected_rows(),  query_result.last_insert_id()) },
                        Err(StoreDataError::MySQLError(db_error)) => { info!("DB Error: {}", db_error) },
                        Err(StoreDataError::IOError(io_error)) => { info!("IO Error: {}", io_error) },
                        Err(StoreDataError::TimeParseError(time_error)) => { info!("Time parse Error: {}", time_error) }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;
    use std::time::Duration;
    use std::thread::sleep;
    use std::io::Write;

    use time::{strptime};
    use mysql::{Value, Pool, OptsBuilder};
    use chrono::NaiveDateTime;

    use configuration::Configuration;
    use data_parser::{StationDataType, WeatherStationData};
    use super::{store_to_db, port_to_station, start_service};

    #[test]
    fn test_port_to_station() {
        assert_eq!(port_to_station(2100), "Nahuelbuta");
        assert_eq!(port_to_station(2101), "Santa_Gracia");
        assert_eq!(port_to_station(2102), "Pan_de_Azucar");
        assert_eq!(port_to_station(2103), "La_Campana");
        assert_eq!(port_to_station(2104), "Wanne_Tuebingen");
        assert_eq!(port_to_station(2105), "unknown");
    }

    #[test]
    fn test_store_to_db1() {
        // let _ = init(LogConfig { log_to_file: true, format: detailed_format, .. LogConfig::new() }, Some("info".to_string()));

        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        let query_result = store_to_db(&pool, "test_store1", &StationDataType::SimpleData(strptime("2016-06-12 12:13:14",
        "%Y-%m-%d %H:%M:%S").unwrap(), 12.73, 0.0, 0.0));
        let query_result = query_result.unwrap().unwrap();
        let affected_rows = query_result.affected_rows();
        assert_eq!(affected_rows, 1);
        let last_insert_id = query_result.last_insert_id();

        let select_result = pool.prep_exec("SELECT * FROM battery_data WHERE id = (:id)", (Value::from(last_insert_id),)).unwrap();

        let mut count = 0;

        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 6);
            let row_id: u64 = row_item.get(0).unwrap();
            assert_eq!(row_id, last_insert_id);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-06-12 12:13:14", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test_store1");
            let row_voltage: f64 = row_item.get(3).unwrap();
            assert_eq!(row_voltage, 12.73);
            count = count + 1;
        }

        assert_eq!(count, 1);

        let delete_result = pool.prep_exec("DELETE FROM battery_data WHERE station = 'test_store1'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }

    #[test]
    fn test_store_to_db2() {
        // let _ = init(LogConfig { log_to_file: true, format: detailed_format, .. LogConfig::new() }, Some("info".to_string()));

        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        let query_result = store_to_db(&pool, "test_store2", &StationDataType::MultipleData(WeatherStationData{
            timestamp: strptime("2016-06-15 15:16:17", "%Y-%m-%d %H:%M:%S").unwrap(),
            air_temperature: 18.15,
            air_relative_humidity: 65.31,
            solar_radiation: 620.4,
            soil_water_content: 0.056,
            soil_temperature: 16.25,
            wind_speed: 4.713,
            wind_max: 9.5,
            wind_direction: 257.9,
            precipitation: 1.232,
            air_pressure: 981.4
        }));
        let query_result = query_result.unwrap().unwrap();
        let affected_rows = query_result.affected_rows();
        assert_eq!(affected_rows, 1);
        let last_insert_id = query_result.last_insert_id();

        let select_result = pool.prep_exec("SELECT * FROM multiple_data WHERE id = (:id)", (Value::from(last_insert_id),)).unwrap();

        let mut count = 0;

        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 13);
            let row_id: u64 = row_item.get(0).unwrap();
            assert_eq!(row_id, last_insert_id);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-06-15 15:16:17", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test_store2");
            let row_air_temperature: f64 = row_item.get(3).unwrap();
            assert_eq!(row_air_temperature, 18.15);
            let row_air_relative_humidity: f64 = row_item.get(4).unwrap();
            assert_eq!(row_air_relative_humidity, 65.31);
            let row_solar_radiation: f64 = row_item.get(5).unwrap();
            assert_eq!(row_solar_radiation, 620.4 );
            let row_soil_water_content: f64 = row_item.get(6).unwrap();
            assert_eq!(row_soil_water_content, 0.056);
            let row_soil_temperature: f64 = row_item.get(7).unwrap();
            assert_eq!(row_soil_temperature, 16.25);
            let row_wind_speed: f64 = row_item.get(8).unwrap();
            assert_eq!(row_wind_speed, 4.713);
            let row_wind_max: f64 = row_item.get(9).unwrap();
            assert_eq!(row_wind_max, 9.5);
            let row_wind_direction: f64 = row_item.get(10).unwrap();
            assert_eq!(row_wind_direction, 257.9);
            let row_precipitation: f64 = row_item.get(11).unwrap();
            assert_eq!(row_precipitation, 1.232);
            let row_air_pressure: f64 = row_item.get(12).unwrap();
            assert_eq!(row_air_pressure, 981.4);
            count = count + 1;
        }

        assert_eq!(count, 1);

        let delete_result = pool.prep_exec("DELETE FROM multiple_data WHERE station = 'test_store2'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }

    #[test]
    fn test_server1() {
        let config = Configuration {
            ports: vec![2001],
            log_level: "info".to_string(),
            hostname: "localhost".to_string(),
            db_name: "test_weatherstation".to_string(),
            username: "test".to_string(),
            password: "test".to_string(),
            binary_filename: None,
            binary_station_name: None
        };

        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        info!("DB connection successful!");


        // Make sure that there is no old data laying around
        let _ = pool.prep_exec("DELETE FROM battery_data WHERE station = 'test1'", ()).unwrap();

        start_service(&config);

        info!("Wait for server...");

        // Wait for the server to start up.
        sleep(Duration::new(1, 0));

        info!("Wait end!");

        {
            // Connect to local server
            let mut stream = TcpStream::connect("127.0.0.1:2001").unwrap();

            let result = stream.write_fmt(format_args!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"2016-04-17 17:29:22\",7.53,0"));
            assert!(result.is_ok());
        } // Socket gets closed here!

        info!("Wait for client...");

        // Wait for the client to submit the data.
        // Wait for the server to parse and process the data.
        sleep(Duration::new(1, 0));

        info!("Wait end!");

        let select_result = pool.prep_exec("SELECT * FROM battery_data WHERE station = 'test1'", ()).unwrap();

        let mut count = 0;

        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 6);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-04-17 17:29:22", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test1");
            let row_voltage: f64 = row_item.get(3).unwrap();
            assert_eq!(row_voltage, 7.53);
            count = count + 1;
        }
        assert_eq!(count, 1);

        let delete_result = pool.prep_exec("DELETE FROM battery_data WHERE station = 'test1'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }

    #[test]
    fn test_server2() {
        let config = Configuration {
            ports: vec![2200],
            log_level: "info".to_string(),
            hostname: "localhost".to_string(),
            db_name: "test_weatherstation".to_string(),
            username: "test".to_string(),
            password: "test".to_string(),
            binary_filename: None,
            binary_station_name: None
        };

        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        info!("DB connection successfull!");

        // Make sure that there is no old data laying around
        let _ = pool.prep_exec("DELETE FROM battery_data WHERE station = 'test2'", ()).unwrap();

        start_service(&config);

        info!("Wait for server...");

        // Wait for the server to start up.
        sleep(Duration::new(1, 0));

        info!("Wait end!");

        {
            // Connect to local server
            let mut stream = TcpStream::connect("127.0.0.1:2200").unwrap();

            let result = stream.write(&vec![0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,12,0,141,64,50,0,0,0,0,68,252,96,0,0,0]);
            assert!(result.is_ok());
        } // Socket gets closed here!

        info!("Wait for client...");

        // Wait for the client to submit the data.
        // Wait for the server to parse and process the data.
        sleep(Duration::new(1, 0));

        info!("Wait end!");

        let select_result = pool.prep_exec("SELECT * FROM battery_data WHERE station = 'test2'", ()).unwrap();

        let mut count = 0;

        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 6);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test2");
            let row_voltage: f64 = row_item.get(3).unwrap();
            assert_eq!(row_voltage, 12.76);
            count = count + 1;
        }
        assert_eq!(count, 1);

        let delete_result = pool.prep_exec("DELETE FROM battery_data WHERE station = 'test2'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }


    // Test server:
    // echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"2016-07-06 00:00:00",12.71,0 | nc localhost 2001
    // echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"2016-07-06 12:00:00",13.86,9.98,356.3,0.055,14.12,1.248,2.6,121.7,0,979 | nc localhost 2001
}
