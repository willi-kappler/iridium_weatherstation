//! Provides the server and handles the incomming requests
//! All ports are handled by the same function

// System modules:
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::spawn;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::io;
use time;
use mysql;
use mysql::{OptsBuilder, Pool, Value};
use mysql::conn::QueryResult;
use std::process;

// Internal modules:
use configuration::{Configuration, HEADER_LENGTH};
use data_parser::{parse_text_data, StationDataType};

quick_error! {
    #[derive(Debug)]
    enum StoreDataError {
        IOError(error: io::Error) { from() }
        MySQLError(error: mysql::Error) { from() }
        TimeParseError(error: time::ParseError) { from() }
    }
}

fn store_to_db<'a>(db_pool: &Pool, station_folder: &str, data: &StationDataType) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    let datetime_format = "%Y-%m-%d %H:%M:%S";

    match data {
        &StationDataType::SingleData(timestamp_tm, voltage) => {
            let timestamp = try!(timestamp_tm.strftime(&datetime_format));
            let result = try!(db_pool.prep_exec("insert into battery_data (timestamp, station,
                battery_voltage) values (:timestamp, :station, :battery_voltage)",
                (Value::from(timestamp.to_string()), Value::from(station_folder), Value::from(voltage))));
            return Ok(Some(result));
        },
        &StationDataType::MultipleData(ref full_data_set) => {
            let timestamp = try!(full_data_set.timestamp.strftime(&datetime_format));
            let result = try!(db_pool.prep_exec("insert into multiple_data (
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
                ) values (
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
                    Value::from(station_folder),
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
                )));
            return Ok(Some(result));
        }
    }
}

fn port_to_station(port: u16) -> String{
    match port {
        2100 => "2100_Na".to_string(),
        2101 => "2101_SG".to_string(),
        2102 => "2102_PdA".to_string(),
        2103 => "2103_LC".to_string(),
        2104 => "2104_Tue".to_string(),
        _ => "unknown".to_string()
    }
}

fn handle_client<'a>(stream: &mut TcpStream, remote_addr: &SocketAddr,
    db_pool: &Arc<Mutex<Pool>>) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    info!("Client socket address: {}", remote_addr);

    let local_addr = try!(stream.local_addr());

    let local_port = match local_addr {
        SocketAddr::V4(local_addr) => local_addr.port(),
        SocketAddr::V6(local_addr) => local_addr.port()
    };

    info!("Port: {}", local_port);

    let mut buffer = Vec::new();

    let len = try!(stream.read_to_end(&mut buffer));
    info!("Number of bytes received: {}", len);

    if buffer.len() > HEADER_LENGTH {
        let (buffer_left, buffer_right) = buffer.split_at(HEADER_LENGTH);

        let str_header = String::from_utf8_lossy(buffer_left);
        let str_data = String::from_utf8_lossy(buffer_right);

        info!("Header: {:?}", buffer_left);
        info!("Data: {:?}", buffer_right);

        info!("Header (ASCII): '{}'", str_header);
        info!("Data (ASCII): '{}'", str_data);

        let station_folder = port_to_station(local_port);

        match parse_text_data(&buffer_right) {
            Ok(parsed_data) => {
                info!("Data parsed correctly");
                match db_pool.lock() {
                    Ok(db_pool) => {
                        try!(store_to_db(&db_pool, &station_folder, &parsed_data));
                    },
                    Err(e) => info!("Mutex (poison) error (db_pool): {}", e)
                }
            },
            Err(e) => {
                info!("Could not parse data: {}", e);
            }
        }
    } else if buffer.len() < HEADER_LENGTH {
        info!("Invalid header (less than {} bytes received)!", HEADER_LENGTH);
        info!("Bytes: {:?}", buffer);
        info!("Bytes (ASCII): '{}'", String::from_utf8_lossy(&buffer));
    } else { // buffer.len() == HEADER_LENGTH -> no data, only header
        info!("No data received, just header.");
        info!("Bytes: {:?}", buffer);
        info!("Bytes (ASCII): '{}'", String::from_utf8_lossy(&buffer));
    }

    info!("handle_client finished");

    Ok(None)
}

pub fn start_service(config: Configuration) {
    let mut listeners = Vec::new();

    for port in config.ports {
        match TcpListener::bind(("0.0.0.0", port)) {
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

    let mut db_builder = OptsBuilder::new();
    db_builder.ip_or_hostname(Some(config.hostname))
           .db_name(Some(config.db_name))
           .user(Some(config.username))
           .pass(Some(config.password));
    let db_pool = match Pool::new(db_builder) {
        Ok(db_pool) => db_pool,
        Err(e) => {
            info!("Database error: {}", e);
            process::exit(1);
        }
    };

    let db_pool = Arc::new(Mutex::new(db_pool));

    for listener in listeners {
        let cloned_pool = db_pool.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
                    let (mut stream, addr) = result;
                    match handle_client(&mut stream, &addr, &cloned_pool) {
                        Ok(None) => {},
                        Ok(Some(query_result)) => { info!("Database insert successfull: {}, {}",
                            query_result.affected_rows(),  query_result.last_insert_id()) },
                        Err(data_error) => { info!("Store Data Error: {}", data_error) }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use time::{strptime};
    use data_parser::{StationDataType, WeatherStationData};
    use mysql::{Value, Pool, OptsBuilder};
    use chrono::naive::datetime::NaiveDateTime;

    use super::{store_to_db, port_to_station};

    #[test]
    fn test_port_to_station() {
        assert_eq!(port_to_station(2100), "2100_Na");
        assert_eq!(port_to_station(2101), "2101_SG");
        assert_eq!(port_to_station(2102), "2102_PdA");
        assert_eq!(port_to_station(2103), "2103_LC");
        assert_eq!(port_to_station(2104), "2104_Tue");
        assert_eq!(port_to_station(2105), "unknown");
    }

    #[test]
    fn test_store_to_db1() {
        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        let query_result = store_to_db(&pool, "test1", &StationDataType::SingleData(strptime("2016-06-12 12:13:14",
        "%Y-%m-%d %H:%M:%S").unwrap(), 12.73));
        let query_result = query_result.unwrap().unwrap();
        let affected_rows = query_result.affected_rows();
        assert_eq!(affected_rows, 1);
        let last_insert_id = query_result.last_insert_id();

        let select_result = pool.prep_exec("select * from battery_data where id=(:id)", (Value::from(last_insert_id),)).unwrap();
        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 4);
            let row_id: u64 = row_item.get(0).unwrap();
            assert_eq!(row_id, last_insert_id);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-06-12 12:13:14", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test1");
            let row_voltage: f64 = row_item.get(3).unwrap();
            assert_eq!(row_voltage, 12.73);
        }

        let delete_result = pool.prep_exec("delete from battery_data where station='test1'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }

    #[test]
    fn test_store_to_db2() {
        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        let query_result = store_to_db(&pool, "test2", &StationDataType::MultipleData(WeatherStationData{
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

        let select_result = pool.prep_exec("select * from multiple_data where id=(:id)", (Value::from(last_insert_id),)).unwrap();
        for opt_item in select_result {
            let mut row_item = opt_item.unwrap();
            assert_eq!(row_item.len(), 13);
            let row_id: u64 = row_item.get(0).unwrap();
            assert_eq!(row_id, last_insert_id);
            let row_timestamp: NaiveDateTime = row_item.get(1).unwrap();
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-06-15 15:16:17", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test2");
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
        }

        let delete_result = pool.prep_exec("delete from multiple_data where station='test2'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }


}
