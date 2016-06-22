//! Provides the server and handles the incomming requests
//! All ports are handled by the same function

// System modules:
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::spawn;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::io;
use time::{now, Tm};
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

fn write_xml_data<'a>(tm: Tm, local_port: u16, data: &StationDataType, file_name: &str) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    let mut file_handle = io::BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.xml", file_name))));

    let datetime_format = "%Y-%m-%d %H:%M:%S";

    let current_datetime = tm.strftime(&datetime_format).unwrap();

    try!(write!(file_handle, "<measure>\n"));
    try!(write!(file_handle, "<port>{}</port>\n", local_port));
    try!(write!(file_handle, "<datetime>{}</datetime>\n", &current_datetime));
    try!(write!(file_handle, "<data>\n"));

    match data {
        &StationDataType::SingleData(timestamp_tm, voltage) => {
            let timestamp = try!(timestamp_tm.strftime(&datetime_format));
            try!(write!(file_handle, "    <timestamp>{}</timestamp>\n", timestamp));
            try!(write!(file_handle, "    <voltage>{}</voltage>\n", voltage));
        },
        &StationDataType::MultipleData(ref full_data_set) => {
            let timestamp = try!(full_data_set.timestamp.strftime(&datetime_format));
            try!(write!(file_handle, "    <timestamp>{}</timestamp>\n", timestamp));
            try!(write!(file_handle, "    <air_temperature>{}</air_temperature>\n", full_data_set.air_temperature));
            try!(write!(file_handle, "    <air_relative_humidity>{}</air_relative_humidity>\n", full_data_set.air_relative_humidity));
            try!(write!(file_handle, "    <solar_radiation>{}</solar_radiation>\n", full_data_set.solar_radiation));
            try!(write!(file_handle, "    <soil_water_content>{}</soil_water_content>\n", full_data_set.soil_water_content));
            try!(write!(file_handle, "    <soil_temperature>{}</soil_temperature>\n", full_data_set.soil_temperature));
            try!(write!(file_handle, "    <wind_speed>{}</wind_speed>\n", full_data_set.wind_speed));
            try!(write!(file_handle, "    <wind_max>{}</wind_max>\n", full_data_set.wind_max));
            try!(write!(file_handle, "    <wind_direction>{}</wind_direction>\n", full_data_set.wind_direction));
            try!(write!(file_handle, "    <precipitation>{}</precipitation>\n", full_data_set.precipitation));
            try!(write!(file_handle, "    <air_pressure>{}</air_pressure>\n", full_data_set.air_pressure));
        }
    }

    try!(write!(file_handle, "</data>\n"));
    try!(write!(file_handle, "</measure>\n\n"));

    Ok(None)
}

fn write_csv_data<'a>(data: &StationDataType, file_name: &str) -> Result<Option<QueryResult<'a>>, StoreDataError> {
    let mut file_handle = io::BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.csv", file_name))));

        let datetime_format = "%Y-%m-%d %H:%M:%S";

        match data {
            &StationDataType::SingleData(timestamp_tm, voltage) => {
                let timestamp = try!(timestamp_tm.strftime(&datetime_format));
                try!(write!(file_handle, "\"{}\", {}\n", timestamp, voltage));
            },
            &StationDataType::MultipleData(ref full_data_set) => {
                let timestamp = try!(full_data_set.timestamp.strftime(&datetime_format));
                try!(write!(file_handle, "\"{}\", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n",
                    timestamp,
                    full_data_set.air_temperature,
                    full_data_set.air_relative_humidity,
                    full_data_set.solar_radiation,
                    full_data_set.soil_water_content,
                    full_data_set.soil_temperature,
                    full_data_set.wind_speed,
                    full_data_set.wind_max,
                    full_data_set.wind_direction,
                    full_data_set.precipitation,
                    full_data_set.air_pressure
                ));
            }
        }

    Ok(None)
}

fn write_to_db<'a>(db_pool: &Pool, station_folder: &str, data: &StationDataType) -> Result<Option<QueryResult<'a>>, StoreDataError> {
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
            let result = try!(db_pool.prep_exec("insert into battery_data (
                    timestamp,
                    station,
                    air_pressure,
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
                    :air_pressure,
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
                    Value::from(full_data_set.air_pressure),
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


    Ok(None)
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
    all_data_file: &Arc<Mutex<String>>, monthly_data_folder: &Arc<Mutex<String>>,
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
                match all_data_file.lock() {
                    Ok(all_data_file) => {
                        let tm = now();
                        let file_name = format!("{}/{}", station_folder, *all_data_file);
                        try!(write_xml_data(tm, local_port, &parsed_data, &file_name));
                        try!(write_csv_data(&parsed_data, &file_name));
                    },
                    Err(e) => info!("Mutex (poison) error (all_data_file): {}", e)
                }

                match monthly_data_folder.lock() {
                    Ok(monthly_data_folder) => {
                        let tm = now();
                        let current_year = tm.strftime("%Y").unwrap();
                        let current_month = tm.strftime("%m").unwrap();
                        // TODO: create separate folder for year and month in Rust
                        let file_name = format!("{}/{}/{}_{}", *monthly_data_folder, station_folder, current_year, current_month);
                        try!(write_xml_data(tm, local_port, &parsed_data, &file_name));
                        try!(write_csv_data(&parsed_data, &file_name));
                    },
                    Err(e) => info!("Mutex (poison) error (monthly_data_folder): {}", e)
                }
                match db_pool.lock() {
                    Ok(db_pool) => {
                        try!(write_to_db(&db_pool, &station_folder, &parsed_data));
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

    let all_data_file = Arc::new(Mutex::new(config.all_data_file.clone()));
    let monthly_data_folder = Arc::new(Mutex::new(config.monthly_data_folder.clone()));
    let db_pool = Arc::new(Mutex::new(db_pool));

    for listener in listeners {
        let all_data_file = all_data_file.clone();
        let monthly_data_folder = monthly_data_folder.clone();
        let cloned_pool = db_pool.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
                    let (mut stream, addr) = result;
                    match handle_client(&mut stream, &addr,
                            &all_data_file, &monthly_data_folder, &cloned_pool) {
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
    use data_parser::StationDataType;
    use time::{now};
    use mysql::{Value, Pool, OptsBuilder};
    use chrono::naive::datetime::NaiveDateTime;

    use super::{write_csv_data, write_xml_data, write_to_db, port_to_station};

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
    fn test_write_csv_data1() {
        assert!(write_csv_data(&StationDataType::SingleData(strptime("2016-06-12 00:00:00",
        "%Y-%m-%d %H:%M:%S").unwrap(), 12.73), "/does_not_work/does_not_work.csv").is_err());
    }


    #[test]
    fn test_write_xml_data1() {
        assert!(write_xml_data(now(), 0, &StationDataType::SingleData(strptime("2016-06-12 00:00:00",
        "%Y-%m-%d %H:%M:%S").unwrap(), 12.73), "/does_not_work/does_not_work.csv").is_err());
    }

    #[test]
    fn test_write_to_db1() {
        let mut db_builder = OptsBuilder::new();
        db_builder.ip_or_hostname(Some("localhost"))
                  .tcp_port(3306)
                  .user(Some("test"))
                  .pass(Some("test"))
                  .db_name(Some("test_weatherstation"));
        let pool = Pool::new(db_builder).unwrap();

        let query_result = write_to_db(&pool, "test1", &StationDataType::SingleData(strptime("2016-06-12 00:00:00",
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
            assert_eq!(row_timestamp, NaiveDateTime::parse_from_str("2016-06-12 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
            let row_station: String = row_item.get(2).unwrap();
            assert_eq!(row_station, "test1");
            let row_voltage: f64 = row_item.get(3).unwrap();
            assert_eq!(row_voltage, 12.73);
        }

        let delete_result = pool.prep_exec("delete from battery_data where station='test1'", ()).unwrap();
        assert_eq!(delete_result.affected_rows(), 1);
    }

}
