//! Provides the server and handles the incomming requests
//! All ports are handled by the same function

// System modules:
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::spawn;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::Result;
use time::{now, Tm};
use mysql;
use std::process;

// Internal modules:
use configuration::{Configuration, HEADER_LENGTH};
use data_parser::{parse_text_data, StationDataType};

fn write_xml_data(tm: Tm, local_port: u16, data: &StationDataType, file_name: &str) -> Result<()> {
    let mut file_handle = BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.xml", file_name))));

    let date_time_format = "%Y-%m-%d %H:%M:%S";

    let current_date_time = tm.strftime(&date_time_format).unwrap();

    try!(write!(file_handle, "<measure>\n"));
    try!(write!(file_handle, "<port>{}</port>\n", local_port));
    try!(write!(file_handle, "<date_time>{}</date_time>\n", &current_date_time));
    try!(write!(file_handle, "<data>\n"));

    // try!(write!(file_handle, "{:?}\n", data));

    match data {
        &StationDataType::SingleData(time_stamp_tm, voltage) => {
            if let Ok(time_stamp) = time_stamp_tm.strftime(&date_time_format) {
                try!(write!(file_handle, "    <time_stamp>{}</time_stamp>\n", time_stamp));
                try!(write!(file_handle, "    <voltage>{}</voltage>\n", voltage));
            }
        },
        &StationDataType::MultipleData(ref full_data_set) => {
            if let Ok(time_stamp) = full_data_set.time_stamp.strftime(&date_time_format) {
                try!(write!(file_handle, "    <time_stamp>{}</time_stamp>\n", time_stamp));
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
    }

    try!(write!(file_handle, "</data>\n"));
    try!(write!(file_handle, "</measure>\n\n"));

    Ok(())
}

fn write_csv_data(data: &StationDataType, file_name: &str) -> Result<()> {
    let mut file_handle = BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.csv", file_name))));

        let date_time_format = "%Y-%m-%d %H:%M:%S";

        match data {
            &StationDataType::SingleData(time_stamp_tm, voltage) => {
                if let Ok(time_stamp) = time_stamp_tm.strftime(&date_time_format) {
                    try!(write!(file_handle, "\"{}\", {}\n", time_stamp, voltage));
                }
            },
            &StationDataType::MultipleData(ref full_data_set) => {
                if let Ok(time_stamp) = full_data_set.time_stamp.strftime(&date_time_format) {
                    try!(write!(file_handle, "\"{}\", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n",
                        time_stamp,
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
        }

    Ok(())
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

fn handle_client(stream: &mut TcpStream, remote_addr: &SocketAddr,
    all_data_file: &Arc<Mutex<String>>, monthly_data_folder: &Arc<Mutex<String>>,
    pool: &Arc<Mutex<mysql::Pool>>) -> Result<()> {
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

    Ok(())
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

    let mut builder = mysql::OptsBuilder::new();
    builder.ip_or_hostname(Some(config.hostname))
           .db_name(Some(config.db_name))
           .user(Some(config.username))
           .pass(Some(config.password));
    let pool = match mysql::Pool::new(builder) {
        Ok(pool) => pool,
        Err(e) => {
            info!("Database error: {}", e);
            process::exit(1);
        }
    };

    let all_data_file = Arc::new(Mutex::new(config.all_data_file.clone()));
    let monthly_data_folder = Arc::new(Mutex::new(config.monthly_data_folder.clone()));
    let cloned_pool = Arc::new(Mutex::new(pool));

    for listener in listeners {
        let all_data_file = all_data_file.clone();
        let monthly_data_folder = monthly_data_folder.clone();
        let cloned_pool = cloned_pool.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
                    let (mut stream, addr) = result;
                    if let Err(io_error) = handle_client(&mut stream, &addr,
                            &all_data_file, &monthly_data_folder, &cloned_pool) {
                        info!("IOError: {}", io_error);
                    }
                }
            }
        });
    }
}
