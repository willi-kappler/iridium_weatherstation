// iridium_weatherstation V0.1 (2016.05.09), written by Willi Kappler
//
// Licensed under the MIT License
//
// A simple data processing tool written in Rust for one of the campbell iridium weather stations
//

// Use half for 16 bit floating point conversion:
// https://crates.io/crates/half
// https://github.com/starkat99/half-rs
// http://starkat99.github.io/half-rs/half/struct.f16.html


// External crates:
#[macro_use] extern crate log;
extern crate flexi_logger;

// Internal crates:
extern crate station_util;

// System modules:
use std::time::Duration;
use std::thread::sleep;

// External modules:
use flexi_logger::{detailed_format, init, LogConfig};

// Internal modules:
use station_util::configuration::{setup_configuration, ALIVE_MSG_INTERVALL};
use station_util::server::{init_db, store_to_db, start_service};
use station_util::data_parser::{parse_binary_data_from_file};


fn main() {
    // Parse command line arguments
    let config = setup_configuration();

    // Initialize logger
    init(LogConfig { log_to_file: true, format: detailed_format, .. LogConfig::new() }, Some(config.log_level.clone()))
    .unwrap_or_else(|e| { panic!("Logger initialization failed with the following error: {}", e) });

    info!("Data processor started.");

    match (config.binary_filename.clone(), config.binary_station_name.clone()) {
        (Some(filename), Some(station_name)) => {
            println!("Reading binary data from file '{}'", filename);

            let db_pool = init_db(&config);

            for parsed_data in parse_binary_data_from_file(&filename) {
                if let Ok(data) = parsed_data {
                    let _ = store_to_db(&db_pool, &station_name, &data).unwrap();
                }
            }

            println!("Data imported succesfully into the database!");

            return;
        }
        _ => {}
    }

    let mut ports = String::new();

    for p in &config.ports {
        ports.push_str(&format!("{}, ", p));
    }

    info!("Using ports: {}", ports);
    info!("Hostname: {}", config.hostname);
    info!("Database: {}", config.db_name);
    info!("DB user: {}", config.username);

    start_service(&config);

    loop {
        info!("Alive message");
        sleep(Duration::new(ALIVE_MSG_INTERVALL, 0));
    }
}
