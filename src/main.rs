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

// Internal crates:
extern crate station_util;

// System modules:
use std::time::Duration;
use std::thread::sleep;

// External crates:
use log4rs;
use log::{info};
use chrono::Local;

// Internal modules:
use station_util::configuration::{setup_configuration, ALIVE_MSG_INTERVALL};
use station_util::server::{init_db, store_to_db, start_service};
use station_util::data_parser::{parse_binary_data_from_file};

fn main() {
    // Parse command line arguments
    let config = setup_configuration();

    // Initialize logger
    let dt = Local::now();
    let log_filename = dt.format("iridium_weatherstation_%Y_%m_%d.log").to_string();

    let file_logger = log4rs::append::file::FileAppender::builder()
        .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new("{d} {l} - {m}{n}")))
        .build(log_filename).unwrap();

    let log_config = log4rs::config::Config::builder()
        .appender(log4rs::config::Appender::builder().build("file_logger", Box::new(file_logger)))
        .build(log4rs::config::Root::builder().appender("file_logger").build(log::LevelFilter::Debug))
        .unwrap();

    let _log_handle = log4rs::init_config(log_config).unwrap();

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

            println!("Data imported successfully into the database!");

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
