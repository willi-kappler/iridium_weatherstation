// iridium_weatherstation V0.3 (2022.04.05), written by Willi Kappler
//
// Licensed under the MIT License
//
// A simple data processing tool written in Rust for one of the campbell iridium weather stations
//

mod config;
mod error;
mod process_data;


use std::fs::File;
use std::net::{TcpListener};
use std::thread::{spawn, sleep};
use std::time::Duration;

use log::{info, debug, error};
use simplelog::{WriteLogger, LevelFilter, ConfigBuilder};
use chrono::Local;

use crate::config::IWConfiguration;
use crate::process_data::handle_connection;


fn main() {
    let dt = Local::now();
    let log_file_name = dt.format("iridium_weatherstation_%Y_%m_%d.log").to_string();
    let log_config = ConfigBuilder::new()
        .set_time_to_local(true)
        .set_time_format_str("%Y.%m.%d - %H:%M:%S")
        .build();

    let _ = WriteLogger::init(
        LevelFilter::Debug,
        log_config, 
        File::create(log_file_name).unwrap()
    );

    info!("Data processor started.");

    let config_file = File::open("iridium_weatherstation_config.json").unwrap();
    let config: IWConfiguration = serde_json::from_reader(config_file).unwrap();

    info!("Configuration was read successfully.");

    debug!("Settings: {:?}", config);

    let mut listeners = Vec::new();

    for port in config.ports {
        match TcpListener::bind(("0.0.0.0", port)) {
            Ok(listener) => {
                debug!("Create listener for port: '{}'", port);
                listeners.push(listener);
            }
            Err(e) => {
                error!("An error occurred while binding to port: '{}'", e);
            }
        }
    }

    for listener in listeners {
        spawn(move || {
            loop {
                match listener.accept() {
                    Ok((stream, socket)) => {
                        match handle_connection(stream, socket) {
                            Ok(_) => {
                                debug!("Data was processed successfully");
                            }
                            Err(e) => {
                                error!("An error occurred while processing the data: '{}'", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("An error occurred while accepting the connection: '{}'", e);
                    }
                }
            }
        });
    }

    loop {
        info!("Alive message");
        sleep(Duration::from_secs(config.alive_message_intervall));
    }
}
