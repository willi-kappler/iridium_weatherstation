//! Utility function used by iridium_weatherstation
//! Just contains references to external and internal modules

// External crates
#[macro_use] extern crate log;
#[macro_use] extern crate clap;
#[macro_use] extern crate quick_error;
#[macro_use] extern crate mysql;

extern crate flexi_logger;
extern crate time;
extern crate regex;
extern crate chrono;

// Internal modules
pub mod configuration;
pub mod server;
pub mod data_parser;
