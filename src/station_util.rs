//! Utility function used by iridium_weatherstation
//! Just contains references to external and internal modules

// External crates
#[macro_use] extern crate log;
#[macro_use] extern crate clap;
extern crate flexi_logger;

// Internal modules
pub mod configuration;
pub mod server;
