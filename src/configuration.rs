//! Handles the configuration for iridium_weatherstation
//! Parses command line arguments via clap and sets default values

// External modules:
use clap::{App, Arg};

pub const HEADER_LENGTH: usize = 48;
pub const ALIVE_MSG_INTERVALL: u64 = 60*60*4;

/// Server configuration from command line arguments
#[derive(Debug, Clone, PartialEq)]
pub struct Configuration {
    /// Ports for weather stations
    pub ports: Vec<u16>,
    /// Set the log level for flexi_logger: error, info or debug
    pub log_level: String,
    /// File that contains all the data
    pub all_data_file: String,
    /// Folder that contains individual data, monthly basis
    pub monthly_data_folder: String,
    /// Connection properties for the the MySQL database
    pub hostname: String,
    pub db_name: String,
    pub username: String,
    pub password: String
}

fn default_ports() -> Vec<u16> {
    vec![2001, 2002, 2003]
}

fn string_to_ports(input_string: &str) -> Vec<u16> {
    let mut result : Vec<u16> = Vec::new();

    for p in input_string.split(':') {
        let value = p.trim().parse::<u16>();
        if let Ok(port) = value {
            result.push(port);
        }
    }

    if result.is_empty() {
        default_ports()
    } else {
        // Ensure that each port is used only once
        result.sort();
        result.dedup();
        result
    }
}

/// This will parse the command line arguments and create a new configuration object.
/// If the arguments are missing or there is a parse error, then the default values are used
pub fn setup_configuration() -> Configuration {
    let matches = App::new("iridium_weatherstation")
        .version("0.1")
        .author("Willi Kappler")
        .about("A small tool for processing data from one of the campbell iridium weather stations")
        .arg(
            Arg::with_name("ports")
            .long("ports")
            .help("Sets the ports for the weather stations (default: 2001:2002: 2003)")
            .takes_value(true)
        )
        .arg(
            Arg::with_name("loglevel")
            .long("loglevel")
            .help("Specify log level: error, info or debug. Default: info")
            .takes_value(true)
        )
        .arg(
            Arg::with_name("hostname")
            .long("hostname")
            .help("The hostname for the MySQL database connection")
            .takes_value(true)
        )
        .arg(
            Arg::with_name("db_name")
            .long("db_name")
            .help("The database name for the MySQL database connection")
            .takes_value(true)
        )
        .arg(
            Arg::with_name("username")
            .long("username")
            .help("The username for the MySQL database connection")
            .takes_value(true)
        )
        .arg(
            Arg::with_name("password")
            .long("password")
            .help("The password for the MySQL database connection")
            .takes_value(true)
        )
        .get_matches();

        let ports = match matches.value_of("ports") {
            Some(p) => string_to_ports(p),
            _ => default_ports()
        };

        let log_level = match matches.value_of("loglevel") {
            Some(value) => value,
            _ => "info"
        };

        let hostname = match matches.value_of("hostname") {
            Some(hostname) => hostname,
            _ => "localhost"
        };

        let db_name = match matches.value_of("db_name") {
            Some(db_name) => db_name,
            _ => "weatherstation"
        };

        let username = match matches.value_of("username") {
            Some(username) => username,
            _ => "root"
        };

        let password = match matches.value_of("password") {
            Some(password) => password,
            _ => "none"
        };

        Configuration {
            ports: ports,
            log_level: log_level.to_string(),
            all_data_file: "all_data".to_string(),
            monthly_data_folder: "monthly".to_string(),
            hostname: hostname.to_string(),
            db_name: db_name.to_string(),
            username: username.to_string(),
            password: password.to_string()
        }
}

#[cfg(test)]
mod tests {
    use super::{string_to_ports, default_ports, setup_configuration, Configuration};

    #[test]
    fn test_setup_configuration() {
        assert_eq!(setup_configuration(), Configuration{ ports: vec![2001, 2002, 2003],
            log_level: "info".to_string(),
            all_data_file: "all_data".to_string(),
            monthly_data_folder: "monthly".to_string(),
            hostname: "localhost".to_string(),
            db_name: "weatherstation".to_string(),
            username: "root".to_string(),
            password: "none".to_string()
        });
    }

    #[test]
    fn test_default_ports() {
        assert_eq!(default_ports(), vec![2001, 2002, 2003]);
    }

    #[test]
    fn test_string_to_ports01() {
        assert_eq!(string_to_ports(""), vec![2001, 2002, 2003]);
    }

    #[test]
    fn test_string_to_ports02() {
        assert_eq!(string_to_ports("xyz"), vec![2001, 2002, 2003]);
    }

    #[test]
    fn test_string_to_ports03() {
        assert_eq!(string_to_ports("123"), vec![123]);
    }

    #[test]
    fn test_string_to_ports04() {
        assert_eq!(string_to_ports("123:"), vec![123]);
    }

    #[test]
    fn test_string_to_ports05() {
        assert_eq!(string_to_ports("123:456"), vec![123, 456]);
    }

    #[test]
    fn test_string_to_ports06() {
        assert_eq!(string_to_ports("123: 456"), vec![123, 456]);
    }

    #[test]
    fn test_string_to_ports07() {
        assert_eq!(string_to_ports("123: 456:999:  675"), vec![123, 456, 675, 999]);
    }

    #[test]
    fn test_string_to_ports08() {
        assert_eq!(string_to_ports("123: 456:999:  675: 123"), vec![123, 456, 675, 999]);
    }
}
