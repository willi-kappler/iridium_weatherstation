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

// Internal modules:
use configuration::Configuration;

fn write_data(tm: Tm, local_port: u16, buffer: &Vec<u8>, file_name: &str) -> Result<()> {
    let mut file_handle = BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(file_name)));

    let current_date_time = tm.strftime("%Y.%m.%d - %H:%M:%S").unwrap();

    try!(write!(file_handle, "<measure>\n"));
    try!(write!(file_handle, "<port>{}</port>\n", local_port));
    try!(write!(file_handle, "<date_time>{}</date_time>\n", &current_date_time));
    try!(write!(file_handle, "<data>\n"));
    try!(write!(file_handle, "{:?}\n", buffer));
    try!(write!(file_handle, "</data>\n"));
    try!(write!(file_handle, "</measure>\n\n"));

    Ok(())
}

fn handle_client(stream: &mut TcpStream, remote_addr: &SocketAddr,
    all_data_file: &Arc<Mutex<String>>, monthly_data_folder: &Arc<Mutex<String>>) -> Result<()> {
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
    info!("Bytes: {:?}", buffer);

    // TODO: use str::from_utf8(buf) if data is sent in clear text
    // Otherwise parse binary data to floats

    match all_data_file.lock() {
        Ok(all_data_file) => {
            let tm = now();
            let file_name = format!("{}/{}.txt", local_port, *all_data_file);
            try!(write_data(tm, local_port, &buffer, &file_name));
        },
        Err(e) => info!("Mutex (poison) error (all_data_file): {}", e)
    }

    match monthly_data_folder.lock() {
        Ok(monthly_data_folder) => {
            let tm = now();
            let current_year = tm.strftime("%Y").unwrap();
            let current_month = tm.strftime("%m").unwrap();
            // TODO: create separate folder for year and month in Rust
            let file_name = format!("{}/{}/{}_{}.txt", *monthly_data_folder, local_port, current_year, current_month);
            try!(write_data(tm, local_port, &buffer, &file_name));
        },
        Err(e) => info!("Mutex (poison) error (monthly_data_folder): {}", e)
    }

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
            Err(e) => info!("Network error: {}", e)
        }
    }

    let all_data_file = Arc::new(Mutex::new(config.all_data_file.clone()));
    let monthly_data_folder = Arc::new(Mutex::new(config.monthly_data_folder.clone()));

    for listener in listeners {
        let all_data_file = all_data_file.clone();
        let monthly_data_folder = monthly_data_folder.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
                    let (mut stream, addr) = result;
                    if let Err(io_error) = handle_client(&mut stream, &addr,
                            &all_data_file, &monthly_data_folder) {
                        info!("IOError: {}", io_error);
                    }
                }
            }
        });
    }
}
