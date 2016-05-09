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
use configuration::{Configuration, HEADER_LENGTH};

fn write_xml_data(tm: Tm, local_port: u16, buffer: &[u8], file_name: &str) -> Result<()> {
    let mut file_handle = BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.xml", file_name))));

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

fn write_csv_data(buffer: &[u8], file_name: &str) -> Result<()> {
    let mut file_handle = BufWriter::new(try!(OpenOptions::new()
        .write(true).create(true).append(true).open(format!("{}.csv", file_name))));

    for i in 3..buffer.len() {
        try!(write!(file_handle, "{}", buffer[i].to_string()));
    }

    try!(write!(file_handle, "\n"));

    Ok(())
}

fn port_to_station(port: u16) -> String{
    match port {
        2100 => "2100_Na".to_string(),
        2101 => "2101_SG".to_string(),
        2102 => "2102_PdA".to_string(),
        2103 => "2103_LC".to_string(),
        _ => "unknown".to_string()
    }
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

    // TODO: use str::from_utf8(buf) if data is sent in clear text
    // Otherwise parse binary data to floats

    // base16 2 byte floats:
    // https://en.wikipedia.org/wiki/Half-precision_floating-point_format
    // https://github.com/sgothel/jogl/blob/master/src/jogl/classes/com/jogamp/opengl/math/Binary16.java
    // https://books.google.de/books?id=FPlICAAAQBAJ&pg=PA84&lpg=PA84&dq=binary16&source=bl&ots=0FAzD4XOqn&sig=98h_pzPlLzUXjB4uY1T8MRIZOnA&hl=de&sa=X&ved=0ahUKEwjkpvXU5ZzLAhVD9HIKHQOfAxYQ6AEITzAH#v=onepage&q=binary16&f=false
    // http://www.gamedev.net/topic/557338-ieee-754-2008-binary-16-inaccuracy-in-wikipedia/

    if buffer.len() > HEADER_LENGTH {
        let (buffer_left, buffer_right) = buffer.split_at(HEADER_LENGTH);

        let str_header = String::from_utf8_lossy(buffer_left);
        let str_data = String::from_utf8_lossy(buffer_right);

        info!("Header: {:?}", buffer_left);
        info!("Data: {:?}", buffer_right);

        info!("Header (ASCII): '{}'", str_header);
        info!("Data (ASCII): '{}'", str_data);

        let station_folder = port_to_station(local_port);

        match all_data_file.lock() {
            Ok(all_data_file) => {
                let tm = now();
                let file_name = format!("{}/{}", station_folder, *all_data_file);
                try!(write_xml_data(tm, local_port, buffer_right, &file_name));
                try!(write_csv_data(buffer_right, &file_name));
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
                try!(write_xml_data(tm, local_port, buffer_right, &file_name));
                try!(write_csv_data(buffer_right, &file_name));
            },
            Err(e) => info!("Mutex (poison) error (monthly_data_folder): {}", e)
        }
    } else if buffer.len() < HEADER_LENGTH {
        info!("Invalid header (less than {} bytes received)!", HEADER_LENGTH);
        info!("Bytes: {:?}", buffer);
        let str_buffer = String::from_utf8_lossy(&buffer);
        info!("Bytes (ASCII): '{}'", str_buffer);
    } else { // buffer.len() == HEADER_LENGTH -> no data, only header
        info!("No data received, just header.");
        info!("Bytes: {:?}", buffer);
        let str_buffer = String::from_utf8_lossy(&buffer);
        info!("Bytes (ASCII): '{}'", str_buffer);
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
