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
use time::now;

// Internal modules:
use configuration::Configuration;

fn handle_client(stream: &mut TcpStream, remote_addr: &SocketAddr, all_data_file: &Arc<Mutex<String>>) -> Result<()> {
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

    match all_data_file.lock() {
        Ok(all_data_file) => {
            let mut file_handle = BufWriter::new(try!(OpenOptions::new().write(true).create(true).append(true).open(&*all_data_file)));

            let tm = now();
            let tm = tm.strftime("%Y.%m.%d - %H:%M:%S").unwrap();

            try!(write!(file_handle, "<measure>\n"));
            try!(write!(file_handle, "<port>{}</port>\n", local_port));
            try!(write!(file_handle, "<date_time>{}</date_time>\n", &tm));
            try!(write!(file_handle, "<data>\n"));
            try!(write!(file_handle, "{:?}\n", buffer));
            try!(write!(file_handle, "</data>\n"));
            try!(write!(file_handle, "</measure>\n"));
        },
        Err(e) => info!("Mutex (poison) error: {}", e)
    }

    Ok(())
}

pub fn start_service(config: Configuration) {
    let mut listeners = Vec::new();

    for port in config.ports {
        let listener = TcpListener::bind(("0.0.0.0", port));
        if let Ok(listener) = listener {
            listeners.push(listener);
        }
    }

    let all_data_file = Arc::new(Mutex::new(config.all_data_file.clone()));

    for listener in listeners {
        let all_data_file = all_data_file.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
					let (mut stream, addr) = result;
                    if let Err(io_error) = handle_client(&mut stream, &addr, &all_data_file) {
                        info!("IOError: {}", io_error);
                    }
                }
            }
        });
    }
}
