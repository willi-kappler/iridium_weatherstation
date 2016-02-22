//! Provides the server and handles the incomming requests
//! All ports are handled by the same function

// System modules:
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::spawn;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;

// Internal modules:
use configuration::Configuration;

/*
#[derive(Debug)]
enum ServerError {
    TCPAddressError(),
    TCPStreamError(),
    IOOpenError(),
    IOWriteError(),
    MutexError()

}
*/

fn handle_client(stream: &mut TcpStream, remote_addr: &SocketAddr, all_data_file: &Arc<Mutex<String>>) {
    info!("Client socket address: {}", remote_addr);

	let local_port = match stream.local_addr() {
		Ok(local_addr) => {
			match local_addr {
				SocketAddr::V4(local_addr) => Some(local_addr.port()),
				SocketAddr::V6(local_addr) => Some(local_addr.port())
			}
		}
		Err(e) => {
			info!("TCP/IP Address error: {}", e);
			None
		}
	};

	let mut buffer = Vec::new();

	let res = stream.read_to_end(&mut buffer);

	match res {
		Ok(len) => {
			info!("Number of bytes received: {}", len);
			info!("Bytes: {:?}", buffer);
		},
		Err(e) => info!("TCP stream read error: {}", e)
	}

	if let Some(port) = local_port {
		info!("Port: {}", port);

        match all_data_file.lock() {
            Ok(all_data_file_1) => {
                match OpenOptions::new().write(true).create(true).append(true).open(&*all_data_file_1) {
                    Ok(file_handle) => {
                        //file_handle.write_fmt();
                    },
                    Err(e) => info!("IO open error: {}", e)
                }
            },
            Err(e) => info!("Mutex error: {}", e)
        }

	}
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
        let all_data_file_1 = all_data_file.clone();
        spawn(move|| {
            loop {
                let result = listener.accept();
                if let Ok(result) = result {
					let (mut stream, addr) = result;
                    handle_client(&mut stream, &addr, &all_data_file_1);
                }
            }
        });
    }
}
