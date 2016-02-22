//! Provides the server and handles the incomming requests
//! All ports are handled by the same function

use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::{spawn, sleep};
use std::time::{Duration};
use std::io::prelude::*;

fn handle_client(stream: &mut TcpStream, addr: &SocketAddr) {
    info!("Client socket address: {}", addr);

	let local_addr = stream.local_addr();

	let local_port = match local_addr {
		Ok(addr) => {
			match addr {
				SocketAddr::V4(addr) => Some(addr.port()),
				SocketAddr::V6(addr) => Some(addr.port())
			}
		}
		Err(e) => {
			info!("An error occured: {}", e);
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
		Err(e) => info!("An error occured: {}", e)
	}

	if let Some(port) = local_port {
		info!("Port: {}", port);
	}

}

pub fn start_service(ports: Vec<u16>) {
    let mut listeners = Vec::new();

    for port in ports {
        let maybe_listener = TcpListener::bind(("0.0.0.0", port));
        if let Ok(listener) = maybe_listener {
            listeners.push(listener);
        }
    }

    for listener in listeners {
        spawn(move|| {
            loop {
                let maybe_result = listener.accept();
                if let Ok(result) = maybe_result {
					let (mut stream, addr) = result;
                    handle_client(&mut stream, &addr);
                }
            }
        });
    }

    loop {
        sleep(Duration::new(10000, 0));
    }
}
