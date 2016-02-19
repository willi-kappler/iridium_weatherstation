//! Provides the server and handles the incomming requests
//! All ports are handles by the same function

use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread::{spawn, sleep};
use std::time::{Duration};

fn handle_client((stream, addr): (TcpStream, SocketAddr)) {
        println!("client socket address: {}", addr);
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
                    handle_client(result);
                }
            }
        });
    }

    loop {
        sleep(Duration::new(10000, 0));
    }
}
