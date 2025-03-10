#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

use codecrafters_kafka::{header::KafkaHeader, response::KafkaResponse};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                stream.set_nonblocking(true).unwrap();
                println!("accepted new connection");
                let mut buffer = Vec::new();
                stream.read(&mut buffer).unwrap();
                println!("{}", String::from_utf8(buffer.to_vec()).unwrap());

                // generate response
                let header = KafkaHeader::new_v0(7);
                let response = KafkaResponse::empty(header);
                let response_bytes = KafkaResponse::to_bytes(response);
                stream.write_all(&response_bytes).unwrap();
                println!("response to new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
