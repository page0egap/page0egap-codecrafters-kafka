#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

use codecrafters_kafka::{
    request::{self, body::KafkaRequestBody, error::RequestError, KafkaRequest},
    response::{KafkaResponse, KafkaResponseHeader},
};

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
                println!("accepted new connection");
                while (true) {
                    let request = KafkaRequest::try_from_reader(&mut stream);
                    if let Err(e) = request.as_ref() {
                        match e {
                            RequestError::EOF => break,
                            _ => (),
                        }
                    }
                    println!("recieve new request");
                    // generate response
                    let response = KafkaResponse::from_request(&request);
                    let response_bytes: Vec<u8> = response.into();
                    stream.write_all(&response_bytes).unwrap();
                    println!("response to new request");
                }

                println!("close the connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
