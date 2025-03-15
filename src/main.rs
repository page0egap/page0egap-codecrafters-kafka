#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
};

use codecrafters_kafka::{
    records::RecordBatch, request::{self, body::KafkaRequestBody, error::RequestError, KafkaRequest}, response::{KafkaResponse, KafkaResponseHeader}
};

fn handle_stream(mut stream: TcpStream) {
    println!("accepted new connection");
    loop {
        let request = KafkaRequest::try_from_reader(&mut stream);
        let request = match request {
            Ok(r) => r,
            Err(_e) => {
                dbg!("stream is eof");
                break;
            }
        };
        println!("recieve new request");
        // generate response
        let response = KafkaResponse::from_request(&request);
        let response_bytes: Vec<u8> = response.into();
        stream.write_all(&response_bytes).unwrap();
        println!("response to new request");
    }

    println!("close the connection");
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let file_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let record_batches = RecordBatch::read_batches_from_file(file_path);
    dbg!(record_batches.is_ok());
    record_batches.unwrap();

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    let mut threads = Vec::new();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                threads.push(thread::spawn(|| {
                    handle_stream(stream);
                }));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    // clear all threads
    for thread in threads {
        thread.join().unwrap()
    }
}
