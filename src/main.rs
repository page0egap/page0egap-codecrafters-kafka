#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, OnceLock, RwLock},
    thread::{self, JoinHandle},
};

use codecrafters_kafka::{
    globals::RECORD_BATCHES,
    records::RecordBatch,
    request::{self, body::KafkaRequestBody, error::RequestError, KafkaRequest},
    response::{KafkaResponse, KafkaResponseHeader},
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
    println!("Logs from your program will appear here!");

    let file_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    match RecordBatch::read_batches_from_file(file_path) {
        Ok(batches) => {
            println!("Successfully read {} record", batches.len());
            RECORD_BATCHES.get_or_init(|| Arc::new(RwLock::new(batches)));
        }
        Err(e) => {
            println!("Unsuccessfully read with error: {}", e);
        }
    };
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    // 修改线程创建以使用全局变量
    let mut threads = Vec::new();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // 每个线程都可以访问全局记录
                threads.push(thread::spawn(|| {
                    handle_stream(stream);
                }));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    for thread in threads {
        thread.join().unwrap()
    }
}
