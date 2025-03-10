use std::{io::Cursor, marker::PhantomData};

use byteorder::{BigEndian, ReadBytesExt};

#[allow(dead_code)]
pub struct KafkaRequest {
    message_size: i32,
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    body: PhantomData<()>,
}

impl KafkaRequest {
    pub fn from_slice(slice: &[u8]) -> Self {
        let mut cursor = Cursor::new(slice);
        let message_size = cursor.read_i32::<BigEndian>().unwrap();
        let request_api_key = cursor.read_i16::<BigEndian>().unwrap();
        let request_api_version = cursor.read_i16::<BigEndian>().unwrap();
        let correlation_id = cursor.read_i32::<BigEndian>().unwrap();
        KafkaRequest {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
            body: PhantomData,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }
}
