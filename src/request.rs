use std::io::{Cursor, Read};

use request_body::KafkaRequestBody;
use byteorder::{BigEndian, ReadBytesExt};
use request_api_key::RequestApiKey;

pub mod request_api_key;
pub mod request_body;

pub struct UnParsedBody;

#[allow(dead_code)]
pub struct KafkaRequest {
    message_size: i32,
    header: KafkaRequestHeader,
    body: KafkaRequestBody,
}

#[allow(dead_code)]
pub struct KafkaRequestHeader {
    request_api_key: RequestApiKey,
    request_api_version: i16,
    correlation_id: i32,
}

// public function
impl KafkaRequest {
    pub fn from_slice(slice: &[u8]) -> Self {
        let mut cursor = Cursor::new(slice);
        let message_size = cursor.read_i32::<BigEndian>().unwrap();
        let header = KafkaRequestHeader::from_reader(&mut cursor);
        let body = KafkaRequestBody::parse_body(&header, &mut cursor);
        KafkaRequest {
            message_size,
            header,
            body,
        }
    }
}

impl KafkaRequest {
    pub fn request_api_key(&self) -> &RequestApiKey {
        &self.header.request_api_key
    }

    pub fn request_api_version(&self) -> i16 {
        self.header.request_api_version
    }

    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id
    }
}

impl KafkaRequestHeader {
    #[allow(unused)]
    fn from_slice(slice: &[u8]) -> Self {
        let mut cursor = Cursor::new(slice);
        Self::from_reader(&mut cursor)
    }

    fn from_reader<R: Read>(reader: &mut R) -> Self {
        let request_api_key = reader.read_u16::<BigEndian>().unwrap();
        let request_api_version = reader.read_i16::<BigEndian>().unwrap();
        let correlation_id = reader.read_i32::<BigEndian>().unwrap();
        KafkaRequestHeader {
            request_api_key: request_api_key.try_into().unwrap(),
            request_api_version,
            correlation_id,
        }
    }
}
