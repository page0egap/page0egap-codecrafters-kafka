use std::io::{self, Cursor, Read};

use api_key::RequestApiKey;
use body::KafkaRequestBody;
use byteorder::{BigEndian, ReadBytesExt};
use error::RequestError;

pub mod api_key;
pub mod body;
pub mod error;
mod utils;

pub struct UnParsedBody;

#[allow(dead_code)]
pub struct KafkaRequest {
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
    pub fn try_from_reader<R: Read>(
        reader: &mut R,
    ) -> Result<Result<Self, RequestError>, io::Error> {
        let message_size = reader.read_i32::<BigEndian>()?;
        let mut buf = vec![0u8; message_size as usize];
        reader.read_exact(&mut buf)?;

        let mut reader = Cursor::new(buf);
        Ok(Self::parse_header_and_body(&mut reader))
    }

    fn parse_header_and_body<R: Read>(reader: &mut R) -> Result<Self, RequestError> {
        let header = KafkaRequestHeader::try_from_reader(reader).map_err(|e| {
            dbg!("header is invalid! {e}");
            e
        })?;
        let body = KafkaRequestBody::try_parse_body(&header, reader).map_err(|e| {
            dbg!("body is invalid! {e}");
            e
        })?;
        Ok(KafkaRequest { header, body })
    }
}

impl KafkaRequest {
    #[inline]
    pub fn request_api_key(&self) -> &RequestApiKey {
        &self.header.request_api_key
    }

    #[inline]
    pub fn request_api_version(&self) -> i16 {
        self.header.request_api_version
    }

    #[inline]
    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    #[inline]
    pub fn request_body(&self) -> &KafkaRequestBody {
        &self.body
    }
}

impl KafkaRequestHeader {
    #[allow(unused)]
    fn try_from_slice(slice: &[u8]) -> Result<Self, RequestError> {
        let mut cursor = Cursor::new(slice);
        Self::try_from_reader(&mut cursor)
    }

    fn try_from_reader<R: Read>(reader: &mut R) -> Result<Self, RequestError> {
        let request_api_key = reader.read_u16::<BigEndian>().unwrap();
        let request_api_version = reader.read_i16::<BigEndian>().unwrap();
        let correlation_id = reader.read_i32::<BigEndian>().unwrap();
        Ok(KafkaRequestHeader {
            request_api_key: request_api_key.try_into().map_err(|_| {
                RequestError::UnsupportedApiKey {
                    api_key: request_api_key,
                    correlation_id,
                }
            })?,
            request_api_version,
            correlation_id,
        })
    }
}
