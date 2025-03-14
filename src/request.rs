use std::io::{self, Cursor, Read};

use api_key::RequestApiKey;
use body::KafkaRequestBody;
use byteorder::{BigEndian, ReadBytesExt};
use error::RequestError;
use header::KafkaRequestHeader;

use crate::traits::TryParseFromReader;

pub mod api_key;
pub mod body;
pub mod error;
pub mod header;
mod utils;

pub struct UnParsedBody;

#[allow(dead_code)]
pub struct KafkaRequest {
    header: KafkaRequestHeader,
    body: KafkaRequestBody,
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
        let header = KafkaRequestHeader::try_parse_from_reader(reader).map_err(|e| {
            dbg!("header is invalid! {e}");
            e
        })?;
        let body = KafkaRequestBody::try_parse_body(&header, reader).map_err(|e| {
            dbg!("body is invalid! {:?}", &e);
            e
        })?;
        Ok(KafkaRequest { header, body })
    }
}

impl KafkaRequest {
    #[inline]
    pub fn request_api_key(&self) -> &RequestApiKey {
        &self.header.request_api_key()
    }

    #[inline]
    pub fn request_api_version(&self) -> i16 {
        self.header.request_api_version()
    }

    #[inline]
    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id()
    }

    #[inline]
    pub fn request_body(&self) -> &KafkaRequestBody {
        &self.body
    }
}
