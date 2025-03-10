use std::io::Read;

use crate::request::request_api_key::RequestApiKey;

use super::KafkaRequestHeader;

#[allow(unused)]
pub enum KafkaRequestBody {
    Empty,
    Produce,
    Fetch,
    ApiVersions,
}

impl KafkaRequestBody {
    pub fn parse_body<R>(header: &KafkaRequestHeader, _reader: &mut R) -> Self
    where
        R: Read,
    {
        match header.request_api_key {
            RequestApiKey::ApiVersions => KafkaRequestBody::ApiVersions,
            _ => KafkaRequestBody::Empty,
        }
    }
}
