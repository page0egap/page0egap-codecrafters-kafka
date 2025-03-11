use std::io::Read;

use api_versions::ApiVersionsRequestBody;

use crate::request::api_key::RequestApiKey;

use super::{error::RequestError, KafkaRequestHeader};

pub mod api_versions;

#[allow(unused)]
pub enum KafkaRequestBody {
    Empty,
    Produce,
    Fetch,
    ApiVersions(ApiVersionsRequestBody),
}

impl KafkaRequestBody {
    pub fn try_parse_body<R>(
        header: &KafkaRequestHeader,
        reader: &mut R,
    ) -> Result<Self, RequestError>
    where
        R: Read,
    {
        let body = match header.request_api_key {
            RequestApiKey::ApiVersions => KafkaRequestBody::ApiVersions(
                ApiVersionsRequestBody::try_from_reader(reader, header)?,
            ),
            _ => KafkaRequestBody::Empty,
        };
        Ok(body)
    }
}
