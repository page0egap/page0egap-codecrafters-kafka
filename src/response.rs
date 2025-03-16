use std::io;

use crate::{
    request::{body::KafkaRequestBody, error::RequestError},
    traits::KafkaSeriarize,
};

pub mod error_code;
mod response_body;
mod response_header;
mod utils;

use response_body::KafkaResponseBody;
pub use response_header::KafkaResponseHeader;

use crate::request::KafkaRequest;

// pub struct KafkaResponse

pub struct KafkaResponse {
    header: KafkaResponseHeader,
    body: KafkaResponseBody,
}

impl KafkaResponse {
    pub fn empty(header: KafkaResponseHeader) -> Self {
        KafkaResponse {
            header,
            body: KafkaResponseBody::Empty,
        }
    }
}

impl KafkaResponse {
    pub fn from_request(request: &Result<KafkaRequest, RequestError>) -> Self {
        match request {
            Ok(request) => Self::new(request),
            Err(error) => Self::new_error_response(error),
        }
    }

    fn new(request: &KafkaRequest) -> Self {
        let mut header = KafkaResponseHeader::new_v0(request.correlation_id());
        let body = match request.request_body() {
            KafkaRequestBody::Empty => todo!(),
            KafkaRequestBody::Produce => todo!(),
            KafkaRequestBody::ApiVersions(body) => {
                KafkaResponseBody::from_api_versions_request_body(body)
            }
            KafkaRequestBody::DescribeTopicPartitions(body) => {
                header = KafkaResponseHeader::new_v1(request.correlation_id());
                KafkaResponseBody::from_describe_topic_partitions_request_body(body)
            }
            KafkaRequestBody::Fetch(body) => KafkaResponseBody::from_fetch_request_body(body),
        };
        Self { header, body }
    }

    // TODO: repair UnsupportedApiKey response body emtpy
    // TODO: repair InvalidFormat response body empty
    fn new_error_response(request_error: &RequestError) -> Self {
        match request_error {
            RequestError::UnsupportedVersion {
                api_version,
                correlation_id,
            } => Self {
                header: KafkaResponseHeader::new_v0(*correlation_id),
                body: KafkaResponseBody::from_api_versions_with_invalid_api_version(*api_version),
            },
            RequestError::UnsupportedApiKey {
                api_key: _,
                correlation_id,
            } => Self {
                header: KafkaResponseHeader::new_v0(*correlation_id),
                body: KafkaResponseBody::Empty,
            },
            RequestError::InvalidFormat {
                field: _,
                correlation_id,
            } => Self {
                header: KafkaResponseHeader::new_v0(*correlation_id),
                body: KafkaResponseBody::Empty,
            },
            RequestError::InvalidFormatWithoutCId(_error_field) => Self {
                header: KafkaResponseHeader::new_v0(-1),
                body: KafkaResponseBody::Empty,
            },
        }
    }
}

impl KafkaSeriarize for KafkaResponse {
    type Error = io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, data: ()) -> std::io::Result<()> {
        let mut buf = Vec::new();
        self.header.serialize(&mut buf, data)?;
        self.body.serialize(&mut buf, data)?;
        let message_size = buf.len() as i32;
        writer.write_all(&message_size.to_be_bytes())?;
        writer.write_all(&buf)?;
        Ok(())
    }
}
