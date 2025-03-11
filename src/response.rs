use crate::request::error::RequestError;

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
        let header = KafkaResponseHeader::new_v0(request.correlation_id());
        let body = match request.request_body() {
            crate::request::body::KafkaRequestBody::Empty => todo!(),
            crate::request::body::KafkaRequestBody::Produce => todo!(),
            crate::request::body::KafkaRequestBody::Fetch => todo!(),
            crate::request::body::KafkaRequestBody::ApiVersions(body) => {
                KafkaResponseBody::from_api_versions_request_body(body)
            }
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
        }
    }
}

impl Into<Vec<u8>> for KafkaResponse {
    #[inline]
    fn into(self) -> Vec<u8> {
        let header_vec: Vec<u8> = self.header.into();
        let body_vec: Vec<u8> = self.body.into();
        let message_size = body_vec.len();
        message_size.to_be_bytes()
            .into_iter()
            .chain(header_vec)
            .chain(body_vec)
            .collect()
    }
}
