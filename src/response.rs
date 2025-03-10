use crate::request::request_api_key::RequestApiKey;

pub mod error_code;
mod response_body;
mod response_header;

use response_body::KafkaResponseBody;
pub use response_header::KafkaResponseHeader;

use crate::request::KafkaRequest;

// pub struct KafkaResponse

pub struct KafkaResponse {
    message_size: i32,
    header: KafkaResponseHeader,
    body: KafkaResponseBody,
}

impl KafkaResponse {
    pub fn empty(header: KafkaResponseHeader) -> Self {
        KafkaResponse {
            message_size: 0,
            header,
            body: KafkaResponseBody::Empty,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.message_size
            .to_be_bytes()
            .into_iter()
            .chain(self.header.to_vec())
            .chain(self.body.to_vec())
            .collect()
    }
}

impl KafkaResponse {
    pub fn from_request(request: &KafkaRequest) -> Self {
        let header = KafkaResponseHeader::new_v0(request.correlation_id());
        let body = match request.request_api_key() {
            RequestApiKey::Produce => todo!(),
            RequestApiKey::Fetch => todo!(),
            RequestApiKey::ApiVersions => KafkaResponseBody::api_versions(request),
        };
        Self {
            message_size: 0,
            header,
            body,
        }
    }
}
