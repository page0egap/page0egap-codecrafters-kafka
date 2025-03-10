use api_versions::KafkaResponseBodyApiVersions;

use crate::request::KafkaRequest;

mod api_versions;
pub enum KafkaResponseBody {
    Empty,
    ApiVersions(KafkaResponseBodyApiVersions),
}

impl KafkaResponseBody {
    pub fn api_versions(request: &KafkaRequest) -> Self {
        Self::ApiVersions(KafkaResponseBodyApiVersions::new(
            request.request_api_version(),
        ))
    }
}

impl KafkaResponseBody {
    #[inline]
    pub fn to_vec(self) -> Vec<u8> {
        match self {
            KafkaResponseBody::Empty => Vec::new(),
            KafkaResponseBody::ApiVersions(kafka_response_body_api_versions) => 
            kafka_response_body_api_versions.into()
        }
    }
}
