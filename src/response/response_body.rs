use api_versions::KafkaResponseBodyApiVersions;

use crate::request::body::api_versions::ApiVersionsRequestBody;

mod api_versions;
pub enum KafkaResponseBody {
    Empty,
    ApiVersions(KafkaResponseBodyApiVersions),
}

/// ApiVersions
impl KafkaResponseBody {
    pub fn from_api_versions_request_body(body: &ApiVersionsRequestBody) -> Self {
        Self::ApiVersions(KafkaResponseBodyApiVersions::new(body.get_api_version()))
    }

    pub fn from_api_versions_with_invalid_api_version(_api_version: i16) -> Self {
        Self::ApiVersions(KafkaResponseBodyApiVersions::error())
    }
}

impl Into<Vec<u8>> for KafkaResponseBody {
    #[inline]
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseBody::Empty => Vec::new(),
            KafkaResponseBody::ApiVersions(inner) => inner.into(),
        }
    }
}
