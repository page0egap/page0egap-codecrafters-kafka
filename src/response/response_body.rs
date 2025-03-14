use api_versions::KafkaResponseBodyApiVersions;
use describe_topic_partitions::KafkaResponseBodyDescribeTopicPartitions;

use crate::request::body::{
    api_versions::ApiVersionsRequestBody,
    describe_topic_partitions::DescribeTopicPartitionsRequestBody,
};

mod api_versions;
mod describe_topic_partitions;

pub enum KafkaResponseBody {
    Empty,
    ApiVersions(KafkaResponseBodyApiVersions),
    DescribeTopicPartitions(KafkaResponseBodyDescribeTopicPartitions),
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

// DescribeTopicPartitions
impl KafkaResponseBody {
    pub fn from_describe_topic_partitions_request_body(
        body: &DescribeTopicPartitionsRequestBody,
    ) -> Self {
        Self::DescribeTopicPartitions(KafkaResponseBodyDescribeTopicPartitions::new(body))
    }
}

impl Into<Vec<u8>> for KafkaResponseBody {
    #[inline]
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseBody::Empty => Vec::new(),
            KafkaResponseBody::ApiVersions(inner) => inner.into(),
            KafkaResponseBody::DescribeTopicPartitions(inner) => inner.into(),
        }
    }
}
