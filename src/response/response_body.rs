use api_versions::KafkaResponseBodyApiVersions;
use describe_topic_partitions::KafkaResponseBodyDescribeTopicPartitions;
use fetch::KafkaResponseBodyFetch;

use crate::{
    request::body::{
        api_versions::ApiVersionsRequestBody,
        describe_topic_partitions::DescribeTopicPartitionsRequestBody, fetch::FetchRequestBody,
    },
    traits::KafkaSeriarize,
};

mod api_versions;
mod describe_topic_partitions;
mod fetch;

pub enum KafkaResponseBody {
    Empty,
    Fetch(KafkaResponseBodyFetch),
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

// Fetch
impl KafkaResponseBody {
    pub fn from_fetch_request_body(body: &FetchRequestBody) -> Self {
        Self::Fetch(KafkaResponseBodyFetch::new(body))
    }
}

impl KafkaSeriarize for KafkaResponseBody {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        match self {
            KafkaResponseBody::Empty => Ok(()),
            KafkaResponseBody::ApiVersions(inner) => inner.serialize(writer, data),
            KafkaResponseBody::DescribeTopicPartitions(inner) => inner.serialize(writer, data),
            KafkaResponseBody::Fetch(inner) => inner.serialize(writer, data),
        }
    }
}
