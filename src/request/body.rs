use std::io::Read;

use api_versions::ApiVersionsRequestBody;
use describe_topic_partitions::DescribeTopicPartitionsRequestBody;
use fetch::FetchRequestBody;

use crate::{request::api_key::RequestApiKey, traits::KafkaDeseriarize};

use super::{error::RequestError, KafkaRequestHeader};

pub mod api_versions;
pub mod describe_topic_partitions;
pub mod fetch;

#[allow(unused)]
pub enum KafkaRequestBody {
    Empty,
    Produce,
    Fetch(FetchRequestBody),
    ApiVersions(ApiVersionsRequestBody),
    DescribeTopicPartitions(DescribeTopicPartitionsRequestBody),
}

impl KafkaDeseriarize for KafkaRequestBody {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let body = match header.request_api_key() {
            RequestApiKey::ApiVersions => KafkaRequestBody::ApiVersions(
                ApiVersionsRequestBody::try_parse_from_reader(reader, header)?,
            ),
            RequestApiKey::DescribeTopicPartitions => KafkaRequestBody::DescribeTopicPartitions(
                DescribeTopicPartitionsRequestBody::try_parse_from_reader(reader, header)?,
            ),
            RequestApiKey::Fetch => KafkaRequestBody::Fetch(
                FetchRequestBody::try_parse_from_reader(reader, header)?,
            ),
            _ => KafkaRequestBody::Empty,
        };
        Ok(body)
    }
}
