use num_enum::TryFromPrimitive;

use crate::consts::{
    api_versions::API_VERSIONS_API_KEY,
    describe_topic_partitions::DESCRIBE_TOPIC_PARTITIONS_API_KEY,
};

#[repr(i16)]
#[derive(Debug, TryFromPrimitive, Clone, Copy)]
pub enum RequestApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = API_VERSIONS_API_KEY,
    DescribeTopicPartitions = DESCRIBE_TOPIC_PARTITIONS_API_KEY,
}
