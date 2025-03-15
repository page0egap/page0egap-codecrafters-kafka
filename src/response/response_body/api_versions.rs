use crate::{
    consts::{
        api_versions::{
            SupportApiVersionsRequestVersion, API_VERSIONS_API_KEY, API_VERSIONS_MAX_VERSION,
            API_VERSIONS_MIN_VERSION,
        },
        describe_topic_partitions::{
            DESCRIBE_TOPIC_MAX_VERSION, DESCRIBE_TOPIC_MIN_VERSION,
            DESCRIBE_TOPIC_PARTITIONS_API_KEY,
        },
    },
    response::{
        self,
        error_code::KafkaError,
        utils::{encode_tagged_fields_to_stream, encode_vec_to_kafka_compact_array_stream},
    },
    common_structs::tagged_field::TaggedField,
};

pub enum KafkaResponseBodyApiVersions {
    V0(ApiVersionsResponseBodyV0),
    V1(ApiVersionsResponseBodyV1),
    V2(ApiVersionsResponseBodyV2),
    V3(ApiVersionsResponseBodyV3),
    V4(ApiVersionsResponseBodyV4),
}

pub struct ApiVersionsResponseBodyV0 {
    error_code: KafkaError,
    api_keys: Vec<ApiKeyRange>,
}

pub struct ApiVersionsResponseBodyV1 {
    error_code: KafkaError,
    api_keys: Vec<ApiKeyRange>,
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
}

pub struct ApiVersionsResponseBodyV2 {
    error_code: KafkaError,
    api_keys: Vec<ApiKeyRange>,
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
}

pub struct ApiVersionsResponseBodyV3 {
    error_code: KafkaError,
    api_keys: Vec<ApiKeyRange>,
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
}

pub struct ApiVersionsResponseBodyV4 {
    error_code: KafkaError,
    api_keys: Vec<ApiKeyRange>,
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
}

struct ApiKeyRange {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiKeyRange {
    fn api_version() -> Self {
        Self {
            api_key: API_VERSIONS_API_KEY,
            min_version: API_VERSIONS_MIN_VERSION,
            max_version: API_VERSIONS_MAX_VERSION,
        }
    }

    fn describe_topic_partitions() -> Self {
        Self {
            api_key: DESCRIBE_TOPIC_PARTITIONS_API_KEY,
            min_version: DESCRIBE_TOPIC_MIN_VERSION,
            max_version: DESCRIBE_TOPIC_MAX_VERSION,
        }
    }
}

impl Default for ApiKeyRange {
    fn default() -> Self {
        Self {
            api_key: API_VERSIONS_API_KEY,
            min_version: API_VERSIONS_MIN_VERSION,
            max_version: API_VERSIONS_MAX_VERSION,
        }
    }
}

impl Into<Vec<u8>> for ApiKeyRange {
    #[inline]
    fn into(self) -> Vec<u8> {
        self.api_key
            .to_be_bytes()
            .into_iter()
            .chain(self.min_version.to_be_bytes())
            .chain(self.max_version.to_be_bytes())
            .collect()
    }
}

impl KafkaResponseBodyApiVersions {
    pub fn error() -> Self {
        Self::V0(ApiVersionsResponseBodyV0 {
            error_code: KafkaError::UnsupportedVersion,
            api_keys: Default::default(),
        })
    }

    pub fn new(api_version: SupportApiVersionsRequestVersion) -> Self {
        // let error_code = if is_in_support_version(api_version) {
        //     KafkaError::None
        // } else {
        //     KafkaError::UnsupportedVersion
        // };
        let error_code = KafkaError::None;
        let mut api_keys = vec![ApiKeyRange::api_version()];
        let throttle_time_ms = 420;
        match api_version {
            SupportApiVersionsRequestVersion::V0 => Self::V0(ApiVersionsResponseBodyV0 {
                error_code,
                api_keys,
            }),
            SupportApiVersionsRequestVersion::V1 => Self::V1(ApiVersionsResponseBodyV1 {
                error_code,
                api_keys,
                throttle_time_ms,
            }),
            SupportApiVersionsRequestVersion::V2 => Self::V2(ApiVersionsResponseBodyV2 {
                error_code,
                api_keys,
                throttle_time_ms,
            }),
            SupportApiVersionsRequestVersion::V3 => Self::V3(ApiVersionsResponseBodyV3 {
                error_code,
                api_keys,
                throttle_time_ms,
            }),
            SupportApiVersionsRequestVersion::V4 => {
                api_keys.push(ApiKeyRange::describe_topic_partitions());
                Self::V4(ApiVersionsResponseBodyV4 {
                    error_code,
                    api_keys,
                    throttle_time_ms,
                })
            }
        }
    }
}

impl Into<Vec<u8>> for KafkaResponseBodyApiVersions {
    #[inline]
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseBodyApiVersions::V0(inner) => inner.into(),
            KafkaResponseBodyApiVersions::V1(inner) => inner.into(),
            KafkaResponseBodyApiVersions::V2(inner) => inner.into(),
            KafkaResponseBodyApiVersions::V3(inner) => inner.into(),
            KafkaResponseBodyApiVersions::V4(inner) => inner.into(),
        }
    }
}

impl Into<Vec<u8>> for ApiVersionsResponseBodyV0 {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let api_keys = self.api_keys;
        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream::<_, _, Vec<u8>>(
                api_keys,
                ApiKeyRange::into,
            ))
            .collect()
    }
}

impl Into<Vec<u8>> for ApiVersionsResponseBodyV1 {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let api_keys = self.api_keys;
        let throttle_time_ms = self.throttle_time_ms;

        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream::<_, _, Vec<u8>>(
                api_keys,
                ApiKeyRange::into,
            ))
            .chain(throttle_time_ms.to_be_bytes())
            .collect()
    }
}

impl Into<Vec<u8>> for ApiVersionsResponseBodyV2 {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let api_keys = self.api_keys;
        let throttle_time_ms = self.throttle_time_ms;

        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream::<_, _, Vec<u8>>(
                api_keys,
                ApiKeyRange::into,
            ))
            .chain(throttle_time_ms.to_be_bytes())
            .collect()
    }
}

impl Into<Vec<u8>> for ApiVersionsResponseBodyV3 {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let api_keys = self.api_keys;
        let throttle_time_ms = self.throttle_time_ms;

        let api_keys_with_empty_tagged_fields: Vec<_> =
            api_keys.into_iter().map(|v| (v, Vec::new())).collect();
        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream(
                api_keys_with_empty_tagged_fields,
                encode_api_key_range_with_tagged_fields_to_vec,
            ))
            .chain(throttle_time_ms.to_be_bytes())
            .chain(response::utils::encode_tagged_fields_to_stream(Vec::new()))
            .collect()
    }
}

impl Into<Vec<u8>> for ApiVersionsResponseBodyV4 {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let api_keys = self.api_keys;
        let throttle_time_ms = self.throttle_time_ms;

        let api_keys_with_empty_tagged_fields: Vec<_> =
            api_keys.into_iter().map(|v| (v, Vec::new())).collect();
        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream(
                api_keys_with_empty_tagged_fields,
                encode_api_key_range_with_tagged_fields_to_vec,
            ))
            .chain(throttle_time_ms.to_be_bytes())
            .chain(response::utils::encode_tagged_fields_to_stream(Vec::new()))
            .collect()
    }
}

fn encode_api_key_range_with_tagged_fields_to_vec(
    input: (ApiKeyRange, Vec<TaggedField>),
) -> Vec<u8> {
    let out: Vec<u8> = input.0.into();
    let tagged_fields_stream = encode_tagged_fields_to_stream(input.1);
    out.into_iter().chain(tagged_fields_stream).collect()
}
