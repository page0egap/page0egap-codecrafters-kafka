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
        fetch::{FETCH_API_KEY, FETCH_MAX_VERSION, FETCH_MIN_VERSION},
    },
    response::{
        error_code::KafkaError,
        utils::{write_kafka_compact_array_stream, write_kafka_tagged_fields_stream},
    },
    traits::KafkaSeriarize,
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

    fn fetch() -> Self {
        Self {
            api_key: FETCH_API_KEY,
            min_version: FETCH_MIN_VERSION,
            max_version: FETCH_MAX_VERSION,
        }
    }
}

impl KafkaSeriarize for ApiKeyRange {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        writer.write_all(&self.api_key.to_be_bytes())?;
        writer.write_all(&self.min_version.to_be_bytes())?;
        writer.write_all(&self.max_version.to_be_bytes())?;
        Ok(())
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
                api_keys.push(ApiKeyRange::fetch());
                Self::V4(ApiVersionsResponseBodyV4 {
                    error_code,
                    api_keys,
                    throttle_time_ms,
                })
            }
        }
    }
}

impl KafkaSeriarize for KafkaResponseBodyApiVersions {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        match self {
            KafkaResponseBodyApiVersions::V0(inner) => inner.serialize(writer, ()),
            KafkaResponseBodyApiVersions::V1(inner) => inner.serialize(writer, ()),
            KafkaResponseBodyApiVersions::V2(inner) => inner.serialize(writer, ()),
            KafkaResponseBodyApiVersions::V3(inner) => inner.serialize(writer, ()),
            KafkaResponseBodyApiVersions::V4(inner) => inner.serialize(writer, ()),
        }
    }
}

impl KafkaSeriarize for ApiVersionsResponseBodyV0 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        let error_code: i16 = self.error_code.into();
        writer.write_all(&error_code.to_be_bytes())?;
        write_kafka_compact_array_stream(writer, self.api_keys, |writer, api_key| {
            api_key.serialize(writer, ())
        })?;
        Ok(())
    }
}

impl KafkaSeriarize for ApiVersionsResponseBodyV1 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        let error_code: i16 = self.error_code.into();
        let throttle_time_ms = self.throttle_time_ms;
        writer.write_all(&error_code.to_be_bytes())?;
        write_kafka_compact_array_stream(writer, self.api_keys, |writer, api_key| {
            api_key.serialize(writer, ())
        })?;
        writer.write_all(&throttle_time_ms.to_be_bytes())?;
        Ok(())
    }
}

impl KafkaSeriarize for ApiVersionsResponseBodyV2 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        let error_code: i16 = self.error_code.into();
        let throttle_time_ms = self.throttle_time_ms;
        writer.write_all(&error_code.to_be_bytes())?;
        write_kafka_compact_array_stream(writer, self.api_keys, |writer, api_key| {
            api_key.serialize(writer, ())
        })?;
        writer.write_all(&throttle_time_ms.to_be_bytes())?;
        Ok(())
    }
}

impl KafkaSeriarize for ApiVersionsResponseBodyV3 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        let error_code: i16 = self.error_code.into();
        let throttle_time_ms = self.throttle_time_ms;
        writer.write_all(&error_code.to_be_bytes())?;
        write_kafka_compact_array_stream(writer, self.api_keys, |writer, api_key| {
            api_key.serialize(writer, ())?;
            write_kafka_tagged_fields_stream(writer, Vec::new())
        })?;
        writer.write_all(&throttle_time_ms.to_be_bytes())?;
        write_kafka_tagged_fields_stream(writer, Vec::new())?;
        Ok(())
    }
}

impl KafkaSeriarize for ApiVersionsResponseBodyV4 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        let error_code: i16 = self.error_code.into();
        let throttle_time_ms = self.throttle_time_ms;
        writer.write_all(&error_code.to_be_bytes())?;
        write_kafka_compact_array_stream(writer, self.api_keys, |writer, api_key| {
            api_key.serialize(writer, ())?;
            write_kafka_tagged_fields_stream(writer, Vec::new())
        })?;
        writer.write_all(&throttle_time_ms.to_be_bytes())?;
        write_kafka_tagged_fields_stream(writer, Vec::new())?;
        Ok(())
    }
}
