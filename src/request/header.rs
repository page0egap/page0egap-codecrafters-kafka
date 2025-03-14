use byteorder::ReadBytesExt;
use std::{borrow::Cow, io::Cursor};

use byteorder::BigEndian;

use crate::traits::TryParseFromReader;

use super::{
    api_key::RequestApiKey,
    error::{ErrorField, RequestError},
    utils::{try_read_nullable_string, try_read_tagged_fields},
};

#[allow(dead_code)]
pub enum KafkaRequestHeader {
    V0(KafkaRequestHeaderV0),
    V1(KafkaRequestHeaderV1),
    V2(KafkaRequestHeaderV2),
}

pub enum KafkaRequestHeaderVersion {
    V0,
    V1,
    V2,
}

pub struct KafkaRequestHeaderV0 {
    pub request_api_key: RequestApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

pub struct KafkaRequestHeaderV1 {
    pub request_api_key: RequestApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

pub struct KafkaRequestHeaderV2 {
    pub request_api_key: RequestApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl KafkaRequestHeader {
    #[allow(unused)]
    fn try_from_slice(slice: &[u8]) -> Result<Self, RequestError> {
        let mut cursor = Cursor::new(slice);
        Self::try_parse_from_reader(&mut cursor)
    }
}

impl KafkaRequestHeader {
    pub fn request_api_key(&self) -> &RequestApiKey {
        match self {
            KafkaRequestHeader::V0(inner) => &inner.request_api_key,
            KafkaRequestHeader::V1(inner) => &inner.request_api_key,
            KafkaRequestHeader::V2(inner) => &inner.request_api_key,
        }
    }

    pub fn request_api_version(&self) -> i16 {
        match self {
            KafkaRequestHeader::V0(inner) => inner.request_api_version,
            KafkaRequestHeader::V1(inner) => inner.request_api_version,
            KafkaRequestHeader::V2(inner) => inner.request_api_version,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            KafkaRequestHeader::V0(inner) => inner.correlation_id,
            KafkaRequestHeader::V1(inner) => inner.correlation_id,
            KafkaRequestHeader::V2(inner) => inner.correlation_id,
        }
    }

    pub fn client_id(&self) -> Option<&str> {
        match self {
            KafkaRequestHeader::V0(_inner) => None,
            KafkaRequestHeader::V1(inner) => Some(&inner.client_id),
            KafkaRequestHeader::V2(inner) => Some(&inner.client_id),
        }
    }
}

impl TryParseFromReader for KafkaRequestHeader {
    type Error = RequestError;

    fn try_parse_from_reader<R: std::io::Read>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let request_api_key = reader
            .read_i16::<BigEndian>()
            .map_err(|_| ErrorField::from(Cow::from("request_api_key")))?;
        let request_api_version = reader
            .read_i16::<BigEndian>()
            .map_err(|_| ErrorField::from(Cow::from("request_api_version")))?;
        let correlation_id = reader
            .read_i32::<BigEndian>()
            .map_err(|_| ErrorField::from(Cow::from("correlation_id")))?;
        let request_api_key =
            request_api_key
                .try_into()
                .map_err(|_| RequestError::UnsupportedApiKey {
                    api_key: request_api_key,
                    correlation_id,
                })?;
        let header_version = header_version_from_request_api_key(request_api_key);
        match header_version {
            KafkaRequestHeaderVersion::V0 => Ok(KafkaRequestHeader::V0(KafkaRequestHeaderV0 {
                request_api_key,
                request_api_version,
                correlation_id,
            })),
            KafkaRequestHeaderVersion::V1 => {
                let client_id =
                    try_read_nullable_string(reader).map_err(|_| RequestError::InvalidFormat {
                        field: ErrorField::from(Cow::from("client_id")),
                        correlation_id,
                    })?;
                Ok(Self::V1(KafkaRequestHeaderV1 {
                    request_api_key,
                    request_api_version,
                    correlation_id,
                    client_id,
                }))
            }
            KafkaRequestHeaderVersion::V2 => {
                let client_id =
                    try_read_nullable_string(reader).map_err(|_| RequestError::InvalidFormat {
                        field: ErrorField::from(Cow::from("client_id")),
                        correlation_id,
                    })?;

                let _ =
                    try_read_tagged_fields(reader).map_err(|_| RequestError::InvalidFormat {
                        field: ErrorField::from(Cow::from("header tagged field")),
                        correlation_id,
                    })?;

                Ok(Self::V2(KafkaRequestHeaderV2 {
                    request_api_key,
                    request_api_version,
                    correlation_id,
                    client_id,
                }))
            }
        }
    }
}

fn header_version_from_request_api_key(api_key: RequestApiKey) -> KafkaRequestHeaderVersion {
    match api_key {
        RequestApiKey::Produce | RequestApiKey::Fetch => KafkaRequestHeaderVersion::V0,
        RequestApiKey::DescribeTopicPartitions | RequestApiKey::ApiVersions => {
            KafkaRequestHeaderVersion::V2
        }
    }
}
