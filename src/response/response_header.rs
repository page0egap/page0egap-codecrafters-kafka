use crate::common_structs::tagged_field::TaggedField;

use super::utils::encode_tagged_fields_to_stream;

pub enum KafkaResponseHeader {
    VO(KafkaResponseHeaderV0),
    V1(KafkaResponseHeaderV1),
}

pub struct KafkaResponseHeaderV0 {
    correlation_id: i32,
}

pub struct KafkaResponseHeaderV1 {
    correlation_id: i32,
    tagged_fields: Vec<TaggedField>,
}

impl KafkaResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        Self::VO(KafkaResponseHeaderV0::new(correlation_id))
    }

    pub fn new_v1(correlation_id: i32) -> Self {
        Self::V1(KafkaResponseHeaderV1::new(correlation_id))
    }
}

impl Into<Vec<u8>> for KafkaResponseHeader {
    #[inline]
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseHeader::VO(inner) => inner.into(),
            KafkaResponseHeader::V1(inner) => inner.into(),
        }
    }
}

impl KafkaResponseHeaderV0 {
    fn new(correlation_id: i32) -> Self {
        KafkaResponseHeaderV0 { correlation_id }
    }
}

impl Into<Vec<u8>> for KafkaResponseHeaderV0 {
    fn into(self) -> Vec<u8> {
        self.correlation_id.to_be_bytes().to_vec()
    }
}

impl KafkaResponseHeaderV1 {
    fn new(correlation_id: i32) -> Self {
        KafkaResponseHeaderV1 {
            correlation_id,
            tagged_fields: Vec::new(),
        }
    }
}

impl Into<Vec<u8>> for KafkaResponseHeaderV1 {
    fn into(self) -> Vec<u8> {
        self.correlation_id
            .to_be_bytes()
            .into_iter()
            .chain(encode_tagged_fields_to_stream(self.tagged_fields))
            .collect()
    }
}
