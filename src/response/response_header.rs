use crate::{common_structs::tagged_field::TaggedField, traits::KafkaSeriarize};

use super::utils::write_kafka_tagged_fields_stream;

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

impl KafkaSeriarize for KafkaResponseHeader {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, data: ()) -> std::io::Result<()> {
        match self {
            KafkaResponseHeader::VO(inner) => inner.serialize(writer, data),
            KafkaResponseHeader::V1(inner) => inner.serialize(writer, data),
        }
    }
}

impl KafkaResponseHeaderV0 {
    fn new(correlation_id: i32) -> Self {
        KafkaResponseHeaderV0 { correlation_id }
    }
}

impl KafkaSeriarize for KafkaResponseHeaderV0 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        writer.write_all(&self.correlation_id.to_be_bytes())
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

impl KafkaSeriarize for KafkaResponseHeaderV1 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(self, writer: &mut W, _data: ()) -> std::io::Result<()> {
        writer.write_all(&self.correlation_id.to_be_bytes())?;
        write_kafka_tagged_fields_stream(writer, self.tagged_fields)
    }
}
