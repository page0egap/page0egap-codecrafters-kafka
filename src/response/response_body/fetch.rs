use crate::{
    request::body::fetch::{FetchRequestBody, FetchRequestBodyV16},
    response::{
        error_code::KafkaError,
        utils::{
            write_compact_vec_u8_stream, write_kafka_compact_array_stream,
            write_kafka_tagged_fields_stream,
        },
    },
    traits::KafkaSeriarize,
};
use byteorder::{BigEndian, WriteBytesExt};

pub enum KafkaResponseBodyFetch {
    V16(FetchResponseBodyV16),
}

impl KafkaResponseBodyFetch {
    pub fn new(request: FetchRequestBody) -> Self {
        match request {
            FetchRequestBody::V16(request) => Self::V16(FetchResponseBodyV16::new(request)),
        }
    }
}

impl KafkaSeriarize for KafkaResponseBodyFetch {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        _data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        match self {
            KafkaResponseBodyFetch::V16(inner) => inner.serialize(writer, ()),
        }
    }
}

pub struct FetchResponseBodyV16 {
    throttle_time_ms: i32,
    error_code: KafkaError,
    session_id: i32,
    responses: Vec<Topic>,
}

impl FetchResponseBodyV16 {
    fn new(request: FetchRequestBodyV16) -> Self {
        if request.topics.is_empty() {
            return Self::empty();
        } else {
            todo!("FetchResponseBodyV16::new")
        }
    }

    fn empty() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: KafkaError::None,
            session_id: 0,
            responses: Vec::new(),
        }
    }
}

impl KafkaSeriarize for FetchResponseBodyV16 {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        _data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        writer.write_i32::<BigEndian>(self.throttle_time_ms)?;
        let error_code: i16 = self.error_code.into();
        writer.write_i16::<BigEndian>(error_code)?;
        writer.write_i32::<BigEndian>(self.session_id)?;
        write_kafka_compact_array_stream(writer, self.responses, |writer, topic| {
            topic.serialize(writer, ())?;
            write_kafka_tagged_fields_stream(writer, Vec::new())
        })?;
        write_kafka_tagged_fields_stream(writer, Vec::new())?;
        Ok(())
    }
}

pub struct Topic {
    topic_id: [u8; 16],
    partitions: Vec<Partition>,
}

impl KafkaSeriarize for Topic {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        _data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        writer.write_all(&self.topic_id)?;
        write_kafka_compact_array_stream(writer, self.partitions, |writer, partition| {
            partition.serialize(writer, ())?;
            write_kafka_tagged_fields_stream(writer, Vec::new())
        })?;
        Ok(())
    }
}

pub struct Partition {
    partition_index: i32,
    error_code: KafkaError,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Vec<AbortedTransaction>,
    preferred_read_replica: i32,
    records: Vec<u8>,
}

impl KafkaSeriarize for Partition {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        _data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        writer.write_i32::<BigEndian>(self.partition_index)?;
        let error_code: i16 = self.error_code.into();
        writer.write_i16::<BigEndian>(error_code)?;
        writer.write_i64::<BigEndian>(self.high_watermark)?;
        writer.write_i64::<BigEndian>(self.last_stable_offset)?;
        writer.write_i64::<BigEndian>(self.log_start_offset)?;
        write_kafka_compact_array_stream(
            writer,
            self.aborted_transactions,
            |writer, transaction| {
                transaction.serialize(writer, ())?;
                write_kafka_tagged_fields_stream(writer, Vec::new())
            },
        )?;
        writer.write_i32::<BigEndian>(self.preferred_read_replica)?;
        write_compact_vec_u8_stream(writer, self.records)?;
        Ok(())
    }
}

pub struct AbortedTransaction {
    producer_id: i64,
    first_offset: i64,
}

impl KafkaSeriarize for AbortedTransaction {
    type Error = std::io::Error;
    type DependentData<'a> = ();

    fn serialize<W: std::io::Write>(
        self,
        writer: &mut W,
        _data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error> {
        writer.write_i64::<BigEndian>(self.producer_id)?;
        writer.write_i64::<BigEndian>(self.first_offset)?;
        Ok(())
    }
}
