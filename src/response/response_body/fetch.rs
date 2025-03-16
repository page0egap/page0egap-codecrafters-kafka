use crate::{
    globals::RECORD_BATCHES,
    records::RecordBatch,
    request::{
        self,
        body::fetch::{FetchRequestBody, FetchRequestBodyV16},
    },
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
    pub fn new(request: &FetchRequestBody) -> Self {
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
    fn new(request: &FetchRequestBodyV16) -> Self {
        if request.topics.is_empty() {
            return Self::empty();
        } else {
            let throttle_time_ms = 0;
            let error_code = KafkaError::None;
            let session_id = request.session_id;
            let mut topics = Vec::new();
            if let Some(records) = RECORD_BATCHES.get() {
                if let Ok(records_guard) = records.read() {
                    for topic in &request.topics {
                        topics.push(Topic::new(topic, &records_guard));
                    }
                }
            }
            Self {
                throttle_time_ms,
                error_code,
                session_id,
                responses: topics,
            }
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

impl Topic {
    pub fn new(topic: &request::body::fetch::Topic, record_batches: &[RecordBatch]) -> Self {
        let mut is_found = false;
        let mut partitions = Vec::new();
        for record_batch in record_batches {
            record_batch
                .records
                .iter()
                .for_each(|record| match &record.value.payload {
                    crate::records::record_value::ClusterMetadataValue::Topic(topic_record) => {
                        if topic.topic_id == topic_record.uuid {
                            is_found = true;
                        }
                    }
                    crate::records::record_value::ClusterMetadataValue::Partition(
                        partition_record,
                    ) => {
                        if topic.topic_id == partition_record.topic_id {
                            is_found = true;
                            partitions.push(Partition::unknown_topic_partition());
                        }
                    }
                    _ => (),
                });
        }

        match (is_found, partitions.is_empty()) {
            (true, true) => Self::emtpy_topic(topic.topic_id.clone()),
            (true, false) => Self::emtpy_topic(topic.topic_id.clone()),
            (false, _) => Self::no_found(topic.topic_id.clone()),
        }
    }

    fn no_found(topic_id: [u8; 16]) -> Self {
        Self {
            topic_id,
            partitions: vec![Partition::unknown_topic_partition()],
        }
    }

    fn emtpy_topic(topic_id: [u8; 16]) -> Self {
        Self {
            topic_id,
            partitions: vec![Partition::known_topic_emtpy_partition()],
        }
    }
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

impl Partition {
    fn unknown_topic_partition() -> Self {
        Self {
            partition_index: 0,
            error_code: KafkaError::UnknownTopicId,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: Vec::new(),
            preferred_read_replica: -1,
            records: Vec::new(),
        }
    }

    fn known_topic_emtpy_partition() -> Self {
        Self {
            partition_index: 0,
            error_code: KafkaError::None,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: Vec::new(),
            preferred_read_replica: -1,
            records: Vec::new(),
        }
    }
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
