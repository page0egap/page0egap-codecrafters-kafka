use std::marker::PhantomData;

use crate::{
    globals::RECORD_BATCHES,
    records::{
        record_value::{ClusterMetadataValue, PartitionRecord},
        RecordBatch,
    },
    request::body::describe_topic_partitions::{
        DescribeTopicPartitionsRequestBody, DescribeTopicPartitionsRequestBodyV0,
    },
    response::{
        error_code::KafkaError,
        utils::{
            encode_string_to_compact_string_stream, encode_tagged_fields_to_stream,
            encode_vec_to_kafka_compact_array_stream,
        },
    },
};

pub enum KafkaResponseBodyDescribeTopicPartitions {
    V0(KafkaResponseBodyDescribeTopicPartitionsV0),
}

pub struct KafkaResponseBodyDescribeTopicPartitionsV0 {
    throttle_time_ms: i32,
    topics: Vec<Topic>,
    next_cursor: PhantomData<String>, // nullable field, here we use empty string
}

pub struct Topic {
    error_code: KafkaError,
    name: String,
    id: [u8; 16],
    is_internal: bool,
    partitions: Vec<Partition>,
    authorized_operation: i32,
}

pub struct Partition {
    error_code: KafkaError,
    index: i32,
    leader_id: i16,
    leader_epoch: i16,
    replicas: Vec<i32>,
    isrs: Vec<i32>,
    eligible_leader_replicas: Vec<i32>,
    last_know_klr: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl KafkaResponseBodyDescribeTopicPartitions {
    pub fn new(request: &DescribeTopicPartitionsRequestBody) -> Self {
        Self::V0(KafkaResponseBodyDescribeTopicPartitionsV0::new(request))
    }
}

impl KafkaResponseBodyDescribeTopicPartitionsV0 {
    fn new(request: &DescribeTopicPartitionsRequestBody) -> Self {
        match request {
            DescribeTopicPartitionsRequestBody::V0(inner) => Self::from_request_v0(inner),
        }
    }

    fn from_request_v0(request: &DescribeTopicPartitionsRequestBodyV0) -> Self {
        let throttle_time_ms = 0;
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in &request.topics {
            if let Some(record_batches) = RECORD_BATCHES.get() {
                let record_batches = record_batches.read();
                if let Ok(record_batches_guard) = record_batches {
                    topics.push(Topic::query_from_record_batches(
                        topic.clone(),
                        &record_batches_guard,
                    ));
                } else {
                    topics.push(Topic::new_unknown(topic.clone()));
                }
            } else {
                topics.push(Topic::new_unknown(topic.clone()));
            }
        }
        Self {
            throttle_time_ms,
            topics,
            next_cursor: PhantomData,
        }
    }
}

impl Topic {
    fn new_unknown(topic: String) -> Self {
        Self {
            error_code: KafkaError::UnknownTopicOrPartition,
            name: topic,
            id: [0u8; 16],
            is_internal: true,
            partitions: Vec::new(),
            authorized_operation: 0,
        }
    }

    fn query_from_record_batches(topic: String, record_batches: &Vec<RecordBatch>) -> Self {
        let mut is_found = false;
        let mut partitions = Vec::new();
        let mut topic_uuid = [0u8; 16];
        for record_batch in record_batches {
            let records = &record_batch.records;
            if records.is_empty() {
                continue;
            }
            let first_record = &records[0].value.payload;
            match first_record {
                ClusterMetadataValue::Topic(topic_record) => {
                    if topic_record.topic_name != topic {
                        continue;
                    }
                    is_found = true;
                    topic_uuid = topic_record.uuid;
                }
                _ => continue,
            }
            for record in records[1..].iter() {
                let record = &record.value.payload;
                if let ClusterMetadataValue::Partition(inner) = record {
                    if inner.topic_id != topic_uuid {
                        continue;
                    }
                    let partition = Partition::from_partition_record(inner);
                    partitions.push(partition);
                }
            }
        }
        if is_found {
            Self {
                error_code: KafkaError::None,
                name: topic,
                id: topic_uuid,
                is_internal: true,
                partitions,
                authorized_operation: 0,
            }
        } else {
            Self::new_unknown(topic)
        }
    }
}

impl Partition {
    fn from_partition_record(partition_record: &PartitionRecord) -> Self {
        let error_code = KafkaError::None;
        let index = partition_record.partition_id;
        let leader_id = partition_record.leader_id;
        let leader_epoch = partition_record.leader_epoch;
        let replicas = partition_record.replicas.clone();
        let isrs = partition_record.isr.clone();
        let eligible_leader_replicas = Vec::new();
        let last_know_klr = Vec::new();
        let offline_replicas = Vec::new();
        Self {
            error_code,
            index,
            leader_id: leader_id as i16,
            leader_epoch: leader_epoch as i16,
            replicas,
            isrs,
            eligible_leader_replicas,
            last_know_klr,
            offline_replicas,
        }
    }
}

impl Into<Vec<u8>> for KafkaResponseBodyDescribeTopicPartitions {
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseBodyDescribeTopicPartitions::V0(inner) => inner.into(),
        }
    }
}

impl Into<Vec<u8>> for KafkaResponseBodyDescribeTopicPartitionsV0 {
    fn into(self) -> Vec<u8> {
        self.throttle_time_ms
            .to_be_bytes()
            .into_iter()
            .chain(encode_vec_to_kafka_compact_array_stream::<_, _, Vec<u8>>(
                self.topics,
                Topic::into,
            ))
            .chain((-1i8).to_be_bytes())
            .chain(encode_tagged_fields_to_stream(Vec::new()))
            .collect()
    }
}

impl Into<Vec<u8>> for Topic {
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        let is_internal: i8 = if self.is_internal { 0 } else { 1 };
        error_code
            .to_be_bytes()
            .into_iter()
            .chain(encode_string_to_compact_string_stream(self.name))
            .chain(self.id)
            .chain(is_internal.to_be_bytes())
            .chain(encode_vec_to_kafka_compact_array_stream::<_, _, Vec<u8>>(
                self.partitions,
                Partition::into,
            ))
            .chain(self.authorized_operation.to_be_bytes())
            .chain(encode_tagged_fields_to_stream(Vec::new()))
            .collect()
    }
}

impl Into<Vec<u8>> for Partition {
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        error_code
            .to_be_bytes()
            .into_iter()
            .chain(self.index.to_be_bytes())
            .chain(self.leader_id.to_be_bytes())
            .chain(self.leader_epoch.to_be_bytes())
            .chain(encode_vec_to_kafka_compact_array_stream(
                self.replicas,
                i32::to_be_bytes,
            ))
            .chain(encode_vec_to_kafka_compact_array_stream(
                self.isrs,
                i32::to_be_bytes,
            ))
            .chain(encode_vec_to_kafka_compact_array_stream(
                self.eligible_leader_replicas,
                i32::to_be_bytes,
            ))
            .chain(encode_vec_to_kafka_compact_array_stream(
                self.last_know_klr,
                i32::to_be_bytes,
            ))
            .chain(encode_vec_to_kafka_compact_array_stream(
                self.offline_replicas,
                i32::to_be_bytes,
            ))
            .chain(encode_tagged_fields_to_stream(Vec::new()))
            .collect()
    }
}
