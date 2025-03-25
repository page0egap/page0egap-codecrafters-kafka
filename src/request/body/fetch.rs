use std::io;

use byteorder::{BigEndian, ReadBytesExt};

use crate::{
    consts::fetch::SupportFetchRequestVersion,
    request::{
        error::RequestError,
        header::KafkaRequestHeader,
        utils::{try_read_compact_string, try_read_tagged_fields, try_read_vec_from_compact_array},
    },
    traits::KafkaDeseriarize,
};

#[derive(Debug)]
pub enum FetchRequestBody {
    V16(FetchRequestBodyV16),
}

impl KafkaDeseriarize for FetchRequestBody {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: io::Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let version = header.request_api_version();
        let version: SupportFetchRequestVersion = header
            .request_api_version()
            .try_into()
            .map_err(|_| RequestError::unsupported_version(version, header.correlation_id()))?;
        let body = match version {
            SupportFetchRequestVersion::V16 => {
                FetchRequestBody::V16(FetchRequestBodyV16::try_parse_from_reader(reader, header)?)
            }
            _ => {
                return Err(RequestError::unsupported_version(
                    version.into(),
                    header.correlation_id(),
                ))
            }
        };
        Ok(body)
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct FetchRequestBodyV16 {
    ax_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    pub session_id: i32,
    session_epoch: i32,
    pub topics: Vec<Topic>,
    forgotten_topics: Vec<ForgettenTopic>,
    rack_id: String,
}

impl KafkaDeseriarize for FetchRequestBodyV16 {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: io::Read>(
        reader: &mut R,
        data: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let correlation_id = data.correlation_id();
        let ax_wait_ms = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("fetch ax_wait_ms", correlation_id))?;

        let min_bytes = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("fetch min_bytes", correlation_id))?;

        let max_bytes = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("fetch max_bytes", correlation_id))?;

        let isolation_level = reader
            .read_i8()
            .map_err(|_| RequestError::invalid_format("fetch isolation_level", correlation_id))?;

        let session_id = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("fetch session_id", correlation_id))?;

        let session_epoch = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("fetch session_epoch", correlation_id))?;

        let topics =
            try_read_vec_from_compact_array(reader, |r| Topic::try_parse_from_reader(r, data))
                .map_err(|e| e.into_request_error("topics length", correlation_id))?;

        let forgotten_topics = try_read_vec_from_compact_array(reader, |r| {
            ForgettenTopic::try_parse_from_reader(r, data)
        })
        .map_err(|e| e.into_request_error("forgotten_topics length", correlation_id))?;

        let rack_id = try_read_compact_string(reader)
            .map_err(|_| RequestError::invalid_format("rack_id", correlation_id))?;
        let _ = try_read_tagged_fields(reader)
            .map_err(|_| RequestError::invalid_format("fetch tagged_fields", correlation_id))?;

        Ok(Self {
            ax_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct Topic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<Partition>,
}

impl KafkaDeseriarize for Topic {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: io::Read>(
        reader: &mut R,
        data: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let correlation_id = data.correlation_id();
        let mut topic_id = [0u8; 16];
        reader
            .read_exact(&mut topic_id)
            .map_err(|_| RequestError::invalid_format("topic topic_id", correlation_id))?;

        let partitions =
            try_read_vec_from_compact_array(reader, |r| Partition::try_parse_from_reader(r, data))
                .map_err(|e| e.into_request_error("parition length", correlation_id))?;

        let _ = try_read_tagged_fields(reader)
            .map_err(|_| RequestError::invalid_format("topic tagged_fields", correlation_id))?;

        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct Partition {
    pub index: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

impl KafkaDeseriarize for Partition {
    type Error = RequestError;
    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: std::io::Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let correlation_id = header.correlation_id();
        let index = reader
            .read_i32::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("partition_id", correlation_id))?;
        let current_leader_epoch = reader.read_i32::<BigEndian>().map_err(|_| {
            RequestError::invalid_format("partition current leader epoch", correlation_id)
        })?;
        let fetch_offset = reader
            .read_i64::<BigEndian>()
            .map_err(|_| RequestError::invalid_format("partition fetch_offset", correlation_id))?;
        let last_fetched_epoch = reader.read_i32::<BigEndian>().map_err(|_| {
            RequestError::invalid_format("partition last_fetched_epoch", correlation_id)
        })?;
        let log_start_offset = reader.read_i64::<BigEndian>().map_err(|_| {
            RequestError::invalid_format("partition log_start_offset", correlation_id)
        })?;
        let partition_max_bytes = reader.read_i32::<BigEndian>().map_err(|_| {
            RequestError::invalid_format("partition partition_max_bytes", correlation_id)
        })?;
        let _ = try_read_tagged_fields(reader)
            .map_err(|_| RequestError::invalid_format("partition tagged_fields", correlation_id))?;
        Ok(Self {
            index,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
        })
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct ForgettenTopic {
    topic_id: [u8; 16],
    partitions: Vec<i32>,
}

impl KafkaDeseriarize for ForgettenTopic {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: io::Read>(
        reader: &mut R,
        data: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let correlation_id = data.correlation_id();
        let mut topic_id = [0u8; 16];
        reader.read_exact(&mut topic_id).map_err(|_| {
            RequestError::invalid_format("forgotten topic topic_id", correlation_id)
        })?;
        let partitions = try_read_vec_from_compact_array(reader, |r| r.read_i32::<BigEndian>())
            .map_err(|_| {
                RequestError::invalid_format("forgotten topic topic_id", correlation_id)
            })?;
        let _ = try_read_tagged_fields(reader).map_err(|_| {
            RequestError::invalid_format("forgotten topic tagged_fields", correlation_id)
        })?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}
