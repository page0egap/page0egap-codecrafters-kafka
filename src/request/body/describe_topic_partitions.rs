use crate::{
    request::{
        error::RequestError,
        header::KafkaRequestHeader,
        utils::{try_read_compact_string, try_read_tagged_fields, try_read_vec_from_compact_array},
    },
    traits::KafkaDeseriarize,
};
use byteorder::{BigEndian, ReadBytesExt};

#[derive(Debug)]
pub enum DescribeTopicPartitionsRequestBody {
    V0(DescribeTopicPartitionsRequestBodyV0),
}

#[allow(unused)]
#[derive(Debug)]
pub struct DescribeTopicPartitionsRequestBodyV0 {
    pub topics: Vec<String>,
    pub response_partition_limit: i32,
    cursor: Cursor,
}

#[allow(unused)]
#[derive(Debug)]
struct Cursor {}

impl KafkaDeseriarize for DescribeTopicPartitionsRequestBody {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: std::io::Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::V0(
            DescribeTopicPartitionsRequestBodyV0::try_parse_from_reader(reader, header)?,
        ))
    }
}

impl KafkaDeseriarize for DescribeTopicPartitionsRequestBodyV0 {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: std::io::Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let build_ill_format_error_helper =
            |field: &'static str| RequestError::invalid_format(field, header.correlation_id());

        let topics_helper = |reader: &mut R| {
            let topic = try_read_compact_string(reader)
                .map_err(|_| build_ill_format_error_helper("topic"))?;
            let _ = try_read_tagged_fields(reader)
                .map_err(|_| build_ill_format_error_helper("topics tagged fields"))?;
            Ok::<String, RequestError>(topic)
        };

        let topics =
            try_read_vec_from_compact_array(reader, topics_helper).map_err(|e| match e {
                crate::request::utils::ReadCompactStringError::LengthError(_) => {
                    build_ill_format_error_helper("topics length")
                }
                crate::request::utils::ReadCompactStringError::InnerError(e) => e,
            })?;
        let response_partition_limit = reader
            .read_i32::<BigEndian>()
            .map_err(|_| build_ill_format_error_helper("response_partition_limit"))?;
        let _ = reader
            .read_i8()
            .map_err(|_| build_ill_format_error_helper("cursor"))?;
        let _ = try_read_tagged_fields(reader)
            .map_err(|_| build_ill_format_error_helper("tagged fields"))?;
        Ok(Self {
            topics,
            response_partition_limit,
            cursor: Cursor {},
        })
    }
}
