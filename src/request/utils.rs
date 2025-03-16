use byteorder::{BigEndian, ReadBytesExt};
use integer_encoding::VarIntReader;
use std::borrow::Cow;
use std::error::Error;
use std::io::{self, Read};
use thiserror::Error;

use crate::common_structs::tagged_field::TaggedField;
use crate::traits::KafkaDeseriarize;

use super::error::{ErrorField, RequestError};

#[derive(Debug, Error)]
pub enum ReadCompactStringError<I: Error> {
    #[error("length {0}")]
    LengthError(io::Error),
    #[error("inner {0}")]
    InnerError(#[from] I),
}

impl ReadCompactStringError<RequestError> {
    pub fn into_request_error<F>(self, length_field: F, correlation_id: i32) -> RequestError
    where
        F: Into<ErrorField>,
    {
        match self {
            ReadCompactStringError::LengthError(_error) => {
                RequestError::invalid_format(length_field, correlation_id)
            }
            ReadCompactStringError::InnerError(inner) => inner,
        }
    }
}

pub fn try_read_vec_from_compact_array<R: Read, Element, FnReadOne, FnError>(
    reader: &mut R,
    f: FnReadOne,
) -> Result<Vec<Element>, ReadCompactStringError<FnError>>
where
    FnReadOne: Fn(&mut R) -> Result<Element, FnError>,
    FnError: std::error::Error,
    FnError: 'static,
{
    let length: usize = reader
        .read_varint()
        .map_err(|e| ReadCompactStringError::LengthError(e))?;
    if length == 0 {
        return Ok(Vec::new());
    }
    let length = length - 1;
    let mut results = Vec::with_capacity(length);
    for _ in 0..length {
        let element = f(reader)?;
        results.push(element);
    }
    Ok(results)
}

pub fn try_read_compact_string<R: Read>(
    reader: &mut R,
) -> Result<String, Box<dyn std::error::Error>> {
    let length: usize = reader.read_varint()?;
    if length == 0usize {
        return Ok(String::new());
    }
    let length = length - 1usize;
    let mut buf = vec![0u8; length];
    reader.read_exact(&mut buf)?;
    let result = String::from_utf8(buf)?;
    Ok(result)
}

pub fn try_read_nullable_string<R: Read>(
    reader: &mut R,
) -> Result<String, Box<dyn std::error::Error>> {
    let length = reader.read_i16::<BigEndian>()?;
    if length == -1 {
        Ok(String::new())
    } else if length < 0 && length != -1 {
        let err = ErrorField::from(Cow::from("invalid nullable string length"));
        Err(Box::new(err))
    } else {
        let length: usize = length as usize;
        let mut buf = vec![0u8; length];
        reader.read_exact(&mut buf)?;
        let result = String::from_utf8(buf)?;
        Ok(result)
    }
}

pub fn try_read_tagged_fields<R: Read>(reader: &mut R) -> Result<Vec<TaggedField>, io::Error> {
    let num: usize = reader.read_varint()?;
    let mut results = Vec::new();
    for _ in 0..num {
        let tagged_field = TaggedField::try_parse_from_reader(reader, ())?;
        results.push(tagged_field);
    }
    Ok(results)
}
