use bytes::Bytes;
use integer_encoding::{VarInt, VarIntReader};
use std::io::{self, Read};

use crate::traits::{KafkaSeriarize, KafkaDeseriarize};

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedField {
    field_tag: usize,
    data: Bytes,
}

impl KafkaDeseriarize for TaggedField {
    type Error = io::Error;
    type DependentData<'a> = ();

    fn try_parse_from_reader<R: Read>(reader: &mut R, _data: ()) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let field_tag: usize = reader.read_varint()?;
        let length = reader.read_varint()?;
        let mut data = Vec::with_capacity(length);
        reader.read_exact(&mut data)?;
        Ok(TaggedField {
            field_tag,
            data: Bytes::from(data),
        })
    }
}

impl KafkaSeriarize for TaggedField {
    type Error = io::Error;
    type DependentData<'a> = ();
    fn serialize<W: io::Write>(self, writer: &mut W, _data: ()) -> io::Result<()> {
        writer.write_all(&self.field_tag.encode_var_vec())?;
        writer.write_all(&self.data.len().encode_var_vec())?;
        writer.write_all(&self.data)?;
        Ok(())
    }
}
