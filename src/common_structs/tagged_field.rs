use bytes::Bytes;
use integer_encoding::{VarInt, VarIntReader};
use std::io::{self, Read};

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedField {
    field_tag: usize,
    data: Bytes,
}

impl TaggedField {
    pub fn try_from_reader<R: Read>(reader: &mut R) -> Result<Self, io::Error> {
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

impl Into<Vec<u8>> for TaggedField {
    #[inline]
    fn into(self) -> Vec<u8> {
        self.field_tag
            .encode_var_vec()
            .into_iter()
            .chain(self.data.len().encode_var_vec())
            .chain(self.data)
            .collect()
    }
}
