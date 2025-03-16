use std::io;

use integer_encoding::VarInt;

use crate::{common_structs::tagged_field::TaggedField, traits::KafkaSeriarize};

pub fn write_compact_string_stream(
    writer: &mut impl std::io::Write,
    input: impl AsRef<str>,
) -> Result<(), io::Error> {
    write_compact_vec_u8_stream(writer, input.as_ref().as_bytes())
}

pub fn write_compact_vec_u8_stream(
    writer: &mut impl std::io::Write,
    v: impl AsRef<[u8]>,
) -> Result<(), io::Error> {
    let v = v.as_ref();
    let encode_length = v.len();
    let encode_length = if encode_length == 0 {
        0
    } else {
        encode_length + 1
    };
    writer.write_all(&encode_length.encode_var_vec())?;
    writer.write_all(&v)?;
    Ok(())
}

pub fn write_kafka_tagged_fields_stream<W>(
    writer: &mut W,
    tagged_fields: Vec<TaggedField>,
) -> Result<(), io::Error>
where
    W: std::io::Write,
{
    writer.write_all(tagged_fields.len().encode_var_vec().as_slice())?;
    for tagged_field in tagged_fields {
        tagged_field.serialize(writer, ())?;
    }
    Ok(())
}

pub fn write_kafka_compact_array_stream<W, T, FnWrite>(
    writer: &mut W,
    v: Vec<T>,
    mut f: FnWrite,
) -> Result<(), io::Error>
where
    W: std::io::Write,
    FnWrite: FnMut(&mut W, T) -> Result<(), io::Error>,
{
    let encode_length = v.len();
    let encode_length = if encode_length == 0 {
        0
    } else {
        encode_length + 1
    };
    writer.write_all(&encode_length.encode_var_vec()).unwrap();
    for inner in v {
        f(writer, inner)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::response::utils::write_kafka_tagged_fields_stream;

    #[test]
    fn test_emtpy_vector() {
        let vec = Vec::new();
        let mut result = Vec::new();
        write_kafka_tagged_fields_stream(&mut result, vec).unwrap();
        assert_eq!(result, vec![0u8])
    }
}
