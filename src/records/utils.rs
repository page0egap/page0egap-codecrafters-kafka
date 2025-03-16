use binrw::BinRead;
use binrw::BinResult;
use binrw::BinWrite;
use binrw::Endian;
use binrw::Error;
use integer_encoding::VarInt;
use integer_encoding::VarIntReader;
use integer_encoding::VarIntWriter;
use std::io::Seek;
use std::io::{Read, Write};

use crate::common_structs::tagged_field::TaggedField;
use crate::traits::KafkaDeseriarize;
use crate::traits::KafkaSeriarize;

pub fn parse_compact_array<'a, R: Read + Seek, T, Args>(
    reader: &mut R,
    endian: Endian,
    args: T::Args<'a>,
) -> BinResult<Vec<T>>
where
    T: BinRead,
    T::Args<'a>: Copy,
{
    let i: usize = reader.read_varint()?;
    let length = if i == 0 { 0 } else { i - 1 };
    let mut results = Vec::with_capacity(length);
    for _ in 0..length {
        let element = BinRead::read_options(reader, endian, args)?;
        results.push(element);
    }
    Ok(results)
}

pub fn write_compact_array<'a, W: Write + Seek, T>(
    array: &impl AsRef<[T]>,
    writer: &mut W,
    endian: Endian,
    args: T::Args<'a>,
) -> BinResult<()>
where
    T: BinWrite,
    T::Args<'a>: Copy,
{
    let array = array.as_ref();
    let length = array.len();
    let write_len = if length == 0 { 0 } else { length + 1 };
    writer.write_varint(write_len)?;
    for element in array {
        BinWrite::write_options(element, writer, endian, args)?;
    }
    Ok(())
}

pub fn parse_tagged_fields<R: Read + Seek>(
    reader: &mut R,
    _endian: Endian,
    _: (),
) -> BinResult<Vec<TaggedField>> {
    let num: usize = reader.read_varint()?;
    let mut results = Vec::new();
    for _ in 0..num {
        let tagged_field = TaggedField::try_parse_from_reader(reader, ())?;
        results.push(tagged_field);
    }
    Ok(results)
}

pub fn write_tagged_fields<V, R: Write + Seek>(
    tagged_fields: &V,
    writer: &mut R,
    _endian: Endian,
    _: (),
) -> BinResult<()>
where
    V: AsRef<[TaggedField]>,
{
    let tagged_fields = tagged_fields.as_ref();
    writer.write_all(&tagged_fields.len().encode_var_vec())?;
    for tagged_field in tagged_fields {
        tagged_field.clone().serialize(writer, ())?;
    }
    Ok(())
}

pub fn parse_compact_string<R: Read + Seek>(
    reader: &mut R,
    _endian: Endian,
    _: (),
) -> BinResult<String> {
    let length: usize = reader.read_varint()?;
    if length == 0 {
        return Ok(String::new());
    }
    let length = length - 1;
    let mut buf = vec![0u8; length];

    // 先读字节
    reader.read_exact(&mut buf)?;

    // 从 UTF-8 转为 String 时可能出错，这里用 map_err 包装为 binrw 的错误
    let result = String::from_utf8(buf).map_err(|utf8_err| Error::AssertFail {
        pos: reader.stream_position().unwrap_or(0),
        message: format!("UTF-8 parse error: {utf8_err}"),
    })?;

    Ok(result)
}

pub fn write_compact_string<S, R: Write + Seek>(
    s: &S,
    writer: &mut R,
    _endian: Endian,
    _: (),
) -> BinResult<()>
where
    S: AsRef<str>,
{
    let vec = s.as_ref().as_bytes();
    let length = vec.len();
    let length = if length == 0 { length } else { length + 1 };
    writer.write_varint(length)?;
    writer.write_all(vec)?;
    Ok(())
}

pub fn parse_vec_u8_with_signed_varint_length<R: Read + Seek>(
    reader: &mut R,
    _endian: Endian,
    _: (),
) -> BinResult<Vec<u8>> {
    let len: i64 = reader.read_varint()?;
    let vec = if len <= 0 {
        Vec::new()
    } else {
        let mut vec = vec![0u8; len as usize];
        reader.read_exact(&mut vec)?;
        vec
    };

    Ok(vec)
}

pub fn write_vec_u8_with_signed_varint_length<W: Write + Seek>(
    slice: &[u8],
    writer: &mut W,
    _endian: Endian,
    _: (),
) -> BinResult<()> {
    writer.write_varint(slice.len() as i64)?;
    writer.write_all(slice)?;
    Ok(())
}
