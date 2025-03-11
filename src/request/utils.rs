use integer_encoding::VarIntReader;
use std::io::Read;

pub fn read_compact_string<R: Read>(reader: &mut R) -> Result<String, Box<dyn std::error::Error>> {
    let length = reader.read_varint()?;
    let mut buf = vec![0u8; length];
    reader.read_exact(&mut buf)?;
    let result = String::from_utf8(buf)?;
    Ok(result)
}

pub fn read_tagged_fields<R: Read>(
    reader: &mut R,
) -> Result<Vec<(usize, Vec<u8>)>, Box<dyn std::error::Error>> {
    let num: usize = reader.read_varint()?;
    let mut results = Vec::new();
    for _ in 0..num {
        let field_tag: usize = reader.read_varint()?;
        let length = reader.read_varint()?;
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data)?;
        results.push((field_tag, data));
    }
    Ok(results)
}
