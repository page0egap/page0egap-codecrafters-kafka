use std::iter;

use integer_encoding::VarInt;

use crate::structs::tagged_field::TaggedField;

pub fn encode_string_to_compact_string_stream(input: String) -> Vec<u8> {
    encode_vec_to_kafka_compact_array_stream(input.into_bytes(), |x| iter::once(x))
}

#[inline]
pub fn encode_tagged_fields_to_stream<VEC>(tagged_fields: VEC) -> Vec<u8>
where
    VEC: AsRef<[TaggedField]>,
    VEC: IntoIterator<Item = TaggedField>,
{
    let mut out = Vec::new();
    // 写入 tag 数
    out.extend_from_slice(&tagged_fields.as_ref().len().encode_var_vec());
    // 依次写入每个 tag 的数据
    for tagged_field in tagged_fields {
        let tagged_field_stream: Vec<u8> = tagged_field.into();
        out.extend(tagged_field_stream);
    }
    out
}

#[inline]
pub fn encode_vec_to_kafka_compact_array_stream<T, FnTrans, FnIntoIter>(
    v: Vec<T>,
    f: FnTrans,
) -> Vec<u8>
where
    FnTrans: Fn(T) -> FnIntoIter,
    FnIntoIter: IntoIterator<Item = u8>,
{
    let encode_length = v.len();
    let encode_length = if encode_length == 0 {
        0
    } else {
        encode_length + 1
    };
    let v_encode_stream = v.into_iter().map(|inner| f(inner)).flatten();

    encode_length
        .encode_var_vec()
        .into_iter()
        .chain(v_encode_stream)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::encode_tagged_fields_to_stream;

    #[test]
    fn test_emtpy_vector() {
        let vec = Vec::new();
        let result = encode_tagged_fields_to_stream(vec);
        assert_eq!(result, vec![0u8])
    }
}
