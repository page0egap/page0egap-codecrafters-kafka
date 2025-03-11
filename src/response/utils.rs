use integer_encoding::VarInt;

#[inline]
pub fn tagged_fields_to_vec(tagged_fields: &[(usize, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    // 写入 tag 数
    out.extend_from_slice(&tagged_fields.len().encode_var_vec());
    // 依次写入每个 tag 的数据
    for (tag, data) in tagged_fields {
        out.extend_from_slice(&tag.encode_var_vec());
        out.extend_from_slice(&data.len().encode_var_vec());
        out.extend_from_slice(data);
    }
    out
}
