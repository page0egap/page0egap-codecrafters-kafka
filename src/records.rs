use std::io::Cursor;

use binrw::{
    binread,
    io::{Read, Seek, Write},
    BinRead, BinResult, BinWrite, Endian, Error,
};

use integer_encoding::VarIntReader;
use integer_encoding::VarIntWriter;
use record_header::RecordHeader;
use record_value::ClusterMetadataRecord;
use utils::{parse_vec_u8_with_signed_varint_length, write_vec_u8_with_signed_varint_length};

pub mod record_header;
pub mod record_value;
mod utils;

#[binread]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)] // 指定使用大端序，与 Kafka 协议字节序保持一致
pub struct RecordBatch {
    pub base_offset: i64, // int64
    #[br(temp)]
    __batch_length: i32, // int32
    pub partition_leader_epoch: i32, // int32

    #[br(temp)]
    #[br(assert(__magic == 0x02i8))]
    __magic: i8,

    pub crc: u32,
    pub attributes: i16,        // int16 (bit flags)
    #[br(temp)]
    __last_offset_delta: i32, // int32
    pub base_timestamp: i64,    // int64
    pub max_timestamp: i64,     // int64
    pub producer_id: i64,       // int64
    pub producer_epoch: i16,    // int16
    pub base_sequence: i32,     // int32

    #[br(temp)]
    __records_length: i32,

    // 读取时，会用 __records_length 来决定要解析多少条 Record
    #[br(count = __records_length)]
    pub records: Vec<Record>,
}

impl BinWrite for RecordBatch {
    type Args<'a> = ();

    fn write_options<'a, W: Write + Seek>(
        &self,
        writer: &mut W,
        endian: Endian,
        _args: Self::Args<'a>,
    ) -> BinResult<()> {
        // crc 之后的字段，先写入一个临时缓冲，用于计算 crc
        let mut batch_after_crc = Vec::new();
        let mut batch_after_crc_cursor = Cursor::new(&mut batch_after_crc);
        // crc 之后的字段，写入 batch_after_crc_cursor，用于计算 crc
        self.attributes
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        // last_offset_delta = records.len() - 1
        let last_offset_delta = self.records.len() as i32 - 1;
        last_offset_delta.write_options(&mut batch_after_crc_cursor, endian, ())?;
        self.base_timestamp
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        self.max_timestamp
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        self.producer_id
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        self.producer_epoch
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        self.base_sequence
            .write_options(&mut batch_after_crc_cursor, endian, ())?;
        // 计算 records 长度
        (self.records.len() as i32).write_options(&mut batch_after_crc_cursor, endian, ())?;
        // 写 records
        for record in &self.records {
            record.write_options(&mut batch_after_crc_cursor, endian, ())?;
        }
        // 计算 crc, The CRC32-C (Castagnoli) polynomial is used for the computation.
        // 使用crc库计算crc
        let crc = crc32c::crc32c(&batch_after_crc);
        // 写入 开头字段
        self.base_offset.write_options(writer, endian, ())?;
        // 计算 batch_length, 为batch_length之后字段的大小
        let batch_length = batch_after_crc.len() as i32 + 9; // 9 是除了 batch_length 之外的字段大小
        batch_length.write_options(writer, endian, ())?;
        self.partition_leader_epoch
            .write_options(writer, endian, ())?;
        (0x02i8).write_options(writer, endian, ())?; // 写入 magic
                                                     // 写入 crc
        crc.write_options(writer, endian, ())?;
        // 写入 crc 之后的字段
        writer.write_all(&batch_after_crc)?;
        Ok(())
    }
}

impl RecordBatch {
    pub fn read_batches_from<R>(reader: &mut R) -> Result<Vec<Self>, Box<dyn std::error::Error>>
    where
        R: Read + Seek,
    {
        let mut batches = Vec::new();
        let mut batch_count = 0;

        // Try to read multiple RecordBatches until EOF
        loop {
            match Self::read(reader) {
                Ok(batch) => {
                    batch_count += 1;
                    println!(
                        "Successfully read RecordBatch #{} with CRC: {:08X}",
                        batch_count, batch.crc
                    );
                    // batch.print_summary();
                    batches.push(batch);
                }
                Err(e) => {
                    // Check if we've reached the end of the stream
                    let reached_end = match reader.stream_position() {
                        Ok(current_pos) => {
                            match reader.seek(std::io::SeekFrom::End(0)) {
                                Ok(total_size) => {
                                    // Seek back to where we were
                                    let _ = reader.seek(std::io::SeekFrom::Start(current_pos));

                                    // If we're at or very near the end (within 4 bytes), consider it done
                                    let remaining = total_size as i64 - current_pos as i64;
                                    remaining <= 4
                                }
                                Err(_) => {
                                    println!("Cannot determine total stream size");
                                    false
                                }
                            }
                        }
                        Err(_) => {
                            println!("Cannot determine current stream position");
                            false
                        }
                    };

                    // If we've reached the end or if it's EOF error, exit normally
                    if reached_end
                        || matches!(e, Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
                    {
                        println!("Reading complete, total {} batches", batch_count);
                        break;
                    }

                    // Otherwise it's a genuine error
                    println!("Parsing error: {:?}", e);

                    // We might try to skip ahead a bit to recover, but for simplicity let's just break
                    println!("Stopping due to parse error");
                    break;
                }
            }
        }

        Ok(batches)
    }

    /// Read multiple RecordBatches from a file at the specified path
    pub fn read_batches_from_file(path: &str) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        use std::fs::File;
        use std::io::BufReader;

        println!("Reading file: {}", path);

        // Open file
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Call the generic method to read from the reader
        Self::read_batches_from(&mut reader)
    }

    /// Print summary information for RecordBatch
    pub fn print_summary(&self) {
        println!("RecordBatch:");
        println!("  base_offset: {}", self.base_offset);
        println!("  partition_leader_epoch: {}", self.partition_leader_epoch);
        println!("  attributes: {:04X}", self.attributes);
        println!("  records count: {}", self.records.len());

        for (i, record) in self.records.iter().enumerate() {
            println!("  Record #{}", i);
            println!("    offset_delta: {}", record.offset_delta);
            println!("    timestamp_delta: {}", record.timestamp_delta);
            println!("    key length: {}", record.key.len());

            // Print content based on record type
            match &record.value.payload {
                record_value::ClusterMetadataValue::BrokerRegistration(_) => {
                    println!("    type: BrokerRegistration")
                }
                record_value::ClusterMetadataValue::Topic(_) => println!("    type: Topic"),
                record_value::ClusterMetadataValue::FeatureLevel(_) => {
                    println!("    type: FeatureLevel")
                }
                record_value::ClusterMetadataValue::Partition(_) => println!("    type: Partition"),
            }

            println!("    headers count: {}", record.headers.len());
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,

    pub key: Vec<u8>,
    pub value: ClusterMetadataRecord,
    pub headers: Vec<RecordHeader>,
}

/// 自定义解析：先读 record_length，再基于它的大小来限制读取其余字段
impl BinRead for Record {
    type Args<'a> = ();

    fn read_options<'a, R: Read + Seek>(
        reader: &mut R,
        endian: Endian,
        _args: Self::Args<'a>,
    ) -> BinResult<Self> {
        // 1. 先读取 record_length
        let record_length: i64 = reader.read_varint()?;
        let start_pos = reader.stream_position()?;

        // 2. 依序读取其他字段
        let attributes = i8::read_options(reader, endian, ())?;
        let timestamp_delta: i64 = reader.read_varint()?;
        let offset_delta = reader.read_varint()?;

        let key = parse_vec_u8_with_signed_varint_length(reader, endian, ())?;
        let value = parse_vec_u8_with_signed_varint_length(reader, endian, ())?;
        let mut value_cursor = Cursor::new(value);
        let value = ClusterMetadataRecord::read_options(&mut value_cursor, endian, ())?;

        // 读取 headers
        let header_count: usize = reader.read_varint()?;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            headers.push(RecordHeader::read_options(reader, endian, ())?);
        }

        // 3. 检查实际读取的字节数 与 record_length 是否一致
        let end_pos = reader.stream_position()?;
        let actual_size = end_pos - start_pos; // 读完上述字段的实际大小
        if actual_size != record_length as u64 {
            return Err(Error::AssertFail {
                pos: end_pos,
                message: format!(
                    "Mismatch record_length: declared {}, but actual read is {} bytes",
                    record_length, actual_size
                ),
            });
        }

        // 4. 返回组装后的 Record
        Ok(Record {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}

/// 自定义写出逻辑：先将 Record 内容写到内存缓冲，再写出 varint 大小，最后写出缓冲
impl BinWrite for Record {
    type Args<'a> = ();

    fn write_options<'a, W: Write + Seek>(
        &self,
        writer: &mut W,
        endian: Endian,
        _args: Self::Args<'a>,
    ) -> BinResult<()> {
        // 1. 将本条Record写到一个临时缓冲中
        let mut record_body = Vec::new();
        let mut record_cursor = Cursor::new(&mut record_body);

        // 写其它字段 (大端序)
        self.attributes
            .write_options(&mut record_cursor, endian, ())?;
        record_cursor.write_varint(self.timestamp_delta)?;
        record_cursor.write_varint(self.offset_delta)?;

        // 写 key
        write_vec_u8_with_signed_varint_length(&self.key, &mut record_cursor, endian, ())?;

        // 写 value
        let mut value_vec: Vec<u8> = Vec::new();
        let mut value_cursor = Cursor::new(&mut value_vec);
        self.value.write_options(&mut value_cursor, endian, ())?;
        write_vec_u8_with_signed_varint_length(&value_vec, &mut record_cursor, endian, ())?;

        // 写 headers
        record_cursor.write_varint(self.headers.len())?;
        for h in &self.headers {
            h.write_options(&mut record_cursor, endian, ())?;
        }

        // 2. 计算总大小并写到输出流（varint）
        let actual_size = record_body.len() as i64;
        writer.write_varint(actual_size)?;

        // 3. 最后将缓存好的record_body写到输出流
        writer.write_all(&record_body)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binrw::{BinRead, BinWrite};
    use std::io::{BufReader, Cursor};

    #[test]
    fn test_record_batch_roundtrip() {
        let original_batch = RecordBatch {
            base_offset: 1000,
            partition_leader_epoch: 0,
            crc: 2216891472,
            attributes: 0b0101_0010,
            base_timestamp: 1690000000,
            max_timestamp: 1690000050,
            producer_id: 42,
            producer_epoch: 1,
            base_sequence: 5,
            records: vec![
                Record {
                    attributes: 0,
                    timestamp_delta: 3,
                    offset_delta: 0,
                    key: b"key".to_vec(),
                    value: ClusterMetadataRecord::mock_broker_registration(),
                    headers: vec![RecordHeader {
                        key: b"k1".to_vec(),
                        value: b"v1".to_vec(),
                    }],
                },
                Record {
                    attributes: 0,
                    timestamp_delta: 4,
                    offset_delta: 1,
                    key: vec![],
                    value: ClusterMetadataRecord::mock_feature_level_record(),
                    headers: vec![
                        RecordHeader {
                            key: b"foo".to_vec(),
                            value: b"bar".to_vec(),
                        },
                        RecordHeader {
                            key: b"baz".to_vec(),
                            value: b"qux".to_vec(),
                        },
                    ],
                },
            ],
        };

        // 序列化: 将 original_batch 写入 buffer
        let mut buffer = vec![];
        original_batch
            .write_options(&mut Cursor::new(&mut buffer), Endian::Big, ())
            .unwrap();

        // 反序列化: 读取 buffer 还原为 RecordBatch
        let decoded_batch: RecordBatch = BinRead::read(&mut Cursor::new(&buffer)).unwrap();
        assert_eq!(original_batch, decoded_batch);

        // 确认 records
        assert_eq!(decoded_batch.records.len(), 2);

        // 对比原始对象中的值，而不是硬编码的字节串
        assert_eq!(
            decoded_batch.records[0].value,
            original_batch.records[0].value
        );
        assert_eq!(
            decoded_batch.records[1].value,
            original_batch.records[1].value
        );

        // Header 校验
        let first_headers = &decoded_batch.records[0].headers;
        assert_eq!(first_headers.len(), 1);
        assert_eq!(first_headers[0].key, b"k1");
        assert_eq!(first_headers[0].value, b"v1");

        let second_headers = &decoded_batch.records[1].headers;
        assert_eq!(second_headers.len(), 2);
        assert_eq!(second_headers[0].key, b"foo");
        assert_eq!(second_headers[0].value, b"bar");
        assert_eq!(second_headers[1].key, b"baz");
        assert_eq!(second_headers[1].value, b"qux");
    }

    #[test]
    fn test_record_roundtrip() {
        // 需要更新这个测试，因为 value 现在是 ClusterMetadataRecord 类型
        let record = Record {
            attributes: 1,
            timestamp_delta: 12345,
            offset_delta: 2,
            key: b"sample_key".to_vec(),
            value: ClusterMetadataRecord::mock_partition_record(), // 使用一个适当的mock值
            headers: vec![RecordHeader {
                key: b"h1".to_vec(),
                value: b"header1".to_vec(),
            }],
        };

        // 序列化
        let mut buffer = vec![];
        record
            .write_options(&mut Cursor::new(&mut buffer), Endian::Big, ())
            .unwrap();

        // 反序列化
        let decoded: Record =
            Record::read_options(&mut Cursor::new(&buffer), Endian::Big, ()).unwrap();

        // record_length 会自动更新并检查，反序列化时必须和实际字段大小匹配
        // 验证其它字段
        assert_eq!(decoded.attributes, 1);
        assert_eq!(decoded.timestamp_delta, 12345);
        assert_eq!(decoded.offset_delta, 2);
        assert_eq!(decoded.key, b"sample_key");
        assert_eq!(decoded.value, record.value); // 比较实际对象
        assert_eq!(decoded.headers.len(), 1);
        assert_eq!(decoded.headers[0].key, b"h1");
        assert_eq!(decoded.headers[0].value, b"header1");
    }

    #[test]
    fn test_real_feature_level_record_data() {
        let real_data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00,
            0x00, 0x01, 0x02, 0xb0, 0x69, 0x45, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8,
            0x18, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00, 0x01, 0x3a, 0x00, 0x00, 0x00, 0x01, 0x2e, 0x01, 0x0c, 0x00,
            0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73,
            0x69, 0x6f, 0x6e, 0x00, 0x14, 0x00, 0x00,
        ];

        let mut reader = BufReader::new(Cursor::new(real_data));
        let result = RecordBatch::read(&mut reader);
        assert!(result.is_ok(), "{:?}", result);
    }
}
