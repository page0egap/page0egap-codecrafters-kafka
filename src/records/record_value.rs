use crate::common_structs::tagged_field::TaggedField;
use binrw::binrw;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use super::utils::{
    parse_compact_array, parse_compact_string, parse_tagged_fields, write_compact_array,
    write_compact_string, write_tagged_fields,
};

/// 表示不同类型的记录
#[derive(Debug, PartialEq, Copy, Clone, TryFromPrimitive, IntoPrimitive)]
#[repr(i8)]
pub enum RecordType {
    BrokerRegistration = 0,
    Topic = 2,
    FeatureLevel = 12,
    Partition = 3,
    // 未来可以方便地添加更多类型...
}

#[binrw]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub struct ClusterMetadataRecord {
    /// 所有记录都共享的字段
    pub frame_version: i8,

    /// 临时字段，不保存在结构体；读时从文件获取，写时动态计算
    #[br(temp)]
    #[bw(calc = compute_record_type(&payload).into())] // 使用 IntoPrimitive 自动转换
    record_type: i8,

    /// 同样共享的字段
    pub record_version: i8,

    /// 剩余有效载荷，依赖 record_type 的值解析
    #[br(args { record_type })]
    pub payload: ClusterMetadataValue,
}

impl ClusterMetadataRecord {
    /// 创建一个 BrokerRegistration 类型的 ClusterMetadataRecord
    pub fn mock_broker_registration() -> Self {
        ClusterMetadataRecord {
            frame_version: 10,
            record_version: 7,
            payload: ClusterMetadataValue::BrokerRegistration(BrokerRegistrationRecord {
                broker_id: 123,
                cluster_id: 456789,
            }),
        }
    }

    /// 创建一个 TopicRecord 类型的 ClusterMetadataRecord
    pub fn mock_topic_record() -> Self {
        ClusterMetadataRecord {
            frame_version: 99,
            record_version: 3,
            payload: ClusterMetadataValue::Topic(TopicRecord {
                topic_name: "example_topic".to_string(),
                uuid: [1u8; 16],
                tagged_fields: Vec::new(),
            }),
        }
    }

    /// 创建一个 FeatureLevelRecord 类型的 ClusterMetadataRecord
    pub fn mock_feature_level_record() -> Self {
        ClusterMetadataRecord {
            frame_version: 100,
            record_version: 1,
            payload: ClusterMetadataValue::FeatureLevel(FeatureLevelRecord {
                feature_name: "example_feature".to_string(),
                level: 5,
                tagged_fields: Vec::new(),
            }),
        }
    }

    /// 创建一个 PartitionRecord 类型的 ClusterMetadataRecord
    pub fn mock_partition_record() -> Self {
        ClusterMetadataRecord {
            frame_version: 5,
            record_version: 2,
            payload: ClusterMetadataValue::Partition(PartitionRecord {
                partition_id: 42,
                topic_id: [10; 16],
                leader_id: 1001,
                leader_epoch: 5,
                replicas: vec![1001, 1002, 1003],
                isr: vec![1001, 1002],
                rra: vec![1001, 1002, 1004],
                ara: vec![1001, 1002, 1005],
                partition_epoch: 2,
                tagged_fields: Vec::new(),
                directories: vec![[15; 16]],
            }),
        }
    }
}

/// 不同记录的枚举，根据 record_type 的值确定走哪个分支
#[binrw]
#[br(import { record_type: i8 })]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub enum ClusterMetadataValue {
    #[br(pre_assert(record_type == RecordType::BrokerRegistration.into()))]
    BrokerRegistration(BrokerRegistrationRecord),

    #[br(pre_assert(record_type == RecordType::Topic.into()))]
    Topic(TopicRecord),

    #[br(pre_assert(record_type == RecordType::FeatureLevel.into()))]
    FeatureLevel(FeatureLevelRecord),

    #[br(pre_assert(record_type == RecordType::Partition.into()))]
    Partition(PartitionRecord),
}

/// 将 match 逻辑单独提取到函数
fn compute_record_type(payload: &ClusterMetadataValue) -> RecordType {
    match payload {
        ClusterMetadataValue::BrokerRegistration(_) => RecordType::BrokerRegistration,
        ClusterMetadataValue::Topic(_) => RecordType::Topic,
        ClusterMetadataValue::FeatureLevel(_) => RecordType::FeatureLevel,
        ClusterMetadataValue::Partition(_) => RecordType::Partition,
    }
}

#[binrw]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub struct BrokerRegistrationRecord {
    pub broker_id: i32,
    pub cluster_id: i64,
    // 可扩展更多字段
}

#[binrw]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub struct TopicRecord {
    #[br(parse_with=parse_compact_string)]
    #[bw(write_with=write_compact_string::<String, _>)]
    pub topic_name: String,
    pub uuid: [u8; 16],
    #[br(parse_with=parse_tagged_fields)]
    #[bw(write_with=write_tagged_fields)]
    pub tagged_fields: Vec<TaggedField>,
}

#[binrw]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub struct FeatureLevelRecord {
    #[br(parse_with=parse_compact_string)]
    #[bw(write_with=write_compact_string::<String, _>)]
    pub feature_name: String,
    pub level: i16,
    #[br(parse_with=parse_tagged_fields)]
    #[bw(write_with=write_tagged_fields)]
    pub tagged_fields: Vec<TaggedField>,
}

/// Kafka 分区记录结构
#[binrw]
#[derive(Debug, PartialEq, Clone)]
#[brw(big)]
pub struct PartitionRecord {
    /// 分区ID
    pub partition_id: i32,

    /// 主题ID
    pub topic_id: [u8; 16],

    /// 副本列表（broker ID 数组）
    #[br(parse_with=parse_compact_array::<_, _, ()>)]
    #[bw(write_with=write_compact_array)]
    pub replicas: Vec<i32>,

    /// ISR列表（同步的副本 broker ID 数组）
    #[br(parse_with=parse_compact_array::<_, _, ()>)]
    #[bw(write_with=write_compact_array)]
    pub isr: Vec<i32>,

    #[br(parse_with=parse_compact_array::<_, _, ()>)]
    #[bw(write_with=write_compact_array)]
    pub rra: Vec<i32>,

    #[br(parse_with=parse_compact_array::<_, _, ()>)]
    #[bw(write_with=write_compact_array)]
    pub ara: Vec<i32>,

    /// 分区所在的领导者（broker）ID
    pub leader_id: i32,

    /// 分区的选举（leader epoch）
    pub leader_epoch: i32,

    pub partition_epoch: i32,

    #[br(parse_with=parse_compact_array::<_, _, ()>)]
    #[bw(write_with=write_compact_array)]
    pub directories: Vec<[u8; 16]>,

    /// 附加标记字段
    #[br(parse_with=parse_tagged_fields)]
    #[bw(write_with=write_tagged_fields)]
    pub tagged_fields: Vec<TaggedField>,
}

#[cfg(test)]
mod tests {
    use crate::records::record_value;

    use super::*;
    use binrw::{BinRead, BinWrite};
    use std::io::Cursor;

    #[test]
    fn test_broker_registration() {
        let original = ClusterMetadataRecord {
            frame_version: 10,
            record_version: 7,
            payload: ClusterMetadataValue::BrokerRegistration(BrokerRegistrationRecord {
                broker_id: 123,
                cluster_id: 456789,
            }),
        };

        let mut data = vec![];
        let mut cursor = Cursor::new(&mut data);
        original.write(&mut cursor).unwrap();
        let decoded = ClusterMetadataRecord::read(&mut Cursor::new(&data)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_topic_record() {
        let original = ClusterMetadataRecord {
            frame_version: 99,
            record_version: 3,
            payload: ClusterMetadataValue::Topic(TopicRecord {
                topic_name: "123".to_string(),
                uuid: [1u8; 16],
                tagged_fields: Vec::new(),
            }),
        };

        let mut data = vec![];
        let mut cursor = Cursor::new(&mut data);
        original.write(&mut cursor).unwrap();
        let decoded = ClusterMetadataRecord::read(&mut Cursor::new(&data)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_feature_level_record() {
        let original = ClusterMetadataRecord {
            frame_version: 100,
            record_version: 1,
            payload: ClusterMetadataValue::FeatureLevel(FeatureLevelRecord {
                feature_name: "999".to_string(),
                level: 5,
                tagged_fields: Vec::new(),
            }),
        };

        let mut data = vec![];
        let mut cursor = Cursor::new(&mut data);
        original.write(&mut cursor).unwrap();
        let decoded = ClusterMetadataRecord::read(&mut Cursor::new(&data)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_partition_record() {
        let original = ClusterMetadataRecord {
            frame_version: 5,
            record_version: 2,
            payload: ClusterMetadataValue::Partition(PartitionRecord {
                partition_id: 42,
                topic_id: [10; 16], // 示例UUID
                leader_id: 1001,
                leader_epoch: 5,
                replicas: vec![1001, 1002, 1003],
                isr: vec![1001, 1002],
                rra: vec![1001, 1002, 1004],
                ara: vec![1001, 1002, 1005],
                partition_epoch: 2,
                tagged_fields: Vec::new(),
                directories: vec![[15; 16]],
            }),
        };

        let mut data = vec![];
        let mut cursor = Cursor::new(&mut data);
        original.write(&mut cursor).unwrap();
        let decoded = ClusterMetadataRecord::read(&mut Cursor::new(&data)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_real_data() {
        let real_raw = [72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33];
        let cluster_metadata_record = ClusterMetadataValue::read_be_args(
            &mut Cursor::new(&real_raw),
            record_value::ClusterMetadataValueBinReadArgs { record_type: 3 },
        )
        .unwrap();
        // test时候打印一下
        println!("{:?}", cluster_metadata_record);
    }
}
