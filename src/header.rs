pub enum KafkaHeader {
    V0(KafkaHeaderV0),
}

pub struct KafkaHeaderV0 {
    correlation_id: i32,
}

impl KafkaHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        KafkaHeader::V0(KafkaHeaderV0 { correlation_id })
    }

    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            KafkaHeader::V0(kafka_header_v0) => {
                kafka_header_v0.correlation_id.to_be_bytes().to_vec()
            }
        }
    }
}
