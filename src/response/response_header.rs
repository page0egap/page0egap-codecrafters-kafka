pub enum KafkaResponseHeader {
    VO(KafkaResponseHeaderV0),
}

pub struct KafkaResponseHeaderV0 {
    correlation_id: i32,
}

impl KafkaResponseHeaderV0 {
    fn new(correlation_id: i32) -> Self {
        KafkaResponseHeaderV0 { correlation_id }
    }
}

impl Into<Vec<u8>> for KafkaResponseHeaderV0 {
    fn into(self) -> Vec<u8> {
        self.correlation_id.to_be_bytes().to_vec()
    }
}

impl KafkaResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        Self::VO(KafkaResponseHeaderV0::new(correlation_id))
    }

    pub fn to_vec(self) -> Vec<u8> {
        match self {
            KafkaResponseHeader::VO(kafka_response_header_v0) => 
            kafka_response_header_v0.into(),
        }
    }
}
