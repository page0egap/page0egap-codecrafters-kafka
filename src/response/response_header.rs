pub enum KafkaResponseHeader {
    VO(KafkaResponseHeaderV0),
}

pub struct KafkaResponseHeaderV0 {
    correlation_id: i32,
}

impl KafkaResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        Self::VO(KafkaResponseHeaderV0::new(correlation_id))
    }
}

impl Into<Vec<u8>> for KafkaResponseHeader {
    #[inline]
    fn into(self) -> Vec<u8> {
        match self {
            KafkaResponseHeader::VO(inner) => inner.into(),
        }
    }
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
