use std::marker::PhantomData;

pub enum KafkaResponseHeader {
    V0(KafkaResponseHeaderV0),
}

pub struct KafkaResponseHeaderV0 {
    correlation_id: i32,
}

impl KafkaResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        KafkaResponseHeader::V0(KafkaResponseHeaderV0 { correlation_id })
    }

    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            KafkaResponseHeader::V0(kafka_header_v0) => {
                kafka_header_v0.correlation_id.to_be_bytes().to_vec()
            }
        }
    }
}


pub struct KafkaResponse {
    message_size: i32,
    header: KafkaResponseHeader,
    body: PhantomData<()>,
}

impl KafkaResponse {
    pub fn empty(header: KafkaResponseHeader) -> Self {
        KafkaResponse {
            message_size: 0,
            header,
            body: PhantomData,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.message_size
            .to_be_bytes()
            .into_iter()
            .chain(self.header.to_bytes())
            .collect()
    }
}
