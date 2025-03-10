use std::marker::PhantomData;

use crate::header::KafkaHeader;

pub struct KafkaResponse {
    message_size: i32,
    header: KafkaHeader,
    body: PhantomData<()>,
}

impl KafkaResponse {
    pub fn empty(header: KafkaHeader) -> Self {
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
