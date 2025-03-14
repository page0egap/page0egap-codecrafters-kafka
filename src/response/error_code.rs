use num_enum::{IntoPrimitive, TryFromPrimitive};
use thiserror::Error;

#[repr(i16)]
#[derive(Error, Debug, TryFromPrimitive, IntoPrimitive)]
pub enum KafkaError {
    #[error("UnknownServerError")]
    UnknownServerError = -1,
    #[error("None")]
    None = 0,
    #[error("UnknownTopicOrPartition")]
    UnknownTopicOrPartition = 3,
    #[error("UnsupportedVersion")]
    UnsupportedVersion = 35,
    #[error("InvalidRequest")]
    InvalidRequest = 42,
}
