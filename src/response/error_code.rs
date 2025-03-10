use num_enum::{IntoPrimitive, TryFromPrimitive};


#[repr(i16)]
#[derive(Debug, TryFromPrimitive, IntoPrimitive)]
pub enum ErrorCode {
    UnknownServerError = -1,
    None = 0,
    UnsupportedVersion = 35,
}