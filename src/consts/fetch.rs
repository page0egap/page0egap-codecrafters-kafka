use num_enum::{IntoPrimitive, TryFromPrimitive};

pub const FETCH_API_KEY: i16 = 1;
pub const FETCH_MIN_VERSION: i16 = 0;
pub const FETCH_MAX_VERSION: i16 = 16;

#[repr(i16)]
#[derive(Debug, TryFromPrimitive, IntoPrimitive)]
pub enum SupportFetchRequestVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
    V5 = 5,
    V6 = 6,
    V7 = 7,
    V8 = 8,
    V9 = 9,
    V10 = 10,
    V11 = 11,
    V12 = 12,
    V13 = 13,
    V14 = 14,
    V15 = 15,
    V16 = 16,
}