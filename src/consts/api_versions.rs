use num_enum::TryFromPrimitive;

pub const API_VERSIONS_API_KEY: i16 = 18;
pub const API_VERSIONS_MIN_VERSION: i16 = 0;
pub const API_VERSIONS_MAX_VERSION: i16 = 4;

#[repr(i16)]
#[derive(Debug, TryFromPrimitive)]
pub enum SupportApiVersionsRequestVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
}
