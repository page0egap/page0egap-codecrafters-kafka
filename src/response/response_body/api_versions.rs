use crate::response::error_code::ErrorCode;

pub struct KafkaResponseBodyApiVersions {
    error_code: ErrorCode,
}

impl KafkaResponseBodyApiVersions {
    pub fn new(api_version: i16) -> Self {
        let error_code = if api_version >= 0 && api_version <= 4 {
            ErrorCode::None
        } else {
            ErrorCode::UnsupportedVersion
        };
        Self { error_code }
    }
}

impl Into<Vec<u8>> for KafkaResponseBodyApiVersions {
    #[inline]
    fn into(self) -> Vec<u8> {
        let error_code: i16 = self.error_code.into();
        error_code.to_be_bytes().to_vec()
    }
}
