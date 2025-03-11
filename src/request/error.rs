use std::borrow::Cow;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("Unsupported Version {api_version} in Session {correlation_id}")]
    UnsupportedVersion {
        api_version: i16,
        correlation_id: i32,
    },
    #[error("Unsupported Api Key {api_key} in Session {correlation_id}")]
    UnsupportedApiKey { api_key: u16, correlation_id: i32 },
    #[error("Invalid Format of field {field} in Session {correlation_id}")]
    InvalidFormat {
        field: Cow<'static, str>,
        correlation_id: i32,
    },
}
