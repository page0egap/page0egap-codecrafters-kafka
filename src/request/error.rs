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
    UnsupportedApiKey { api_key: i16, correlation_id: i32 },
    #[error("Invalid Format of field {field} in Session {correlation_id}")]
    InvalidFormat {
        field: ErrorField,
        correlation_id: i32,
    },
    #[error("Invalid Format of field {0} in Session without knowing correlation_id")]
    InvalidFormatWithoutCId(#[from] ErrorField),
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ErrorField(Cow<'static, str>);

impl From<Cow<'static, str>> for ErrorField {
    fn from(value: Cow<'static, str>) -> Self {
        Self(value)
    }
}
