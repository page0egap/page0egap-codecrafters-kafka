use crate::consts::api_versions::SupportApiVersionsRequestVersion;
use crate::request::{self, error::RequestError, KafkaRequestHeader};
use crate::traits::KafkaDeseriarize;
use std::io::Read;

#[derive(Debug)]
pub enum ApiVersionsRequestBody {
    V0,
    V1,
    V2,
    V3(ApiVersionsRequestBodyV3),
    V4(ApiVersionsRequestBodyV4),
}

impl ApiVersionsRequestBody {
    pub fn get_api_version(&self) -> SupportApiVersionsRequestVersion {
        match self {
            ApiVersionsRequestBody::V0 => SupportApiVersionsRequestVersion::V0,
            ApiVersionsRequestBody::V1 => SupportApiVersionsRequestVersion::V1,
            ApiVersionsRequestBody::V2 => SupportApiVersionsRequestVersion::V2,
            ApiVersionsRequestBody::V3(_) => SupportApiVersionsRequestVersion::V3,
            ApiVersionsRequestBody::V4(_) => SupportApiVersionsRequestVersion::V4,
        }
    }
}

impl KafkaDeseriarize for ApiVersionsRequestBody {
    type Error = RequestError;

    type DependentData<'a> = &'a KafkaRequestHeader;

    fn try_parse_from_reader<R: Read>(
        reader: &mut R,
        header: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let api_version = header.request_api_version();
        let correlation_id = header.correlation_id();
        let version: SupportApiVersionsRequestVersion =
            api_version
                .try_into()
                .map_err(|_| RequestError::UnsupportedVersion {
                    api_version,
                    correlation_id,
                })?;
        let body = match version {
            SupportApiVersionsRequestVersion::V0 => ApiVersionsRequestBody::V0,
            SupportApiVersionsRequestVersion::V1 => ApiVersionsRequestBody::V1,
            SupportApiVersionsRequestVersion::V2 => ApiVersionsRequestBody::V2,
            SupportApiVersionsRequestVersion::V3 => ApiVersionsRequestBody::V3(
                ApiVersionsRequestBodyV3::try_parse_from_reader(reader, &header)?,
            ),
            SupportApiVersionsRequestVersion::V4 => ApiVersionsRequestBody::V4(
                ApiVersionsRequestBodyV4::try_parse_from_reader(reader, &header)?,
            ),
        };
        Ok(body)
    }
}

#[derive(Debug)]
pub struct ApiVersionsRequestBodyV3 {
    #[allow(unused)]
    client_software_name: String,
    #[allow(unused)]
    client_software_version: String,
}

#[derive(Debug)]
pub struct ApiVersionsRequestBodyV4 {
    #[allow(unused)]
    client_software_name: String,
    #[allow(unused)]
    client_software_version: String,
}

impl KafkaDeseriarize for ApiVersionsRequestBodyV3 {
    type Error = RequestError;
    type DependentData<'a> = &'a KafkaRequestHeader;
    fn try_parse_from_reader<R: Read>(
        reader: &mut R,
        data: &KafkaRequestHeader,
    ) -> Result<Self, Self::Error> {
        let id = data.correlation_id();
        let client_software_name = request::utils::try_read_compact_string(reader)
            .map_err(|_| RequestError::invalid_format("client_software_name", id))?;
        let client_software_version = request::utils::try_read_compact_string(reader)
            .map_err(|_| RequestError::invalid_format("client_software_version", id))?;
        let _ = request::utils::try_read_tagged_fields(reader)
            .map_err(|_| RequestError::invalid_format("_tagged_fields", id))?;
        Ok(ApiVersionsRequestBodyV3 {
            client_software_name,
            client_software_version,
        })
    }
}

impl KafkaDeseriarize for ApiVersionsRequestBodyV4 {
    type Error = RequestError;
    type DependentData<'a> = &'a KafkaRequestHeader;
    fn try_parse_from_reader<R: Read>(
        reader: &mut R,
        data: &KafkaRequestHeader,
    ) -> Result<Self, Self::Error> {
        let id = data.correlation_id();
        let client_software_name = request::utils::try_read_compact_string(reader)
            .map_err(|_| RequestError::invalid_format("client_software_name", id))?;
        let client_software_version = request::utils::try_read_compact_string(reader)
            .map_err(|_| RequestError::invalid_format("client_software_version", id))?;
        let _ = request::utils::try_read_tagged_fields(reader)
            .map_err(|_| RequestError::invalid_format("_tagged_fields", id))?;
        Ok(ApiVersionsRequestBodyV4 {
            client_software_name,
            client_software_version,
        })
    }
}
