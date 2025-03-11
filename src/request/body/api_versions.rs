use crate::consts::api_versions::SupportApiVersionsRequestVersion;
use crate::request::{self, error::RequestError, KafkaRequestHeader};
use std::{borrow::Cow, io::Read};

type Field = Cow<'static, str>;

pub enum ApiVersionsRequestBody {
    V0,
    V1,
    V2,
    V3(ApiVersionsRequestBodyV3),
    V4(ApiVersionsRequestBodyV4),
}

impl ApiVersionsRequestBody {
    pub fn try_from_reader<R: Read>(
        reader: &mut R,
        header: &KafkaRequestHeader,
    ) -> Result<Self, RequestError> {
        let api_version = header.request_api_version;
        let correlation_id = header.correlation_id;
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
                ApiVersionsRequestBodyV3::try_from_reader(reader).map_err(|field| {
                    RequestError::InvalidFormat {
                        field,
                        correlation_id,
                    }
                })?,
            ),
            SupportApiVersionsRequestVersion::V4 => ApiVersionsRequestBody::V4(
                ApiVersionsRequestBodyV4::try_from_reader(reader).map_err(|field| {
                    RequestError::InvalidFormat {
                        field,
                        correlation_id,
                    }
                })?,
            ),
        };
        Ok(body)
    }
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

pub struct ApiVersionsRequestBodyV3 {
    #[allow(unused)]
    client_software_name: String,
    #[allow(unused)]
    client_software_version: String,
}

pub struct ApiVersionsRequestBodyV4 {
    #[allow(unused)]
    client_software_name: String,
    #[allow(unused)]
    client_software_version: String,
}

impl ApiVersionsRequestBodyV3 {
    fn try_from_reader<R: Read>(reader: &mut R) -> Result<Self, Field> {
        let client_software_name = request::utils::read_compact_string(reader)
            .map_err(|_| Cow::from("client_software_name"))?;
        let client_software_version = request::utils::read_compact_string(reader)
            .map_err(|_| Cow::from("client_software_version"))?;
        let _ =
            request::utils::read_tagged_fields(reader).map_err(|_| Cow::from("_tagged_fields"))?;
        Ok(ApiVersionsRequestBodyV3 {
            client_software_name,
            client_software_version,
        })
    }
}

impl ApiVersionsRequestBodyV4 {
    fn try_from_reader<R: Read>(reader: &mut R) -> Result<Self, Field> {
        let client_software_name = request::utils::read_compact_string(reader)
            .map_err(|_| Cow::from("client_software_name"))?;
        let client_software_version = request::utils::read_compact_string(reader)
            .map_err(|_| Cow::from("client_software_version"))?;
        let _ =
            request::utils::read_tagged_fields(reader).map_err(|_| Cow::from("_tagged_fields"))?;
        Ok(ApiVersionsRequestBodyV4 {
            client_software_name,
            client_software_version,
        })
    }
}
