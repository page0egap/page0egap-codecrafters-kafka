use num_enum::TryFromPrimitive;

#[repr(u16)]
#[derive(Debug, TryFromPrimitive)]
pub enum RequestApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = 18,
}
