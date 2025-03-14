use std::io::Read;

pub trait TryParseFromReader {
    type Error: std::error::Error;
    fn try_parse_from_reader<R: Read>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
