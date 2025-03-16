use std::io::{Read, Write};

pub trait KafkaDeseriarize {
    type Error: std::error::Error;
    type DependentData<'a>;
    fn try_parse_from_reader<R: Read>(
        reader: &mut R,
        data: Self::DependentData<'_>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

pub trait KafkaSeriarize {
    type Error: std::error::Error;
    type DependentData<'a>;
    fn serialize<W: Write>(
        self,
        writer: &mut W,
        data: Self::DependentData<'_>,
    ) -> Result<(), Self::Error>;
}
