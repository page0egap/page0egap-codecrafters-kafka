use binrw::{BinRead, BinWrite};
use integer_encoding::VarIntReader;
use integer_encoding::VarIntWriter;

#[derive(Debug, PartialEq, Clone)]
pub struct RecordHeader {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl BinRead for RecordHeader {
    type Args<'a> = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _endian: binrw::Endian,
        _args: Self::Args<'_>,
    ) -> binrw::BinResult<Self> {
        let key_length = reader.read_varint().map_err(binrw::Error::Io)?;
        let mut key = vec![0u8; key_length];
        reader.read_exact(&mut key).map_err(binrw::Error::Io)?;

        let value_length = reader.read_varint().map_err(binrw::Error::Io)?;
        let mut value = vec![0u8; value_length];
        reader.read_exact(&mut value).map_err(binrw::Error::Io)?;

        Ok(Self { key, value })
    }
}

impl BinWrite for RecordHeader {
    type Args<'a> = ();

    fn write_options<W: std::io::Write + std::io::Seek>(
        &self,
        writer: &mut W,
        _endian: binrw::Endian,
        _args: Self::Args<'_>,
    ) -> binrw::BinResult<()> {
        writer
            .write_varint(self.key.len())
            .map_err(binrw::Error::Io)?;
        writer.write(&self.key).map_err(binrw::Error::Io)?;
        writer
            .write_varint(self.value.len())
            .map_err(binrw::Error::Io)?;
        writer.write(&self.value).map_err(binrw::Error::Io)?;
        Ok(())
    }
}
