use bincode::{BorrowDecode, Decode, Encode};
use std::cmp::Ordering;

use super::FileChunk;

impl PartialOrd for FileChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.as_bytes().cmp(other.hash.as_bytes())
    }
}

impl Encode for FileChunk {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.hash.as_bytes().encode(encoder)
    }
}

impl<'de> BorrowDecode<'de> for FileChunk {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(FileChunk {
            hash: blake3::Hash::from_bytes(<_>::decode(decoder)?),
        })
    }
}

impl Decode for FileChunk {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(FileChunk {
            hash: blake3::Hash::from_bytes(<_>::decode(decoder)?),
        })
    }
}
