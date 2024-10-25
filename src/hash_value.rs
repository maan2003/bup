use bincode::{impl_borrow_decode, Decode, Encode};
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HashValue(pub blake3::Hash);

impl PartialOrd for HashValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HashValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl Encode for HashValue {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.0.as_bytes().encode(encoder)
    }
}

impl Decode for HashValue {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(HashValue {
            0: blake3::Hash::from_bytes(<_>::decode(decoder)?),
        })
    }
}

impl_borrow_decode!(HashValue);
