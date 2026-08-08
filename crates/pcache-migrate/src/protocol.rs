use std::collections::HashMap;
use std::io::{self, Read};

use sha2::{Digest, Sha256};
use thiserror::Error;

pub const LRU_CACHE_SIZE: usize = 1_048_576;
const MAGIC: &[u8; 8] = b"HATHPC01";
const VERSION: u16 = 1;
const MAX_STATIC_RANGES: u32 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCacheState {
    pub cache_count: u32,
    pub cache_size: u64,
    pub lru_clear_pointer: usize,
    pub static_range_ages: HashMap<String, u64>,
    pub lru_cache_table: Vec<u16>,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("failed to read HPCACHE/1 stream: {0}")]
    Io(#[from] io::Error),
    #[error("invalid HPCACHE/1 magic")]
    Magic,
    #[error("unsupported HPCACHE/1 version {0}")]
    Version(u16),
    #[error("invalid {field}: {value}")]
    InvalidValue { field: &'static str, value: u64 },
    #[error("invalid static range {0:?}")]
    InvalidRange(String),
    #[error("duplicate static range {0}")]
    DuplicateRange(String),
    #[error("HPCACHE/1 SHA-256 digest mismatch")]
    DigestMismatch,
    #[error("HPCACHE/1 stream has trailing bytes")]
    TrailingData,
}

pub fn read<R: Read>(input: R) -> Result<LegacyCacheState, ProtocolError> {
    let mut reader = DigestReader::new(input);

    let mut magic = [0u8; MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic != *MAGIC {
        return Err(ProtocolError::Magic);
    }

    let version = reader.read_u16()?;
    if version != VERSION {
        return Err(ProtocolError::Version(version));
    }

    let cache_count = reader.read_u32()?;
    let cache_size = reader.read_u64()?;
    let lru_clear_pointer = reader.read_u32()?;
    if lru_clear_pointer as usize >= LRU_CACHE_SIZE {
        return Err(ProtocolError::InvalidValue {
            field: "lru_clear_pointer",
            value: lru_clear_pointer as u64,
        });
    }

    let range_count = reader.read_u32()?;
    if range_count > MAX_STATIC_RANGES {
        return Err(ProtocolError::InvalidValue {
            field: "static_range_count",
            value: range_count as u64,
        });
    }
    let mut static_range_ages = HashMap::with_capacity(range_count as usize);
    for _ in 0..range_count {
        let range_length = reader.read_u16()?;
        if range_length != 4 {
            return Err(ProtocolError::InvalidValue {
                field: "static range length",
                value: range_length as u64,
            });
        }
        let mut range_bytes = [0u8; 4];
        reader.read_exact(&mut range_bytes)?;
        if !range_bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(ProtocolError::InvalidRange(
                String::from_utf8_lossy(&range_bytes).into_owned(),
            ));
        }
        let range = String::from_utf8(range_bytes.to_vec()).expect("ASCII range validated");
        let oldest_modified_ms = reader.read_u64()?;
        if static_range_ages
            .insert(range.clone(), oldest_modified_ms)
            .is_some()
        {
            return Err(ProtocolError::DuplicateRange(range));
        }
    }

    let lru_length = reader.read_u32()?;
    if lru_length as usize != LRU_CACHE_SIZE {
        return Err(ProtocolError::InvalidValue {
            field: "lru_length",
            value: lru_length as u64,
        });
    }
    let mut lru_cache_table = Vec::with_capacity(LRU_CACHE_SIZE);
    for _ in 0..LRU_CACHE_SIZE {
        lru_cache_table.push(reader.read_u16()?);
    }

    let expected_digest = reader.finalize();
    let mut input = reader.into_inner();
    let mut actual_digest = [0u8; 32];
    input.read_exact(&mut actual_digest)?;
    if actual_digest != expected_digest.as_slice() {
        return Err(ProtocolError::DigestMismatch);
    }
    let mut trailing = [0u8; 1];
    if input.read(&mut trailing)? != 0 {
        return Err(ProtocolError::TrailingData);
    }

    Ok(LegacyCacheState {
        cache_count,
        cache_size,
        lru_clear_pointer: lru_clear_pointer as usize,
        static_range_ages,
        lru_cache_table,
    })
}

#[doc(hidden)]
pub fn encode_hpcache_v1_for_test(state: &LegacyCacheState) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(48 + state.lru_cache_table.len() * 2 + 32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&state.cache_count.to_le_bytes());
    bytes.extend_from_slice(&state.cache_size.to_le_bytes());
    bytes.extend_from_slice(&(state.lru_clear_pointer as u32).to_le_bytes());
    bytes.extend_from_slice(&(state.static_range_ages.len() as u32).to_le_bytes());
    for (range, oldest_modified_ms) in &state.static_range_ages {
        bytes.extend_from_slice(&(range.len() as u16).to_le_bytes());
        bytes.extend_from_slice(range.as_bytes());
        bytes.extend_from_slice(&oldest_modified_ms.to_le_bytes());
    }
    bytes.extend_from_slice(&(state.lru_cache_table.len() as u32).to_le_bytes());
    for value in &state.lru_cache_table {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    bytes
}

struct DigestReader<R> {
    input: R,
    digest: Sha256,
}

impl<R: Read> DigestReader<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            digest: Sha256::new(),
        }
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        self.input.read_exact(bytes)?;
        self.digest.update(bytes);
        Ok(())
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        let mut bytes = [0u8; 2];
        self.read_exact(&mut bytes)?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let mut bytes = [0u8; 4];
        self.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut bytes = [0u8; 8];
        self.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn finalize(&mut self) -> sha2::digest::Output<Sha256> {
        self.digest.clone().finalize()
    }

    fn into_inner(self) -> R {
        self.input
    }
}

#[cfg(test)]
mod tests {
    use super::{LRU_CACHE_SIZE, LegacyCacheState, encode_hpcache_v1_for_test, read};

    fn valid_hpcache_v1() -> Vec<u8> {
        let mut lru_cache_table = vec![0; LRU_CACHE_SIZE];
        lru_cache_table[123] = 0x8000;
        lru_cache_table[456] = 0xffff;
        encode_hpcache_v1_for_test(&LegacyCacheState {
            cache_count: 7,
            cache_size: 99,
            lru_clear_pointer: 17,
            static_range_ages: std::collections::HashMap::from([(
                "a3f0".to_string(),
                1_700_000_000_000,
            )]),
            lru_cache_table,
        })
    }

    #[test]
    fn valid_stream_is_accepted() {
        assert!(read(valid_hpcache_v1().as_slice()).is_ok());
    }
}
