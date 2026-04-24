use std::fmt;
use serde::{Deserialize, Serialize};

/// SHA-1 hash as a 40-char lowercase hex string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sha1Hash(String);

impl Sha1Hash {
    pub fn new(s: impl Into<String>) -> Option<Self> {
        let s = s.into();
        if s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Some(Self(s.to_lowercase()))
        } else {
            None
        }
    }

    pub fn static_range(&self) -> StaticRange {
        StaticRange(self.0[..4].to_string())
    }

    pub fn l1_dir(&self) -> &str { &self.0[..2] }
    pub fn l2_dir(&self) -> &str { &self.0[2..4] }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for Sha1Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StaticRange(String);
impl StaticRange {
    pub fn as_str(&self) -> &str { &self.0 }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileId(String);
impl FileId {
    pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
    pub fn as_str(&self) -> &str { &self.0 }
}
impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientKey(String);
impl ClientKey {
    pub fn new(s: impl Into<String>) -> Option<Self> {
        let s = s.into();
        if s.len() == 20 && s.chars().all(|c| c.is_ascii_alphanumeric()) {
            Some(Self(s))
        } else {
            None
        }
    }
    pub fn as_str(&self) -> &str { &self.0 }
    pub fn as_bytes(&self) -> &[u8] { self.0.as_bytes() }
}

/// File type with lowercase Display for use in file IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileType {
    Jpeg,
    Png,
    Gif,
    Mp4,
    Webm,
    Webp,
    Avif,
    Jxl,
    Other(String),
}

impl FileType {
    pub fn from_ext(ext: &str) -> Self {
        match ext.to_lowercase().as_str() {
            "jpg" | "jpeg" => Self::Jpeg,
            "png" => Self::Png,
            "gif" => Self::Gif,
            "mp4" => Self::Mp4,
            "wbm" | "webm" => Self::Webm,
            "wbp" | "webp" => Self::Webp,
            "avf" | "avif" => Self::Avif,
            "jxl" => Self::Jxl,
            other => Self::Other(other.to_string()),
        }
    }

    pub fn as_mime(&self) -> &str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::Gif => "image/gif",
            Self::Mp4 => "video/mp4",
            Self::Webm => "video/webm",
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
            Self::Jxl => "image/jxl",
            Self::Other(_) => "application/octet-stream",
        }
    }

    /// Return the lowercase file extension for use in file IDs.
    /// MUST match Java HVFile.getFileid() output: jpg, png, gif, mp4, wbm, wbp, avf, jxl
    pub fn as_ext(&self) -> &str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Mp4 => "mp4",
            Self::Webm => "wbm",
            Self::Webp => "wbp",
            Self::Avif => "avf",
            Self::Jxl => "jxl",
            Self::Other(s) => s.as_str(),
        }
    }
}
