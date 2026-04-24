use crate::types::{FileId, FileType, Sha1Hash};
use std::path::{Path, PathBuf};
use regex::Regex;
use std::sync::LazyLock;

static FILEID_WITH_RES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-f0-9]{40}-\d{1,10}-\d{1,5}-\d{1,5}-(jpg|png|gif|mp4|wbm|wbp|avf|jxl)$")
        .expect("invalid regex")
});

static FILEID_WITHOUT_RES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-f0-9]{40}-\d{1,10}-(jpg|png|gif|mp4|wbm|wbp|avf|jxl)$")
        .expect("invalid regex")
});

#[derive(Debug, Clone)]
pub struct HVFile {
    pub hash: Sha1Hash,
    pub size: u32,
    pub xres: u32,
    pub yres: u32,
    pub file_type: FileType,
}

impl HVFile {
    pub fn is_valid_fileid(s: &str) -> bool {
        FILEID_WITH_RES.is_match(s) || FILEID_WITHOUT_RES.is_match(s)
    }

    pub fn from_fileid(fileid: &str) -> Option<Self> {
        if !Self::is_valid_fileid(fileid) {
            return None;
        }
        let parts: Vec<&str> = fileid.split('-').collect();
        let hash = Sha1Hash::new(parts[0])?;
        let size: u32 = parts[1].parse().ok()?;

        let (xres, yres, file_type) = if parts.len() == 3 {
            (0u32, 0u32, FileType::from_ext(parts[2]))
        } else {
            let x: u32 = parts[2].parse().ok()?;
            let y: u32 = parts[3].parse().ok()?;
            (x, y, FileType::from_ext(parts[4]))
        };

        Some(Self { hash, size, xres, yres, file_type })
    }

    /// Build the full file ID string. Uses `as_ext()` for lowercase extensions,
    /// matching Java HVFile.getFileid() output byte-for-byte.
    pub fn fileid(&self) -> FileId {
        if self.xres > 0 {
            FileId::new(format!(
                "{}-{}-{}-{}-{}",
                self.hash.as_str(),
                self.size,
                self.xres,
                self.yres,
                self.file_type.as_ext()
            ))
        } else {
            FileId::new(format!(
                "{}-{}-{}",
                self.hash.as_str(),
                self.size,
                self.file_type.as_ext()
            ))
        }
    }

    pub fn cache_path(&self, cache_dir: &Path) -> PathBuf {
        cache_dir
            .join(self.hash.l1_dir())
            .join(self.hash.l2_dir())
            .join(self.fileid().as_str())
    }

    pub fn static_range(&self) -> String {
        self.hash.static_range().as_str().to_string()
    }

    pub fn mime_type(&self) -> &str {
        self.file_type.as_mime()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_fileid_with_res() {
        let id = "aabbccddeeff00112233445566778899aabbccdd-12345-800-600-jpg";
        assert!(HVFile::is_valid_fileid(id));
        let hv = HVFile::from_fileid(id).unwrap();
        assert_eq!(hv.size, 12345);
        assert_eq!(hv.xres, 800);
        assert_eq!(hv.yres, 600);
        assert!(matches!(hv.file_type, FileType::Jpeg));
    }

    #[test]
    fn test_valid_fileid_without_res() {
        let id = "aabbccddeeff00112233445566778899aabbccdd-99999-png";
        assert!(HVFile::is_valid_fileid(id));
        let hv = HVFile::from_fileid(id).unwrap();
        assert_eq!(hv.xres, 0);
        assert_eq!(hv.yres, 0);
    }

    #[test]
    fn test_invalid_fileid() {
        assert!(!HVFile::is_valid_fileid("not-valid"));
        assert!(HVFile::from_fileid("not-valid").is_none());
    }

    #[test]
    fn test_roundtrip_fileid_preserves_lowercase_ext() {
        let original = "aabbccddeeff00112233445566778899aabbccdd-12345-800-600-jpg";
        let hv = HVFile::from_fileid(original).unwrap();
        assert_eq!(hv.fileid().as_str(), original);
    }

    #[test]
    fn test_roundtrip_webm() {
        let original = "aabbccddeeff00112233445566778899aabbccdd-99999-wbm";
        let hv = HVFile::from_fileid(original).unwrap();
        assert_eq!(hv.fileid().as_str(), original);
    }

    #[test]
    fn test_mime_types() {
        let h = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let cases = [
            (format!("{}-100-jpg", h), "image/jpeg"),
            (format!("{}-100-mp4", h), "video/mp4"),
            (format!("{}-100-wbm", h), "video/webm"),
        ];
        for (full_id, expected_mime) in cases {
            let hv = HVFile::from_fileid(&full_id).unwrap();
            assert_eq!(hv.mime_type(), expected_mime);
        }
    }
}
