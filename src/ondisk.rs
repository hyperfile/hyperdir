use std::fmt;
use log::warn;
use hyperfile::ondisk::InodeRaw;

pub(crate) const DEFAULT_NAME_LEN: usize = 255;
const _DUMMY_FILENAME: [u8; DEFAULT_NAME_LEN + 1] = [0xFFu8; DEFAULT_NAME_LEN + 1];
pub(crate) const DUMMY_FILENAME: [u8; DEFAULT_NAME_LEN + 1] = {
    let mut n = _DUMMY_FILENAME;
    n[DEFAULT_NAME_LEN] = 0;
    n
};

// define dir file's inode
pub struct DirFileInodeRaw {
    pub inode: InodeRaw,
}

impl DirFileInodeRaw {
    pub fn as_mut_u8_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self) as *mut u8,
                std::mem::size_of::<Self>()
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct DirFileEntryRaw {
    pub(crate) inode: InodeRaw,
    /// UUID of the child's own prefix (`DIR/<uuid>` or `FILE/<uuid>`). This is
    /// the stable identity used to address the child; the directory entry's
    /// `name` may change via rename without touching it.
    pub(crate) uuid: [u8; 16],
    pub(crate) name: [u8; DEFAULT_NAME_LEN + 1],
}

impl fmt::Display for DirFileEntryRaw {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_dummy() {
            write!(f, "[DUMMRY VALUE ENTRY]")
        } else {
            write!(f, "{}", std::str::from_utf8(&self.name).unwrap())
        }
    }
}

impl Default for DirFileEntryRaw {
    fn default() -> Self {
        Self {
            inode: InodeRaw::default(),
            uuid: [0u8; 16],
            name: [0u8; DEFAULT_NAME_LEN + 1],
        }
    }
}

impl DirFileEntryRaw {
    pub fn from(inode_raw: &InodeRaw, uuid: &[u8; 16], filename: &[u8]) -> Self {
        let mut raw: Self = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        raw.inode = *inode_raw;
        raw.uuid = *uuid;
        // raw.name is DEFAULT_NAME_LEN + 1 bytes; the last byte is reserved for
        // the trailing NUL so DirFileEntry::from_raw can parse with CStr.
        // Hence the maximum usable filename byte length is DEFAULT_NAME_LEN.
        if filename.len() > DEFAULT_NAME_LEN {
            raw.name[..DEFAULT_NAME_LEN].copy_from_slice(&filename[..DEFAULT_NAME_LEN]);
            raw.name[DEFAULT_NAME_LEN] = 0;
            warn!("filename {} too long, trimmed to {} bytes",
                String::from_utf8_lossy(filename),
                DEFAULT_NAME_LEN);
        } else {
            raw.name[..filename.len()].copy_from_slice(filename);
            // remaining bytes including the trailing NUL are already zero
            // from MaybeUninit::zeroed above
        };
        raw
    }

    pub fn from_slice(buf: &[u8]) -> Self {
        let sz = std::mem::size_of::<Self>();
        if sz > buf.len() {
            panic!("invalid size of input, DirFileEntryRaw size {} - buffer size: {}", sz, buf.len());
        }
        let mut raw: Self = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let (trimed, _) = buf.split_at(sz);
        raw.as_mut_u8_slice().copy_from_slice(trimed);
        raw
    }

    pub fn as_mut_u8_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self) as *mut u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub fn as_u8_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub fn dummy_value() -> Self {
        Self {
            inode: InodeRaw::default(),
            uuid: [0u8; 16],
            name: DUMMY_FILENAME,
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.name == DUMMY_FILENAME
    }

    /// The stored filename bytes, up to (not including) the NUL terminator.
    pub fn name_bytes(&self) -> &[u8] {
        let end = self.name.iter().position(|&b| b == 0).unwrap_or(self.name.len());
        &self.name[..end]
    }

    /// True if this entry's filename equals `name` (raw byte compare).
    pub fn name_eq(&self, name: &[u8]) -> bool {
        self.name_bytes() == name
    }
}
