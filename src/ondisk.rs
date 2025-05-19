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
            name: [0u8; DEFAULT_NAME_LEN + 1],
        }
    }
}

impl DirFileEntryRaw {
    pub(crate) fn from(inode_raw: &InodeRaw, filename: &[u8]) -> Self {
        let mut raw: Self = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        raw.inode = *inode_raw;
        if filename.len() > DEFAULT_NAME_LEN + 1 {
            raw.name.copy_from_slice(&filename[0..DEFAULT_NAME_LEN]);
            warn!("filename {} too long, trimed to {}",
                std::str::from_utf8(filename).unwrap(),
                std::str::from_utf8(&raw.name).unwrap());
        } else {
            let dest = &mut raw.name[0..filename.len()];
            dest.copy_from_slice(filename);
        };
        raw
    }

    pub(crate) fn from_slice(buf: &[u8]) -> Self {
        let sz = std::mem::size_of::<Self>();
        if sz > buf.len() {
            panic!("invalid size of input, DirFileEntryRaw size {} - buffer size: {}", sz, buf.len());
        }
        let mut raw: Self = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let (trimed, _) = buf.split_at(sz);
        raw.as_mut_u8_slice().copy_from_slice(trimed);
        raw
    }

    pub(crate) fn as_mut_u8_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self) as *mut u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub(crate) fn as_u8_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub(crate) fn dummy_value() -> Self {
        Self {
            inode: InodeRaw::default(),
            name: DUMMY_FILENAME,
        }
    }

    pub(crate) fn is_dummy(&self) -> bool {
        &self.name == &DUMMY_FILENAME
    }
}
