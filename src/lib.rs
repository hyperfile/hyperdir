use std::io::Result;
use std::cmp::Ordering;
use std::time::SystemTime;
use std::str::FromStr;
use std::collections::HashMap;
use bytes::Bytes;
use ulid::Ulid;
use base64::{engine, alphabet, Engine as _};
use hyperfile::staging::config::StagingConfig;

#[allow(async_fn_in_trait)]
pub trait DirStaging {
    // list dir staging and filter out all inode object, return vec of scatter inodes
    async fn list_scatter_inodes(&self) -> Result<Vec<DirScatterInode>>;
    // fetch inodes, return vec of (scatter, inode raw)
    async fn collect_scatter_inodes(&self, v_scatters: Vec<DirScatterInode>) -> Result<Vec<(DirScatterInode, Bytes)>>;
    // remove list of inode object on dir staging
    async fn remove_scatter_inodes(&self, v: Vec<String>) -> Result<()>;
    // convert from file level staging to dir level
    fn to_dir_staging(staging: &Self) -> Self;
    // convert from generic staging config to dir staging config
    fn to_dir_staging_config(config: &StagingConfig) -> StagingConfig;
    // emit scatter event to dir location based on input file level staging
    async fn emit_scatter_event(&self, buf: &[u8], op: DirScatterInodeOp) -> Result<()>;
}

pub mod ondisk;
pub mod file;
pub mod s3;
pub mod hyper;
pub mod fs;

pub const DEFAULT_DIR_SUFFIX: &str = "_$folder$";
pub const DEFAULT_DIR_FILE_FOLDER: &str = "$dirfile$";
pub const DEFAUTL_DIR_INODE_MARKER: &str = "_$folder$/inode_";
pub const DEFAULT_DIR_FILE_SUFFIX: &str = "_$folder$/$dirfile$";

#[derive(Clone, Debug)]
#[repr(u8)]
pub enum DirScatterInodeOp {
    Create = 1,
    Update = 2,
    PreDelete = 3,
    Delete = 4,
    Unkown = 255,
}

impl DirScatterInodeOp {
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Create,
            2 => Self::Update,
            3 => Self::PreDelete,
            4 => Self::Delete,
            _ => Self::Unkown,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirScatterInode {
    pub key: String,
    pub op: DirScatterInodeOp,
    pub ulid: Ulid,
    pub filename: String,
    pub last_modified: SystemTime,
}

/// Scatter Inode Format: dir/staging/path[_$folder$]/[inode]_{ulid}_{filename in base64}_{op}_{size}
impl DirScatterInode {
    // decode path to dir staging root and file name
    pub fn path_decode(scatter_inode: &str, last_modified: SystemTime) -> Self {
        let components: Vec<&str> = scatter_inode.split(DEFAULT_DIR_SUFFIX).collect();
        assert!(components.len() == 2);
        assert!(components[1].starts_with("/inode_"));

        let key = scatter_inode.to_owned();

        let parts: Vec<&str> = components[1].split('_').collect();

        let ulid = Ulid::from_string(parts[1]).expect("failed to decode ulid from event path");

        let alphabet = alphabet::Alphabet::new("*!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
        let crazy_config = engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(engine::DecodePaddingMode::RequireNone);
        let crazy_engine = engine::GeneralPurpose::new(&alphabet, crazy_config);
        let decoded = crazy_engine.decode(parts[2]).expect("failed to decode filename from event path");
        let filename = String::from_utf8(decoded).expect("failed to get back string of filename");

        let op_u8 = u8::from_str(parts[3]).expect("failed to decode DirScatterInodeOp from event path ");
        let op = DirScatterInodeOp::from_u8(op_u8);

        Self { key, op, ulid, filename, last_modified }
    }

    pub fn path_encode(dir_staging_path: &str, filename: &str, op: u8) -> String {
        assert!(!dir_staging_path.ends_with("/"));
        let ulid = Ulid::new().to_string();

        let alphabet = alphabet::Alphabet::new("*!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
        let crazy_config = engine::GeneralPurposeConfig::new()
            .with_encode_padding(false);
        let crazy_engine = engine::GeneralPurpose::new(&alphabet, crazy_config);
        let encoded_filename = crazy_engine.encode(filename);

        format!("{dir_staging_path}{DEFAULT_DIR_SUFFIX}/inode_{ulid}_{encoded_filename}_{op}")
    }

    // input:
    //   - full list of scatter inodes
    // output:
    //   - latest view of scatters need to fetch inode binary or to delete
    pub fn filter_last_view(scatters: Vec<Self>) -> Vec<Self> {
        // group by filename use HashMap<filename, Vec<DirScatterInode>>
        let mut map: HashMap<String, Vec<Self>> = HashMap::new();
        for s in scatters.into_iter() {
            if let Some(v) = map.get_mut(&s.filename) {
                v.push(s);
            } else {
                let mut v = Vec::new();
                let key = s.filename.to_owned();
                v.push(s);
                map.insert(key, v);
            }
        }

        // iter map to sort by last_modified (if eq) and then ulid dt for each filename group
        for (_, v) in map.iter_mut() {
            v.sort_by(|a, b| {
                let res = a.last_modified.cmp(&b.last_modified);
                let res = if res == Ordering::Equal {
                    a.ulid.datetime().cmp(&b.ulid.datetime())
                } else {
                    res
                };
                if res == Ordering::Equal {
                    // FIXME: should be recoverable of this case
                    panic!("can not decide sequence of two scatter");
                }
                res
            });
        }

        let mut last_view: Vec<Self> = Vec::new();
        // dedup and merge by op
        for (_, mut v) in map.into_iter() {
            let last = v.pop().expect("unable to get last scatter from map");
            last_view.push(last);
        }

        last_view
    }
}
