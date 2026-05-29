use std::io::Result;
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
    // get scatter inodes path
    fn scatter_inodes_path(&self) -> String;
}

pub mod ondisk;
pub mod file;
pub mod s3;
pub mod hyper;
pub mod fs;
pub mod interceptor;

pub use interceptor::ScatterFirstInterceptor;
pub use file::CompactStats;

pub const DEFAULT_DIR_INODE_SCATTER_FOLDER: &str = "!";
pub const DEFAULT_DIR_INODE_MARKER: &str = "inode_";

#[derive(Clone, Debug)]
#[repr(u8)]
pub enum DirScatterInodeOp {
    Create = 1,
    Update = 2,
    PreDelete = 3,
    Delete = 4,
    Unknown = 255,
}

impl DirScatterInodeOp {
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Create,
            2 => Self::Update,
            3 => Self::PreDelete,
            4 => Self::Delete,
            _ => Self::Unknown,
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

/// Scatter Inode Format: dir/staging/dirname/{DEFAULT_DIR_INODE_SCATTER_FOLDER}/[inode]_{ulid}_{filename in base64}_{op}
impl DirScatterInode {
    // decode path to dir staging root and file name
    pub fn path_decode(scatter_inode: &str, last_modified: SystemTime) -> Self {
        let components: Vec<&str> = scatter_inode.split(DEFAULT_DIR_INODE_SCATTER_FOLDER).collect();
        assert!(components.len() == 2);
        assert!(components[1].starts_with("/inode_"));

        let key = scatter_inode.to_owned();

        let parts: Vec<&str> = components[1].split('_').collect();

        let ulid = Ulid::from_string(parts[1]).expect("failed to decode ulid from event path");

        let alphabet = alphabet::Alphabet::new("*-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
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

        let alphabet = alphabet::Alphabet::new("*-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
        let crazy_config = engine::GeneralPurposeConfig::new()
            .with_encode_padding(false);
        let crazy_engine = engine::GeneralPurpose::new(&alphabet, crazy_config);
        let encoded_filename = crazy_engine.encode(filename);

        format!("{dir_staging_path}/{DEFAULT_DIR_INODE_SCATTER_FOLDER}/inode_{ulid}_{encoded_filename}_{op}")
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

        // iter map to build a total order per filename group:
        //   1. last_modified (S3-side time)
        //   2. ulid (full 128-bit, monotonic per process)
        //   3. key (S3 object key, globally unique by S3 semantics)
        // The third tier is belt-and-suspenders: ulid alone is already unique
        // unless two writers in the same millisecond happen to draw colliding
        // randomness, but key uniqueness is guaranteed by S3 itself, so this
        // gives a stable total order without panics.
        for (_, v) in map.iter_mut() {
            v.sort_by(|a, b| {
                a.last_modified.cmp(&b.last_modified)
                    .then_with(|| a.ulid.cmp(&b.ulid))
                    .then_with(|| a.key.cmp(&b.key))
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
