use std::io::Result;
use std::time::SystemTime;
use std::str::FromStr;
use std::collections::HashMap;
use bytes::Bytes;
use ulid::Ulid;
use uuid::Uuid;
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
    // convert from generic staging config to dir staging config
    fn to_dir_staging_config(config: &StagingConfig) -> StagingConfig;
    // Emit a scatter event into THIS directory's `!/` namespace.
    //
    // `self` is the parent directory's staging (the directory that owns the
    // entry). `filename` and `child_uuid` identify the child whose change is
    // being committed. The parent cannot be derived from the child's prefix
    // under UUID addressing, so both must be supplied explicitly.
    async fn emit_scatter_event(&self, filename: &str, child_uuid: &Uuid, buf: &[u8], op: DirScatterInodeOp) -> Result<()>;
    // get scatter inodes path
    fn scatter_inodes_path(&self) -> String;

    /// Path of this directory's compactor lease object.
    fn compact_lease_path(&self) -> String;

    /// Try to acquire the leader lease for this directory's compactor.
    ///
    /// On success returns a guard whose `etag` is the conditional token to be
    /// passed to [`release_compact_lease`]. If a fresh (non-expired) lease is
    /// already held by another holder, returns `Err(ErrorKind::ResourceBusy)`
    /// without writing or modifying anything in S3. If a lease is found but
    /// has expired past `expires_at_unix_ms`, takes it over via `If-Match`.
    async fn acquire_compact_lease(&self, ttl_ms: u64) -> Result<CompactLeaseGuard>;

    /// Release a previously-acquired compactor lease.
    ///
    /// Best-effort: if the underlying object has been taken over by another
    /// holder (we lost the race after our lease expired), the conditional
    /// DELETE returns 412 and this method logs and returns `Ok(())`. Other
    /// S3 errors are logged but not propagated, since a held lease will
    /// expire naturally on TTL.
    async fn release_compact_lease(&self, guard: CompactLeaseGuard) -> Result<()>;
}

/// Default TTL for the compactor leader lease. A compactor that holds the
/// lease but crashes will block another compactor for at most this duration.
pub const DEFAULT_COMPACT_LEASE_TTL_MS: u64 = 60_000;

/// Object name (under each directory's prefix) used for the compactor lease.
pub const DEFAULT_COMPACT_LEASE_FILE: &str = "_compact.lease";

/// On-disk header carried in the body of a Delete (tombstone) scatter object.
///
/// Wire format of a Delete scatter body is:
///
/// ```text
/// +------------------------+----------------------+
/// | TombstoneHeader (16 B) | InodeRaw (~512 B)    |
/// +------------------------+----------------------+
/// ```
///
/// The `InodeRaw` portion is verbatim: the same bytes that hyperfile would
/// have written to the file's own `<child>/inode` object. Preserving it
/// means a future `fs_undelete` can rebuild the parent directory's bmap entry
/// without having to re-read the (still-present) child prefix, and it keeps
/// the audit trail "what exactly was deleted" inside the scatter object.
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy)]
pub struct TombstoneHeader {
    /// Wall-clock time the deletion was requested, in milliseconds since the
    /// Unix epoch.
    pub deleted_at_unix_ms: i64,
    /// Earliest wall-clock time at which `fs_gc` is permitted to physically
    /// reclaim the child file's prefix. A value of `0` means "no retention",
    /// i.e. the next GC pass may immediately reclaim.
    pub retention_until_unix_ms: i64,
}

impl TombstoneHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Build the body of a Delete scatter: `TombstoneHeader` followed by the raw
/// inode bytes captured from the child file at the moment of unlink.
pub(crate) fn build_tombstone_body(
    deleted_at_unix_ms: i64,
    retention_until_unix_ms: i64,
    inode_raw_bytes: &[u8],
) -> Vec<u8> {
    let header = TombstoneHeader { deleted_at_unix_ms, retention_until_unix_ms };
    // Safety: `header` is a `#[repr(C)]` plain-old-data struct of fixed size,
    // and the slice is read-only and lifetime-bounded to this scope.
    let header_bytes = unsafe {
        std::slice::from_raw_parts(
            (&header as *const TombstoneHeader) as *const u8,
            TombstoneHeader::SIZE,
        )
    };
    let mut body = Vec::with_capacity(TombstoneHeader::SIZE + inode_raw_bytes.len());
    body.extend_from_slice(header_bytes);
    body.extend_from_slice(inode_raw_bytes);
    body
}

/// Parse a tombstone scatter body into its header and the trailing inode
/// payload. Returns the inode bytes by reference into `buf`; callers that
/// need to keep them past `buf`'s lifetime must copy.
pub(crate) fn parse_tombstone_body(buf: &[u8]) -> Result<(TombstoneHeader, &[u8])> {
    if buf.len() < TombstoneHeader::SIZE {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
            format!("tombstone body too small: {} < {} bytes",
                    buf.len(), TombstoneHeader::SIZE)));
    }
    let (header_bytes, rest) = buf.split_at(TombstoneHeader::SIZE);
    // Safety: `header_bytes.len() == TombstoneHeader::SIZE` and the struct is
    // `#[repr(C, align(8))]` with no internal references; reading it from a
    // byte slice this way is well-defined as long as the slice is properly
    // aligned. We copy by value into the local `header` so subsequent reads
    // off `buf` don't alias.
    let header = unsafe { std::ptr::read_unaligned(header_bytes.as_ptr() as *const TombstoneHeader) };
    Ok((header, rest))
}

/// Handle to an acquired compactor lease. Pass back to
/// [`DirStaging::release_compact_lease`] to relinquish it.
///
/// Holds no resources on its own; dropping this without calling
/// `release_compact_lease` simply waits for the TTL to expire.
#[derive(Debug, Clone)]
pub struct CompactLeaseGuard {
    /// Caller-side identifier of the lease holder (a fresh ULID per acquire).
    /// Useful in logs to identify "who has the lease".
    pub holder_id: String,
    /// S3 ETag of the lease object as last written by us. The conditional
    /// DELETE in `release_compact_lease` uses this so we don't release a
    /// lease that has since been taken over.
    pub etag: String,
    /// Full S3 key of the lease object. Carried for debug logging.
    pub lease_key: String,
    /// Wall-clock expiration the lease was acquired with. Past this time, any
    /// other client may take over the lease.
    pub expires_at_unix_ms: i64,
}

/// Wire format for the lease object body. Two-line key=value, ASCII-only,
/// no external dep. Robust enough for an object that is touched at most once
/// per compaction round.
pub(crate) fn format_lease_body(holder: &str, expires_at_unix_ms: i64) -> String {
    format!("holder={}\nexpires_at_unix_ms={}\n", holder, expires_at_unix_ms)
}

pub(crate) fn parse_lease_body(buf: &[u8]) -> Result<(String, i64)> {
    let s = std::str::from_utf8(buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData,
            format!("compact lease body not UTF-8: {}", e)))?;
    let mut holder: Option<String> = None;
    let mut expires_at: Option<i64> = None;
    for line in s.lines() {
        if let Some(v) = line.strip_prefix("holder=") {
            holder = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("expires_at_unix_ms=") {
            expires_at = v.parse::<i64>().ok();
        }
    }
    let holder = holder.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData,
        "compact lease body missing 'holder='"))?;
    let expires_at = expires_at.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData,
        "compact lease body missing 'expires_at_unix_ms='"))?;
    Ok((holder, expires_at))
}

/// Current Unix time in milliseconds. Saturates rather than wrapping; in the
/// extremely unlikely case of a system clock past i64::MAX ms past epoch, the
/// lease comparison will treat any held lease as expired (correct fallback).
pub(crate) fn unix_now_ms() -> i64 {
    use std::time::UNIX_EPOCH;
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub mod ondisk;
pub mod file;
pub mod s3;
pub mod hyper;
pub mod fs;
pub mod interceptor;
pub mod layout;

pub use interceptor::ScatterFirstInterceptor;
pub use file::CompactStats;
pub use fs::GcStats;
pub use layout::{HyperDirLayout, ROOT_DIR_UUID};

pub const DEFAULT_DIR_INODE_SCATTER_FOLDER: &str = "!";
/// Suffix of a live (Create/Update) inode scatter object.
pub const DEFAULT_DIR_INODE_SUFFIX: &str = ".inode";
/// Suffix of a tombstone (Delete) scatter object.
pub const DEFAULT_DIR_TOMBSTONE_SUFFIX: &str = ".tombstone";

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
    pub uuid: Uuid,
    pub filename: String,
    pub last_modified: SystemTime,
}

/// Scatter Inode Format:
/// `<parent dir key>/{DEFAULT_DIR_INODE_SCATTER_FOLDER}/{ulid}_{base64(filename)}_{uuid}_{op}{suffix}`
///
/// `suffix` is `.tombstone` for a Delete op and `.inode` for everything else,
/// so live entries and tombstones can be filtered by object suffix alone.
///
/// `uuid` is the child's own UUID (the prefix where its hyperfile lives,
/// `DIR/<uuid>` or `FILE/<uuid>`). It is carried in the key because a rename
/// changes only `filename` while the child keeps its identity, and because
/// the parent cannot otherwise recover the child's prefix from its name.
impl DirScatterInode {
    // decode path to dir staging root and file name
    pub fn path_decode(scatter_inode: &str, last_modified: SystemTime) -> Self {
        let components: Vec<&str> = scatter_inode.split(DEFAULT_DIR_INODE_SCATTER_FOLDER).collect();
        assert!(components.len() == 2);

        let key = scatter_inode.to_owned();

        // components[1] = "/{ulid}_{base64(name)}_{uuid}_{op}{suffix}"
        let stem = components[1]
            .trim_start_matches('/')
            .strip_suffix(DEFAULT_DIR_INODE_SUFFIX)
            .or_else(|| components[1].trim_start_matches('/').strip_suffix(DEFAULT_DIR_TOMBSTONE_SUFFIX))
            .expect("scatter object missing .inode/.tombstone suffix");

        // "{ulid}_{base64(name)}_{uuid}_{op}" split on '_':
        //   [ ulid, base64(name), uuid, op ]
        // base64 uses an alphabet of [*-A-Za-z0-9] (no '_') and the uuid is
        // hyphenated (no '_'), so '_' is an unambiguous separator.
        let parts: Vec<&str> = stem.split('_').collect();

        let ulid = Ulid::from_string(parts[0]).expect("failed to decode ulid from event path");

        let alphabet = alphabet::Alphabet::new("*-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
        let crazy_config = engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(engine::DecodePaddingMode::RequireNone);
        let crazy_engine = engine::GeneralPurpose::new(&alphabet, crazy_config);
        let decoded = crazy_engine.decode(parts[1]).expect("failed to decode filename from event path");
        let filename = String::from_utf8(decoded).expect("failed to get back string of filename");

        let uuid = Uuid::parse_str(parts[2]).expect("failed to decode uuid from event path");

        let op_u8 = u8::from_str(parts[3]).expect("failed to decode DirScatterInodeOp from event path ");
        let op = DirScatterInodeOp::from_u8(op_u8);

        Self { key, op, ulid, uuid, filename, last_modified }
    }

    pub fn path_encode(dir_staging_path: &str, filename: &str, uuid: &Uuid, op: u8) -> String {
        assert!(!dir_staging_path.ends_with("/"));
        let ulid = Ulid::new().to_string();

        let alphabet = alphabet::Alphabet::new("*-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap();
        let crazy_config = engine::GeneralPurposeConfig::new()
            .with_encode_padding(false);
        let crazy_engine = engine::GeneralPurpose::new(&alphabet, crazy_config);
        let encoded_filename = crazy_engine.encode(filename);

        let suffix = if op == DirScatterInodeOp::Delete as u8 {
            DEFAULT_DIR_TOMBSTONE_SUFFIX
        } else {
            DEFAULT_DIR_INODE_SUFFIX
        };

        format!("{dir_staging_path}/{DEFAULT_DIR_INODE_SCATTER_FOLDER}/{ulid}_{encoded_filename}_{uuid}_{op}{suffix}")
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


#[cfg(test)]
mod scatter_key_tests {
    use super::*;
    use std::time::SystemTime;

    fn roundtrip(name: &str, op: DirScatterInodeOp) {
        let uuid = Uuid::new_v4();
        let op_u8 = op.clone() as u8;
        let key = DirScatterInode::path_encode("DIR/parent-uuid", name, &uuid, op_u8);
        let decoded = DirScatterInode::path_decode(&key, SystemTime::UNIX_EPOCH);
        assert_eq!(decoded.filename, name);
        assert_eq!(decoded.uuid, uuid);
        assert_eq!(decoded.op as u8, op_u8);
    }

    #[test]
    fn inode_suffix_for_non_delete() {
        let key = DirScatterInode::path_encode(
            "DIR/p", "foo", &Uuid::nil(), DirScatterInodeOp::Create as u8);
        assert!(key.ends_with(DEFAULT_DIR_INODE_SUFFIX));
        assert!(!key.ends_with(DEFAULT_DIR_TOMBSTONE_SUFFIX));
    }

    #[test]
    fn tombstone_suffix_for_delete() {
        let key = DirScatterInode::path_encode(
            "DIR/p", "foo", &Uuid::nil(), DirScatterInodeOp::Delete as u8);
        assert!(key.ends_with(DEFAULT_DIR_TOMBSTONE_SUFFIX));
    }

    #[test]
    fn roundtrip_ops_and_names() {
        roundtrip("hello.txt", DirScatterInodeOp::Create);
        roundtrip("with space", DirScatterInodeOp::Update);
        roundtrip("a/weird\\name", DirScatterInodeOp::Delete);
        roundtrip("unicode-\u{6587}\u{4ef6}", DirScatterInodeOp::Create);
    }
}
