use std::io::{Result, Error, ErrorKind};
use std::time::{Instant, Duration};
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use std::ffi::OsStr;
use std::ffi::CStr;
use std::collections::{HashMap, HashSet, BTreeMap};
use log::warn;
use uuid::Uuid;
use tokio::sync::{
    Semaphore, OwnedSemaphorePermit,
    Mutex, OwnedMutexGuard,
};
use btree_ondisk::{BlockLoader, bmap::BMap, NodeValue, NullNodeCache};
use btree_ondisk::btree::BtreeNodeDirty;
use btree_ondisk::DEFAULT_CACHE_UNLIMITED;
use hyperfile::file::{HyperTrait, DirtyDataBlocks, FlushTiming};
use hyperfile::{BlockIndex, BlockPtr, SegmentId, SegmentOffset, BMapUserData};
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::inode::{Inode, FlushInodeFlag};
use hyperfile::config::{HyperFileConfig, HyperFileMetaConfig};
use hyperfile::staging::{StagingIntercept, Staging, config::StagingConfig};
use hyperfile::segment::SegmentReadWrite;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::file::mode::HyperFileMode;
use hyperfile::ondisk::{BMapRawType, InodeRaw};
use super::ondisk::{DirFileEntryRaw, DEFAULT_NAME_LEN};
use super::{DirStaging, DirScatterInode, DirScatterInodeOp};

pub const DIR_FILE_ENTRY_RAW_SIZE: usize = std::mem::size_of::<DirFileEntryRaw>();

#[derive(Debug)]
pub struct DirFileEntry {
    pub inode: Inode,
    pub uuid: Uuid,
    pub name: String,
}

impl DirFileEntry {
    fn from_raw(raw: &DirFileEntryRaw) -> Self {
        let name = match CStr::from_bytes_until_nul(&raw.name) {
            Ok(cstr) => {
                match cstr.to_str() {
                    Ok(s) => { s.to_string() },
                    Err(e) => {
                        // invalid filename
                        warn!("decode file name error: {}", e);
                        String::new()
                    },
                }
            },
            Err(e) => {
                // invalid filename
                warn!("decode file name error: {}", e);
                String::new()
            },
        };

        Self {
            inode: Inode::from_raw(&raw.inode, None),
            uuid: Uuid::from_bytes(raw.uuid),
            name,
        }
    }
}

pub(crate) type EntryNameHash = u64;

/// Statistics returned from [`HyperDirFile::compact`].
#[derive(Default, Debug, Clone, Copy)]
pub struct CompactStats {
    /// Number of scatter objects listed and processed in this round.
    pub scatters_processed: usize,
    /// Number of bmap entries inserted/updated from Create/Update scatters.
    pub entries_added: usize,
    /// Number of bmap entries removed from Delete scatters.
    pub entries_removed: usize,
    /// Number of Delete scatters that won their filename group and were
    /// retained in S3 as tombstones (subject to retention; reclaimed by
    /// [`HyperDir::fs_gc`]).
    pub tombstones_kept: usize,
    /// True when the scatter list was empty; flush + scatter cleanup were
    /// skipped and this call had no S3 side effects.
    pub no_op: bool,
}

/// Internal: the result of listing + classifying + body-fetching scatter
/// objects, ready to be applied to either an in-memory view (`read_dir`) or
/// the persisted bmap (`compact`).
struct ScatterChanges {
    /// All scatter object keys that were observed in the LIST. A successful
    /// compact will delete this set after the bmap flush commits, *except*
    /// for any keys that appear in `tombstone_keys`. `read_dir` ignores
    /// this field.
    all_keys: Vec<String>,
    /// Filename -> entry, for filenames whose latest scatter is Create or
    /// Update. Keyed by the full filename (not its hash) so two distinct
    /// names that share a CRC64 are both preserved within one batch.
    upserts: HashMap<String, DirFileEntryRaw>,
    /// Filenames whose latest scatter is Delete.
    deletes: HashSet<String>,
    /// Subset of `all_keys`: the latest Delete scatter per filename. These
    /// must NOT be deleted by `compact`; they remain in S3 as tombstones
    /// until `fs_gc` reclaims them after their retention expires.
    tombstone_keys: HashSet<String>,
}

#[inline]
fn hash_filename(name: &str) -> EntryNameHash {
    let mut c = crc64fast::Digest::new();
    c.write(name.as_bytes());
    c.sum64()
}

#[inline]
fn hash_filename_bytes(name: &[u8]) -> EntryNameHash {
    let mut c = crc64fast::Digest::new();
    c.write(name);
    c.sum64()
}

impl NodeValue for DirFileEntryRaw {
    fn is_invalid(&self) -> bool {
        self.name == [0u8; DEFAULT_NAME_LEN + 1]
    }

    fn invalid_value() -> DirFileEntryRaw {
        DirFileEntryRaw::default()
    }

    fn is_valid_extern_assign(&self) -> bool {
        false
    }
}

pub struct HyperDirFile<'a, T: Send + Clone, L: BlockLoader<BlockPtr>> {
    bmap: BMap<'a, BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>,
    staging: T,
    bmap_ud: BMapUserData,
    config: HyperFileConfig,
    flags: HyperFileFlags,
    last_flush: Instant,
    sema: Arc<Semaphore>,
    flush_lock: Arc<Mutex<()>>,
    inode: Inode,
    flush_timing: FlushTiming,
}

impl<'a, T, L> HyperDirFile<'a, T, L>
    where
        T: Staging<L> + SegmentReadWrite + DirStaging + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone,
{
    pub async fn new(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader, NullNodeCache)?;

        let inode = Inode::default_dir()
            .with_mode(&mode)
            .with_meta_config(&meta_config);
        let bmap_ud = BMapUserData::new(BlockPtrFormat::Flat);
        bmap.set_userdata(bmap_ud.as_u32());

        let mut file = Self {
            bmap,
            staging,
            bmap_ud,
            config,
            flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(1)),
            flush_lock: Arc::new(Mutex::new(())),
            inode,
            flush_timing: FlushTiming::default(),
        };
        // flush inode for hyper file new created
        file.flush_inode(FlushInodeFlag::Create).await?;
        Ok(file)
    }

    /// open a hyper file
    /// open by loading inode from staging,
    /// if inode is not found in staging, create hyper file from scratch
    pub async fn open(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        Self::do_open(staging, meta_block_loader, config, flags, 0).await
    }

    /// open a hyper file with cno for read-only
    pub async fn open_cno(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        if !flags.is_rdonly() {
            return Err(Error::new(ErrorKind::ReadOnlyFilesystem, "write access is not allowed for open specific cno"));
        }
        Self::do_open(staging, meta_block_loader, config, flags, cno).await
    }

    async fn do_open(staging: T, meta_block_loader: L, mut config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let res_inode = if cno == 0 {
            staging.load_inode(raw_inode.as_mut_u8_slice()).await
        } else {
            staging.load_inode_from_segment(raw_inode.as_mut_u8_slice(), cno as SegmentId).await
        };
        let inode_state = match res_inode {
            Ok(od_state) => {
                /* if we load inode without error, we use inode as truth of metadata */
                od_state
            },
            Err(e) => {
                return Err(e);
            },
        };
        // get back meta config from inode raw
        let meta_config = HyperFileMetaConfig::from_u32(raw_inode.i_meta_config);
        let b = raw_inode.i_bmap;
        let bmap = BMap::<BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>::read(&b, meta_config.meta_block_size, meta_block_loader, NullNodeCache)?;
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            1
        };

        // overwrite the default meta config with the one we get from inode
        config.meta = meta_config;

        let mut file = Self {
            bmap,
            staging,
            bmap_ud,
            config,
            flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
            inode: Inode::from_raw(&raw_inode, inode_state),
            flush_timing: FlushTiming::default(),
        };
        // refresh bmap if need to do recovery
        let _ = file.refresh_bmap().await?;
        Ok(file)
    }

    pub async fn release(&mut self) -> Result<SegmentId> {
        self.flush().await
    }

    pub fn stat(&self) -> libc::stat {
        // TODO: set dev and rdev here
        let dev = 0;
        let rdev = 0;
        self.inode.to_stat(dev, rdev)
    }

    // fast stat by read inode without open file
    pub async fn stat_fast(staging: T) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        staging.load_inode(raw_inode.as_mut_u8_slice()).await?;
        let inode = Inode::from_raw(&raw_inode, None);
        Ok(inode.to_stat(0, 0))
    }

    // fast update stat by load inode and flush inode
    pub async fn update_stat_fast(staging: T, stat: &libc::stat) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let od_state = staging.load_inode(raw_inode.as_mut_u8_slice()).await?;
        let mut inode = Inode::from_raw(&raw_inode, od_state);
        inode.update_stat(stat);
        let raw = inode.to_raw(raw_inode.i_bmap);
        let od_state = inode.get_ondisk_state();
        let _ = staging.flush_inode(raw.as_u8_slice(), od_state, FlushInodeFlag::Update).await?;
        Ok(inode.to_stat(stat.st_dev, stat.st_rdev))
    }

    pub async fn update_stat(&mut self, stat: &libc::stat) -> Result<libc::stat> {
        let stat = self.inode.update_stat(stat);
        let _ = self.flush().await?;
        Ok(stat)
    }

    pub async fn flush_inode(&mut self, flag: FlushInodeFlag) -> Result<()> {
        // TODO update necessary inode fields
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap.as_slice());
        let raw_inode = self.inode.to_raw(b);
        let od_state = self.staging.flush_inode(raw_inode.as_u8_slice(), self.inode.get_ondisk_state(), flag).await?;
        self.inode.clear_attr_dirty();
        self.inode.set_ondisk_state(od_state);
        self.inode.set_last_ondisk_cno(self.inode.get_last_cno());
        Ok(())
    }

    pub fn staging_config(&self) -> &StagingConfig {
        &self.config.staging
    }

    pub fn staging_interceptor(&mut self, i: impl StagingIntercept<T> + 'static) {
        self.staging.interceptor(i);
    }

    pub fn flags(&self) -> & HyperFileFlags {
        &self.flags
    }

    /// Look up a single directory entry by name.
    ///
    /// Resolves CRC64 collisions by open-addressing: probes from the name's
    /// home slot, comparing the full name stored in each entry until a match
    /// or an empty slot (NotFound) is found.
    pub async fn read_entry(&self, name: &str) -> Result<DirFileEntry> {
        let home = hash_filename(name);
        match self.probe_lookup(name.as_bytes(), home).await? {
            Some((_, entry)) => Ok(DirFileEntry::from_raw(&entry)),
            None => Err(Error::new(ErrorKind::NotFound, format!("entry not found: {}", name))),
        }
    }

    /// Pure-read directory enumeration.
    ///
    /// Lists outstanding scatter objects, walks the persisted bmap, and merges
    /// the two views in memory keyed by filename. Does not modify any S3
    /// state, so it is safe to call concurrently from multiple clients.
    ///
    /// The returned snapshot reflects "the latest scatter applied on top of
    /// the bmap as of the LIST/GET round-trips". Two consecutive calls may
    /// differ if other writers commit between them. Outstanding scatter
    /// objects are *not* deleted here; that is the job of [`compact`].
    pub async fn read_dir(&self) -> Result<Vec<DirFileEntry>> {
        let changes = self.collect_scatter_changes().await?;

        // start from the persisted bmap snapshot, keyed by filename
        let mut view: HashMap<String, DirFileEntryRaw> = HashMap::new();
        for entry in self.walk_bmap_snapshot().await? {
            view.insert(String::from_utf8_lossy(entry.name_bytes()).into_owned(), entry);
        }

        // overlay scatter changes (also keyed by filename)
        for (name, entry) in changes.upserts {
            view.insert(name, entry);
        }
        for name in &changes.deletes {
            view.remove(name);
        }

        Ok(view
            .into_values()
            .map(|raw| DirFileEntry::from_raw(&raw))
            .collect())
    }

    /// Consolidate outstanding scatter objects into the persisted bmap.
    ///
    /// Acquires the directory's compactor leader lease before doing any work
    /// (see [`DirStaging::acquire_compact_lease`]). Concurrent callers on the
    /// same directory will see `Err(ErrorKind::ResourceBusy)` returned fast,
    /// rather than racing through the consolidate-and-flush path; correctness
    /// also relies on hyperfile's inode-flush OCC, but the lease avoids the
    /// duplicated I/O that OCC alone cannot.
    ///
    /// On a clean run the lease is released as soon as the consolidation
    /// commits; on any error or panic, the lease will be reclaimed by the
    /// next caller after [`DEFAULT_COMPACT_LEASE_TTL_MS`] passes.
    ///
    /// If the scatter LIST is empty this method is a no-op (no flush, no
    /// scatter delete) and returns `CompactStats { no_op: true, .. }`.
    pub async fn compact(&mut self) -> Result<CompactStats> {
        let lease = self.staging
            .acquire_compact_lease(crate::DEFAULT_COMPACT_LEASE_TTL_MS)
            .await?;
        let result = self.compact_inner().await;
        // Always best-effort release, regardless of compact_inner's outcome.
        // release_compact_lease itself only propagates errors that are not
        // expected as a side effect of the lease's natural lifecycle.
        let _ = self.staging.release_compact_lease(lease).await;
        result
    }

    /// Body of [`compact`], assuming the caller has already acquired the
    /// compactor lease for this directory.
    async fn compact_inner(&mut self) -> Result<CompactStats> {
        let changes = self.collect_scatter_changes().await?;
        if changes.all_keys.is_empty() {
            return Ok(CompactStats { no_op: true, ..Default::default() });
        }

        let scatters_processed = changes.all_keys.len();
        let tombstones_kept = changes.tombstone_keys.len();

        let mut entries_added = 0;
        for (name, entry) in &changes.upserts {
            self.probe_upsert(name.as_bytes(), *entry).await?;
            entries_added += 1;
        }

        let mut entries_removed = 0;
        for name in &changes.deletes {
            if self.probe_delete(name.as_bytes()).await? {
                entries_removed += 1;
            }
            // A tombstone for a name that was never persisted (created and
            // deleted before any compact ran, or already consolidated) just
            // returns false; idempotent.
        }

        // hyperfile's flush handles OCC retry per FlushConflictPolicy.
        self.flush().await?;

        // Best-effort scatter cleanup. Keep tombstones (latest Delete scatter
        // per filename) so fs_gc can find and process them after retention;
        // strip everything else: stale scatters of any op, plus consolidated
        // Create/Update winners. Re-application on the bmap side is
        // idempotent if we ever see a tombstone twice.
        let to_delete: Vec<String> = changes.all_keys.into_iter()
            .filter(|k| !changes.tombstone_keys.contains(k))
            .collect();
        if !to_delete.is_empty() {
            self.staging.remove_scatter_inodes(to_delete).await?;
        }

        Ok(CompactStats {
            scatters_processed,
            entries_added,
            entries_removed,
            tombstones_kept,
            no_op: false,
        })
    }

    /// List scatter, classify by op, fetch bodies for Create/Update.
    ///
    /// Pure read; both `read_dir` and `compact` start from this.
    async fn collect_scatter_changes(&self) -> Result<ScatterChanges> {
        let scatters = self.staging.list_scatter_inodes().await?;
        let all_keys: Vec<String> = scatters.iter().map(|s| s.key.clone()).collect();

        let last_view = DirScatterInode::filter_last_view(scatters);

        let mut to_remove = Vec::new();
        let mut to_fetch = Vec::new();
        let mut tombstone_keys: HashSet<String> = HashSet::new();
        for s in last_view {
            match s.op {
                DirScatterInodeOp::Unknown => {
                    warn!("scatter inode of unknown op: {}", s.key);
                },
                DirScatterInodeOp::PreDelete => {
                    warn!("scatter inode of PreDelete (not yet implemented): {}", s.key);
                },
                DirScatterInodeOp::Delete => {
                    // The latest Delete per filename is a tombstone; remember
                    // its key so compact won't strip it from S3. Older Delete
                    // scatters for the same name aren't in last_view and will
                    // be cleaned up like ordinary stale scatter.
                    tombstone_keys.insert(s.key.clone());
                    to_remove.push(s);
                },
                DirScatterInodeOp::Create | DirScatterInodeOp::Update => to_fetch.push(s),
            }
        }

        let fetched = self.staging.collect_scatter_inodes(to_fetch).await?;
        let mut upserts: HashMap<String, DirFileEntryRaw> = HashMap::new();
        for (s, body) in fetched {
            let inode_raw = InodeRaw::from_u8_slice(&body);
            let entry = DirFileEntryRaw::from(
                &inode_raw,
                s.uuid.as_bytes(),
                <String as AsRef<OsStr>>::as_ref(&s.filename).as_encoded_bytes(),
            );
            upserts.insert(s.filename, entry);
        }

        let mut deletes: HashSet<String> = HashSet::new();
        for s in to_remove {
            deletes.insert(s.filename);
        }

        Ok(ScatterChanges { all_keys, upserts, deletes, tombstone_keys })
    }

    /// Walk the persisted bmap read-only, returning all valid entries.
    /// Skips dummy values inserted by hyperfile as bmap placeholders.
    async fn walk_bmap_snapshot(&self) -> Result<Vec<DirFileEntryRaw>> {
        let mut out: Vec<DirFileEntryRaw> = Vec::new();
        let opt_last_key = match self.bmap.last_key().await {
            Ok(k) => Some(k),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        if let Some(last_key) = opt_last_key {
            let mut n: BlockIndex = 0;
            loop {
                let key = match self.bmap.seek_key(&n).await {
                    Ok(k) => k,
                    Err(e) if e.kind() == ErrorKind::NotFound => break,
                    Err(e) => return Err(e),
                };
                if key > last_key {
                    break;
                }
                let val = self.bmap.lookup(&key).await?;
                if !val.is_dummy() {
                    out.push(val);
                }
                if key == BlockIndex::MAX {
                    break;
                }
                n = key + 1;
            }
        }
        Ok(out)
    }

    /// Probe from `home` for the slot holding `name`, returning its
    /// (slot, entry) or `None` if absent. Linear probing: a slot whose entry
    /// is a dummy or a colliding (different) name is skipped; the first
    /// absent slot (NotFound) terminates the chain.
    ///
    /// Probing does not wrap; with CRC64 spread uniformly over u64 and tiny
    /// clusters in practice, a chain reaching `u64::MAX` does not occur.
    async fn probe_lookup(&self, name: &[u8], home: BlockIndex) -> Result<Option<(BlockIndex, DirFileEntryRaw)>> {
        let mut k = home;
        loop {
            match self.bmap.lookup(&k).await {
                Ok(entry) => {
                    if !entry.is_dummy() && entry.name_eq(name) {
                        return Ok(Some((k, entry)));
                    }
                },
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            }
            k = match k.checked_add(1) {
                Some(next) => next,
                None => return Ok(None),
            };
        }
    }

    /// Insert or update `entry` for `name`: write at the existing slot if the
    /// name is already present, else at the first empty slot in the probe
    /// chain from the name's home slot.
    async fn probe_upsert(&mut self, name: &[u8], entry: DirFileEntryRaw) -> Result<()> {
        let home = hash_filename_bytes(name);
        let mut k = home;
        loop {
            match self.bmap.lookup(&k).await {
                Ok(existing) => {
                    if !existing.is_dummy() && existing.name_eq(name) {
                        break; // update in place at k
                    }
                },
                Err(e) if e.kind() == ErrorKind::NotFound => break, // empty slot at k
                Err(e) => return Err(e),
            }
            k = k.checked_add(1)
                .ok_or_else(|| Error::other("probe chain exhausted u64 key space"))?;
        }
        let _ = self.bmap.insert(k, entry).await?;
        Ok(())
    }

    /// Delete `name`, repairing the probe chain with backward-shift so no gap
    /// breaks the lookup of entries that probed past the removed slot.
    /// Returns false if `name` was not present.
    async fn probe_delete(&mut self, name: &[u8]) -> Result<bool> {
        let home = hash_filename_bytes(name);
        let slot = match self.probe_lookup(name, home).await? {
            Some((k, _)) => k,
            None => return Ok(false),
        };

        // Backward-shift: scan forward from the hole; move any following entry
        // whose home slot is at or before the hole into the hole.
        let mut hole = slot;
        let mut j = match slot.checked_add(1) { Some(v) => v, None => slot };
        while j != slot {
            let entry_j = match self.bmap.lookup(&j).await {
                Ok(e) => e,
                Err(e) if e.kind() == ErrorKind::NotFound => break, // cluster ended
                Err(e) => return Err(e),
            };
            // Dummies don't occur for directory bmaps in practice; treat one
            // as home == j so it never shifts.
            let home_j = if entry_j.is_dummy() { j } else { hash_filename_bytes(entry_j.name_bytes()) };
            if home_j <= hole {
                let _ = self.bmap.insert(hole, entry_j).await?;
                hole = j;
            }
            j = match j.checked_add(1) { Some(v) => v, None => break };
        }
        self.bmap.delete(&hole).await?;
        Ok(true)
    }

    /// Rename an entry within this directory.
    ///
    /// Same-directory rename is cheap under UUID identity: the child keeps its
    /// UUID and underlying storage; only the parent's name->entry mapping
    /// changes. Both bmap edits (remove old, insert new) are staged in memory
    /// and committed by a single inode flush, so the rename is atomic against
    /// concurrent writers via hyperfile's inode OCC.
    ///
    /// The destination must not already exist; replace-over-existing returns
    /// `AlreadyExists` and is left to the caller (it would otherwise orphan
    /// the displaced child's storage). `old_name == new_name` is a no-op.
    pub async fn rename_within(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if old_name == new_name {
            return Ok(());
        }
        let old_home = hash_filename(old_name);
        let entry = match self.probe_lookup(old_name.as_bytes(), old_home).await? {
            Some((_, e)) => e,
            None => return Err(Error::new(ErrorKind::NotFound,
                format!("rename source not found: {}", old_name))),
        };
        let new_home = hash_filename(new_name);
        if self.probe_lookup(new_name.as_bytes(), new_home).await?.is_some() {
            return Err(Error::new(ErrorKind::AlreadyExists,
                format!("rename target exists: {}", new_name)));
        }

        // Same inode + uuid, new name.
        let new_entry = DirFileEntryRaw::from(&entry.inode, &entry.uuid, new_name.as_bytes());
        self.probe_delete(old_name.as_bytes()).await?;
        self.probe_upsert(new_name.as_bytes(), new_entry).await?;

        // One flush commits both bmap edits atomically.
        self.flush().await?;
        Ok(())
    }
}

impl<'a, T, L> HyperTrait<T, L, NullNodeCache, DirFileEntryRaw> for HyperDirFile<'a, T, L>
    where
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone,
{
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr {
        BlockPtrFormat::encode(segid, offset, seq, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
        BlockPtrFormat::decode(blk_ptr, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode_display(&self, blk_ptr: &BlockPtr) -> String {
        if BlockPtrFormat::is_dummy_value(blk_ptr) {
            "[Dummy]".to_string()
        } else if BlockPtrFormat::is_invalid_value(blk_ptr) {
            "[Invalid]".to_string()
        } else if BlockPtrFormat::is_zero_block(blk_ptr) {
            "[Zero Block]".to_string()
        } else if BlockPtrFormat::is_on_staging(blk_ptr) {
            let (id, off) = self.blk_ptr_decode(blk_ptr);
            let group_id = BlockPtrFormat::decode_micro_group_id(blk_ptr);
            format!("[Staging: id {} - offset {} - group {}]", id, off, group_id)
        } else {
            format!("[Unknown: 0x{:x}]", blk_ptr)
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        // do nothing
    }

    fn set_data_blocks_cache_unlimited(&mut self) {
        // do nothing
    }

    fn restore_data_blocks_cache_limit(&mut self) {
        // do nothing
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        DirtyDataBlocks { inner: Some(BTreeMap::new()), owned: None }
    }

    fn clear_data_blocks_dirty(&mut self) {
    }

    async fn lock(&self) -> OwnedSemaphorePermit {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        permit
    }

    fn unlock(&self, permit: OwnedSemaphorePermit) {
        drop(permit);
    }

    async fn flush_lock(&self) -> OwnedMutexGuard<()> {
        self.flush_lock.clone().lock_owned().await
    }

    fn flush_unlock(&self, lock: OwnedMutexGuard<()>) {
        drop(lock);
    }

    fn bmap_as_slice(&self) -> &[u8] {
        self.bmap.as_slice()
    }

    fn bmap_get_block_loader(&self) -> L {
        self.bmap.get_block_loader()
    }

    fn bmap_get_node_cache(&self) -> NullNodeCache {
        NullNodeCache
    }

    fn bmap_dirty(&self) -> bool {
        self.bmap.dirty()
    }

    fn bmap_lookup_dirty(&self) -> Vec<BtreeNodeDirty<'_, BlockIndex, DirFileEntryRaw, BlockPtr>> {
        self.bmap.lookup_dirty()
    }

    async fn bmap_assign_meta_node(&self, blk_ptr: BlockPtr, node: BtreeNodeDirty<'_, BlockIndex, DirFileEntryRaw, BlockPtr>) -> Result<()> {
        self.bmap.assign_meta_node(blk_ptr, node).await
    }

    async fn bmap_assign_data_node(&self, blk_idx: &BlockIndex, blk_ptr: BlockPtr) -> Result<()> {
        self.bmap.assign_data_node(blk_idx, blk_ptr).await
    }

    fn bmap_clear_dirty(&mut self) {
        self.bmap.clear_dirty()
    }

    fn bmap_update(&mut self, bmap: BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>) {
        self.bmap = unsafe {
            std::mem::transmute::<BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>, BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>>(bmap)
        };
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L, NullNodeCache>, blk_idx: &BlockIndex) -> Result<Option<DirFileEntryRaw>> {
        bmap.insert(*blk_idx, DirFileEntryRaw::dummy_value()).await
    }

    fn bmap_set_cache_unlimited(&self) -> usize {
        let limit = self.bmap.get_cache_limit();
        self.bmap.set_cache_limit(DEFAULT_CACHE_UNLIMITED);
        limit
    }

    fn bmap_set_cache_limit(&self, limit: usize) {
        self.bmap.set_cache_limit(limit);
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }

    #[allow(mutable_transmutes)]
    fn inode_mut(&self) -> &mut Inode {
        unsafe {
            std::mem::transmute::<&Inode, &mut Inode>(&self.inode)
        }
    }

    fn staging(&self) -> &T {
        &self.staging
    }

    fn config(&self) -> &HyperFileConfig {
        &self.config
    }

    fn set_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    fn flush_timing(&self) -> &FlushTiming {
        &self.flush_timing
    }

    #[cfg(feature = "wal")]
    async fn wal_set_mem_segment(&self, _mem_segid: SegmentId, _mem_segdata: Weak<Pin<Box<Vec<u8>>>>) {
        todo!();
    }

    #[cfg(feature = "wal")]
    async fn wal_clear_mem_segment(&self, _mem_segid: SegmentId) {
        todo!();
    }

    #[cfg(feature = "wal")]
    fn wal_spawn_delete_segment(&self, _segid: SegmentId) {
        todo!();
    }
}
