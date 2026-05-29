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
    /// Hash -> entry, for filenames whose latest scatter is Create or Update.
    upserts: HashMap<EntryNameHash, DirFileEntryRaw>,
    /// Hashes whose latest scatter is Delete.
    deletes: HashSet<EntryNameHash>,
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

    pub async fn read_entry(&self, hash: &EntryNameHash) -> Result<DirFileEntry> {
        let entry_raw = self.bmap.lookup(hash).await?;
        Ok(DirFileEntry::from_raw(&entry_raw))
    }

    /// Pure-read directory enumeration.
    ///
    /// Lists outstanding scatter objects, walks the persisted bmap, and merges
    /// the two views in memory. Does not modify any S3 state, so it is safe to
    /// call concurrently from multiple clients.
    ///
    /// The returned snapshot reflects "the latest scatter applied on top of
    /// the bmap as of the LIST/GET round-trips". Two consecutive calls may
    /// differ if other writers commit between them. Outstanding scatter
    /// objects are *not* deleted here; that is the job of [`compact`].
    pub async fn read_dir(&self) -> Result<Vec<DirFileEntry>> {
        let changes = self.collect_scatter_changes().await?;

        // start from the persisted bmap snapshot
        let mut view = self.walk_bmap_snapshot().await?;

        // overlay scatter changes
        for (hash, entry) in changes.upserts {
            view.insert(hash, entry);
        }
        for hash in &changes.deletes {
            view.remove(hash);
        }

        Ok(view
            .into_values()
            .filter(|raw| !raw.is_dummy())
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
        for (hash, entry) in &changes.upserts {
            let _ = self.bmap.insert(*hash, *entry).await?;
            entries_added += 1;
        }

        let mut entries_removed = 0;
        for hash in &changes.deletes {
            match self.bmap.delete(hash).await {
                Ok(_) => entries_removed += 1,
                // Tombstone for a name that was never persisted (e.g., a file
                // created and deleted before any compact ran), or already
                // consolidated by a previous compact run that only kept the
                // tombstone object. Idempotent.
                Err(e) if e.kind() == ErrorKind::NotFound => {},
                Err(e) => return Err(e),
            }
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
        let mut upserts: HashMap<EntryNameHash, DirFileEntryRaw> = HashMap::new();
        for (s, body) in fetched {
            let hash = hash_filename(&s.filename);
            let inode_raw = InodeRaw::from_u8_slice(&body);
            let entry = DirFileEntryRaw::from(
                &inode_raw,
                s.uuid.as_bytes(),
                <String as AsRef<OsStr>>::as_ref(&s.filename).as_encoded_bytes(),
            );
            upserts.insert(hash, entry);
        }

        let mut deletes: HashSet<EntryNameHash> = HashSet::new();
        for s in to_remove {
            deletes.insert(hash_filename(&s.filename));
        }

        Ok(ScatterChanges { all_keys, upserts, deletes, tombstone_keys })
    }

    /// Walk the persisted bmap read-only, returning the set of valid entries.
    /// Skips dummy values inserted by hyperfile as bmap placeholders.
    async fn walk_bmap_snapshot(&self) -> Result<HashMap<EntryNameHash, DirFileEntryRaw>> {
        let mut map: HashMap<EntryNameHash, DirFileEntryRaw> = HashMap::new();
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
                    map.insert(key, val);
                }
                if key == BlockIndex::MAX {
                    break;
                }
                n = key + 1;
            }
        }
        Ok(map)
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
