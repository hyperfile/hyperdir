use std::io::{Result, Error, ErrorKind};
use std::time::{Instant, Duration};
use std::sync::Arc;
use std::ffi::OsStr;
use std::ffi::CStr;
use std::collections::{HashMap, BTreeMap};
use log::warn;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use btree_ondisk::{BlockLoader, bmap::BMap, NodeValue};
use btree_ondisk::btree::BtreeNodeDirty;
use hyperfile::file::{HyperTrait, DirtyDataBlocks};
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
            name,
        }
    }
}

pub(crate) type EntryNameHash = u64;

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

pub struct HyperDirFile<'a, T, L: BlockLoader<BlockPtr>> {
    bmap: BMap<'a, BlockIndex, DirFileEntryRaw, BlockPtr, L>,
    staging: T,
    bmap_ud: BMapUserData,
    config: HyperFileConfig,
	flags: HyperFileFlags,
    last_flush: Instant,
    sema: Arc<Semaphore>,
    inode: Inode,
}

impl<'a, T, L> HyperDirFile<'a, T, L>
    where
        T: Staging<T, L> + SegmentReadWrite + DirStaging,
        L: BlockLoader<BlockPtr> + Clone,
{
    pub async fn new(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, DirFileEntryRaw, BlockPtr, L>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader);

        let inode = Inode::default_dir()
            .with_mode(&mode)
            .with_meta_config(&meta_config);
        let bmap_ud = BMapUserData::new(BlockPtrFormat::Flat);
        bmap.set_userdata(bmap_ud.as_u32());

		let mut file = Self {
            bmap: bmap,
            staging: staging,
            bmap_ud: bmap_ud,
            config: config,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(1)),
            inode: inode,
        };
        // flush inode for hyper file new created
        let _ = file.flush_inode(FlushInodeFlag::Create).await?;
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
        let inode_state;
        let res_inode = if cno == 0 {
            staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await
        } else {
            staging.load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), cno as SegmentId).await
        };
        match res_inode {
            Ok(od_state) => {
                /* if we load inode without error, we use inode as truth of metadata */
                inode_state = od_state;
            },
            Err(e) => {
                return Err(e);
            },
        }
        // get back meta config from inode raw
        let meta_config = HyperFileMetaConfig::from_u32(raw_inode.i_meta_config);
        let b = raw_inode.i_bmap;
        let bmap = BMap::<BlockIndex, DirFileEntryRaw, BlockPtr, L>::read(&b, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            1
        };

		// overwrite the default meta config with the one we get from inode
		config.meta = meta_config;

		let mut file = Self {
            bmap: bmap,
            staging: staging,
            bmap_ud: bmap_ud,
            config: config,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            inode: Inode::from_raw(&raw_inode, inode_state),
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
        staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let inode = Inode::from_raw(&raw_inode, None);
        Ok(inode.to_stat(0, 0))
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

    pub async fn read_entry(&self, hash: &EntryNameHash) -> Result<DirFileEntry> {
        let entry_raw = self.bmap.lookup(hash).await?;
        Ok(DirFileEntry::from_raw(&entry_raw))
    }

    pub async fn read_dir(&mut self) -> Result<Vec<DirFileEntry>> {
        // #1. list all inodes
        let v_scattered_inodes = self.staging.list_scatter_inodes().await?;
        // build all scatters key list
        let v_all_scatters_key: Vec<String> = v_scattered_inodes.iter().map(|s| s.key.to_owned()).collect();

        // #2. dedup inodes to get latest view and fetch inode raw
        let v_last_view = DirScatterInode::filter_last_view(v_scattered_inodes);

        // group by fetch and remove op
        let mut v_removed = Vec::new();
        let mut v_fetch = Vec::new();
        for scatter in v_last_view.into_iter() {
            match scatter.op {
                DirScatterInodeOp::Unkown => {
                    warn!("a scatter inode of unkown op: {}", scatter.key);
                },
                DirScatterInodeOp::PreDelete => {
                    warn!("a scatter inode of PreDelete, not yet implemented");
                },
                DirScatterInodeOp::Delete => {
                    v_removed.push(scatter);
                },
                _ => {
                    v_fetch.push(scatter);
                },
            }
        }
        let v_fetched_scattered_inodes = self.staging.collect_scatter_inodes(v_fetch).await?;

        // #3. transform
        let mut v_transformed_scattered_inodes: HashMap<EntryNameHash, DirFileEntryRaw> = HashMap::new();
        for (scatter, bytes) in v_fetched_scattered_inodes.into_iter() {
            // calc crc64
            let mut c = crc64fast::Digest::new();
            c.write(scatter.filename.as_bytes());
            let hashed_name = c.sum64();
            // convert bytes to inode raw
            let inode_raw = InodeRaw::from_u8_slice(&bytes);
            // filename into OsStr
            let entry_raw = DirFileEntryRaw::from(&inode_raw, <String as AsRef<OsStr>>::as_ref(&scatter.filename).as_encoded_bytes());
            v_transformed_scattered_inodes.insert(hashed_name, entry_raw);
        }

        // #4. apply changed and deleted into bmap
        // apply changed
        for (hashed_name, entry_raw) in v_transformed_scattered_inodes.iter() {
            // force update bmap
            let _ = self.bmap.insert(*hashed_name, *entry_raw).await?;
        }
        // apply deleted
        for scatter in v_removed.iter() {
            // calc crc64
            let mut c = crc64fast::Digest::new();
            c.write(scatter.filename.as_bytes());
            let hashed_name = c.sum64();
            match self.bmap.delete(&hashed_name).await {
                Ok(_) => {},
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                    // if not found means file not exists in bmap,
                    // let's ignore and continue
                },
            }
        }

        // #5. read in all K/V from bmap to get latest view
        // temp hash map to hold latest view of dir elements need to be load from staging
        let mut map: HashMap<EntryNameHash, DirFileEntryRaw> = HashMap::new();
        // final view of dir elements
        let mut last_view_map: HashMap<EntryNameHash, DirFileEntryRaw> = HashMap::new();

        let opt_last_key = match self.bmap.last_key().await {
            Ok(k) => { Some(k) },
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e);
                }
                None
            },
        };
        // build temp map, bmap including dummy value
        if let Some(last_key) = opt_last_key {
            let mut n = 0;
            while n <= last_key {
                let key = self.bmap.seek_key(&n).await?;
                let val = self.bmap.lookup(&key.clone()).await?;
                map.insert(key.clone(), val);
                if key > n {
                    // n not found in bmap, reset n with key for next
                    n = key;
                } else {
                    // if n == key, incr n for next
                    n += 1;
                }
            }
        }

        // #6. build last view map
        for (hashed_name, entry_raw) in map.into_iter() {
            last_view_map.insert(hashed_name, entry_raw);
        }
        drop(v_transformed_scattered_inodes);

        // #7. update bmap and flush
        self.flush().await?;

        // #8. cleanup
        self.staging.remove_scatter_inodes(v_all_scatters_key).await?;

        // #9. build result
        let v: Vec<DirFileEntry> = last_view_map.into_iter()
                .map(|(_, raw)| DirFileEntry::from_raw(&raw))
                .collect();
        Ok(v)
    }
}

impl<'a, T, L> HyperTrait<T, L, DirFileEntryRaw> for HyperDirFile<'a, T, L>
    where
        T: Staging<T, L> + SegmentReadWrite,
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
            return format!("[Dummy]");
        } else if BlockPtrFormat::is_invalid_value(blk_ptr) {
            return format!("[Invalid]");
        } else if BlockPtrFormat::is_zero_block(blk_ptr) {
            return format!("[Zero Block]");
        } else if BlockPtrFormat::is_on_staging(blk_ptr) {
            let (id, off) = self.blk_ptr_decode(blk_ptr);
            let group_id = BlockPtrFormat::decode_micro_group_id(blk_ptr);
            return format!("[Staging: id {} - offset {} - group {}]", id, off, group_id);
        } else {
            return format!("[Unkown: 0x{:x}]", blk_ptr);
        }
    }

    fn clear_data_blocks_cache(&mut self) {
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

    fn bmap_as_slice(&self) -> &[u8] {
        self.bmap.as_slice()
    }

    fn bmap_get_block_loader(&self) -> L {
        self.bmap.get_block_loader()
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

    fn bmap_update(&mut self, bmap: BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L>) {
        *&mut self.bmap = unsafe {
            std::mem::transmute::<BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L>, BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L>>(bmap)
        };
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, DirFileEntryRaw, BlockPtr, L>, blk_idx: &BlockIndex) -> Result<Option<DirFileEntryRaw>> {
        bmap.insert(*blk_idx, DirFileEntryRaw::dummy_value()).await
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
}
