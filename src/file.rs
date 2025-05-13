use std::io::{Result, Error, ErrorKind};
use std::time::{Instant, Duration};
use std::sync::Arc;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use btree_ondisk::{BlockLoader, bmap::BMap, NodeValue};
use hyperfile::file::{HyperTrait, DirtyDataBlocks};
use hyperfile::{BlockIndex, BlockPtr, SegmentId, SegmentOffset, BMapUserData};
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::inode::{Inode, FlushInodeFlag};
use hyperfile::config::{HyperFileConfig, HyperFileMetaConfig};
use hyperfile::staging::Staging;
use hyperfile::segment::SegmentReadWrite;
use hyperfile::meta_loader::s3::S3BlockLoader;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::ondisk::{BMapRawType, InodeRaw};
use super::ondisk::{DirFileEntryRaw, DEFAULT_NAME_LEN};

const DIR_FILE_ENTRY_RAW_SIZE: usize = std::mem::size_of::<DirFileEntryRaw>();

#[derive(Debug)]
pub struct DirFileEntry {
    pub inode: Inode,
    pub name: String,
}

impl DirFileEntry {
    fn from_raw(raw: &DirFileEntryRaw) -> Self {
        let name = match std::str::from_utf8(&raw.name) {
            Ok(s) => { s.to_owned() },
            Err(_) => { String::new() }, // invalid filename
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
        T: Staging<T, L> + SegmentReadWrite,
        L: BlockLoader<BlockPtr> + Clone,
{
    pub async fn new(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, DirFileEntryRaw, BlockPtr, L>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader);

        let inode = Inode::default_dir();
        let bmap_ud = BMapUserData::new(BlockPtrFormat::Nop);
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
                if e.kind() == ErrorKind::NotFound {
                    return Self::new(staging, meta_block_loader, config, flags).await;
                }
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
            sema: Arc::new(Semaphore::new(1)),
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
        let blksize = self.config.meta.data_block_size;
        self.inode.to_stat(dev, rdev, blksize)
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
}

impl<'a, T, L> HyperTrait<'a, T, L, DirFileEntryRaw> for HyperDirFile<'a, T, L>
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
        String::new()
    }

    fn clear_data_blocks_cache(&mut self) {
        // do nothing
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        DirtyDataBlocks { inner: None, owned: None }
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

    fn bmap(&self) -> &BMap<'a, BlockIndex, DirFileEntryRaw, BlockPtr, L> {
        &self.bmap
    }

    fn bmap_mut(&mut self) -> &mut BMap<'a, BlockIndex, DirFileEntryRaw, BlockPtr, L> {
        &mut self.bmap
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'a, BlockIndex, DirFileEntryRaw, BlockPtr, L>, blk_idx: &BlockIndex) -> Result<Option<DirFileEntryRaw>> {
        Ok(None)
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }

    fn inode_mut(&mut self) -> &mut Inode {
        &mut self.inode
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
