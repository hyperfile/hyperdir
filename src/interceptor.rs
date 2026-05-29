//! Scatter-first commit interceptor.
//!
//! `ScatterFirstInterceptor` plugs into hyperfile's `StagingIntercept` so that
//! every `flush_inode` call on a child file writes the parent directory's
//! scatter object **before** the file's own inode object is persisted. The
//! scatter is therefore the durable commit point of a single logical mutation:
//!
//! ```text
//! 1. before_flush_inode  -> PUT  s3://.../<parent>/!/inode_<ulid>_<name>_<op>
//!                                with `If-None-Match: *`     (commit point)
//! 2. hyperfile core      -> PUT  s3://.../<child>/inode      (replication)
//! 3. after_flush_inode   -> no-op
//! ```
//!
//! If step 1 fails the whole `flush_inode` fails and step 2 never runs, so the
//! parent directory never observes a partial commit. If step 1 succeeds but
//! step 2 (or anything later) fails, the scatter alone is enough for a future
//! reader/compactor to reconstruct the child inode by replaying the scatter
//! body. Step 2 is therefore best-effort, idempotent replication.

use std::io::Result;
use std::pin::Pin;
use std::future::Future;
use log::debug;
use hyperfile::staging::{StagingIntercept, s3::S3Staging};
use hyperfile::inode::FlushInodeFlag;
use crate::{DirStaging, DirScatterInodeOp};

/// Stateless, cloneable interceptor that turns each `flush_inode` into a
/// scatter-first commit on the parent directory's staging.
///
/// Currently holds no state. A future revision may carry a per-handle
/// transaction id (ulid) so retries triggered by hyperfile's
/// `RetryLastWriterWins` policy reuse the same scatter key, giving
/// writer-side exactly-once semantics on top of the conditional PUT. Until
/// then, retries simply emit a fresh scatter; consolidation deduplicates.
#[derive(Default, Clone, Debug)]
pub struct ScatterFirstInterceptor;

impl ScatterFirstInterceptor {
    pub fn new() -> Self { Self }
}

impl StagingIntercept<S3Staging> for ScatterFirstInterceptor {
    fn before_flush_inode(
        &self,
        staging: &S3Staging,
        payload: &[u8],
        flag: FlushInodeFlag,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // The hook receives the *child* file's staging. The scatter object
        // belongs in the *parent* directory's `!/` prefix; `to_dir_staging`
        // derives the parent staging from the child. Both `dir_staging` and
        // `payload` are owned copies so the returned future has no lifetime
        // tie back to the caller-owned `staging`/`payload` references.
        let dir_staging = <S3Staging as DirStaging>::to_dir_staging(staging);
        let payload = payload.to_vec();
        Box::pin(async move {
            let op = match flag {
                FlushInodeFlag::Create => DirScatterInodeOp::Create,
                FlushInodeFlag::Update => DirScatterInodeOp::Update,
                // Delete scatters are emitted by fs_unlink with a tombstone
                // body (TombstoneHeader || InodeRaw), not via this hook. If
                // hyperfile ever wires flush_inode(Delete) to a real call
                // path, the bytes hyperfile would pass here are just the
                // inode raw, which the GC and undelete paths cannot parse as
                // a tombstone. Skip rather than emit a malformed body.
                FlushInodeFlag::Delete => {
                    debug!("ScatterFirstInterceptor: skipping FlushInodeFlag::Delete (use fs_unlink instead)");
                    return Ok(());
                },
                // Internal hyperfile flags that aren't real commits in the
                // parent-directory sense; nothing to scatter.
                FlushInodeFlag::Ignore | FlushInodeFlag::Unkown => {
                    debug!("ScatterFirstInterceptor: skipping flag {:?}", flag);
                    return Ok(());
                },
            };
            dir_staging.emit_scatter_event(&payload, op).await
        })
    }

    fn after_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // Nothing to do: the file inode write that just completed is the
        // best-effort replication of the scatter we already committed in
        // before_flush_inode. If the inode PUT failed, hyperfile's own retry
        // loop already handled it; if it succeeded, the parent's scatter is
        // still the source of truth.
        Box::pin(async { Ok(()) })
    }

    fn after_remove_inode(
        &self,
        _staging: &S3Staging,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // unlink path: the child file's whole prefix is being torn down. The
        // tombstone scatter is emitted via the `Delete` flag in
        // `before_flush_inode` already; nothing further to do here.
        Box::pin(async { Ok(()) })
    }
}
