use std::io::{Result, Error, ErrorKind};
use std::time::SystemTime;
use bytes::Bytes;
use log::{warn, error};
use ulid::Ulid;
use uuid::Uuid;
use aws_sdk_s3::primitives::SdkBody;
use hyperfile::staging::config::StagingConfig;
use hyperfile::staging::s3::S3Staging;
use super::{DirStaging, DirScatterInode, DirScatterInodeOp, CompactLeaseGuard};
use super::{DEFAULT_DIR_INODE_SCATTER_FOLDER, DEFAULT_COMPACT_LEASE_FILE};
use super::{DEFAULT_DIR_INODE_SUFFIX, DEFAULT_DIR_TOMBSTONE_SUFFIX};
use super::{format_lease_body, parse_lease_body, unix_now_ms};

/// List every scatter object under `prefix` (recursive — no delimiter — so it
/// covers the per-name `{base64}/` subfolders) and decode them.
async fn list_scatter_with_prefix(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str)
    -> std::io::Result<Vec<DirScatterInode>>
{
    let mut stream = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();
    let mut scatters = Vec::new();
    while let Some(page) = stream.next().await {
        if let Ok(list_res) = page {
            if let Some(objects) = list_res.contents {
                for obj in objects.iter() {
                    if let Some(key) = obj.key() {
                        if key.ends_with(DEFAULT_DIR_INODE_SUFFIX)
                            || key.ends_with(DEFAULT_DIR_TOMBSTONE_SUFFIX)
                        {
                            let st = SystemTime::try_from(
                                obj.last_modified.expect("unable to get last_modified from object")
                            ).expect("unable to convert DateTime to SystemTime");
                            scatters.push(DirScatterInode::path_decode(key, st));
                        }
                    }
                }
            }
        }
    }
    Ok(scatters)
}

impl DirStaging for S3Staging {
    async fn list_scatter_inodes(&self) -> Result<Vec<DirScatterInode>> {
        list_scatter_with_prefix(&self.client, &self.bucket, &self.scatter_inodes_path()).await
    }

    async fn list_scatter_inodes_for_name(&self, filename: &str) -> Result<Vec<DirScatterInode>> {
        // Scoped to one name's subfolder: `!/{base64(name)}/`.
        let prefix = format!("{}{}/", self.scatter_inodes_path(), DirScatterInode::encode_filename(filename));
        list_scatter_with_prefix(&self.client, &self.bucket, &prefix).await
    }

    async fn collect_scatter_inodes(&self, v_scatters: Vec<DirScatterInode>) -> Result<Vec<(DirScatterInode, Bytes)>> {
        let mut v_res = Vec::new();
        // TODO: get_object in spawn
        for scatter in v_scatters.into_iter() {
            let key = &scatter.key;
        	let res = self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await;
        	match res {
            	Ok(output) => {
                    let agg_bytes = output.body.collect().await?;
                    v_res.push((scatter, agg_bytes.into_bytes()));
            	},
            	Err(sdk_err) => {
            	    let err_str = format!("GetObject s3://{}/{} error: {}", self.bucket, key, sdk_err);
            	    error!("{}", err_str);
            	    return Err(Error::other(err_str));
            	},
        	}
        }
        Ok(v_res)
    }

    async fn remove_scatter_inodes(&self, delete_keys: Vec<String>) -> Result<()> {
        let mut err = false;
        for keys in delete_keys.chunks(1000) {
            let obj_ids = keys.iter().map(|k| {
                    // FIXME: move the value, avoid ref
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k.to_string())
                        .build()
                        .unwrap()
                }).collect::<Vec<aws_sdk_s3::types::ObjectIdentifier>>();
            let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(obj_ids))
                    .quiet(true)
                    .build()
                    .unwrap();
            match self.client.delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
            {
                Ok(_) => {},
                Err(sdk_err) => {
                    err = true;
                    // TODO: check delete objects result with signle delete error
                    error!("delete objects error: {}", sdk_err);
                }
            }
        }
        if err {
            return Err(Error::new(ErrorKind::Interrupted, "at least one of delete objects op failed"));
        }
        Ok(())
    }

    fn to_dir_staging_config(config: &StagingConfig) -> StagingConfig {
        let mut config = config.clone();
        assert!(!config.root_uri.ends_with('/'));

        config.inode_file_uri = format!("{}/inode", config.root_uri);
        config
    }

    async fn emit_scatter_event(&self, filename: &str, child_uuid: &Uuid, buf: &[u8], op: DirScatterInodeOp) -> Result<()> {
        // `self` is the parent directory's staging; its root_path is the
        // directory's own prefix and the scatter lands in its `!/` namespace.
        let key = DirScatterInode::path_encode(&self.root_path, filename, child_uuid, op.clone() as u8);
        let mut builder = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key);
        builder = match op {
            DirScatterInodeOp::Create | DirScatterInodeOp::Update | DirScatterInodeOp::Delete => {
                // Create/Update body = raw InodeRaw bytes.
                // Delete body       = TombstoneHeader || raw InodeRaw bytes
                //                     (see crate::TombstoneHeader docs).
                let body = SdkBody::from(buf);
                builder.body(body.into())
            },
            DirScatterInodeOp::PreDelete => {
                // 2-phase delete intent: empty body, op encoded in the key
                // is enough. Not yet exercised end-to-end.
                builder
            },
            DirScatterInodeOp::Unknown => {
                return Err(Error::new(ErrorKind::InvalidInput,
                    format!("emit_scatter_event called with Unknown op for s3://{}/{}",
                            self.bucket, key)));
            },
        };

        // If-None-Match: * makes this PUT exactly-once for its (ulid-named) key.
        // The scatter object is the durable commit point of a logical mutation:
        // once this PUT succeeds the change is committed in the parent
        // directory's view. The file's own inode object that hyperfile writes
        // after this hook returns is a best-effort replication that any
        // reader/compactor can redo idempotently.
        builder = builder.if_none_match('*');

        match builder.send().await {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                // 412 Precondition Failed / 409 Conflict: an object already
                // exists at this key. ULID uniqueness makes this essentially
                // impossible in normal operation; we treat it as already
                // committed and continue. If a future change reuses ULIDs across
                // retries (for true exactly-once on the writer side), this
                // branch should additionally verify body equivalence via GET
                // before accepting.
                if let Some(resp) = sdk_err.raw_response() {
                    let st = resp.status().as_u16();
                    if st == 412 || st == 409 {
                        warn!("scatter PUT s3://{}/{} returned {} (treating as already committed)",
                              self.bucket, key, st);
                        return Ok(());
                    }
                }
                let err_str = format!("PutObject s3://{}/{} error: {}", self.bucket, key, sdk_err);
                error!("{}", err_str);
                Err(Error::other(err_str))
            },
        }
    }

    fn scatter_inodes_path(&self) -> String {
        format!("{}/{DEFAULT_DIR_INODE_SCATTER_FOLDER}/", self.root_path)
    }

    fn compact_lease_path(&self) -> String {
        format!("{}/{DEFAULT_COMPACT_LEASE_FILE}", self.root_path)
    }

    async fn acquire_compact_lease(&self, ttl_ms: u64) -> Result<CompactLeaseGuard> {
        let lease_key = self.compact_lease_path();
        let holder_id = Ulid::new().to_string();
        let now_ms = unix_now_ms();
        let expires_at_unix_ms = now_ms.saturating_add(ttl_ms as i64);
        let body_str = format_lease_body(&holder_id, expires_at_unix_ms);
        let body = SdkBody::from(body_str.as_bytes());

        let res = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&lease_key)
            .body(body.into())
            .if_none_match('*')
            .send()
            .await;

        match res {
            Ok(output) => {
                let etag = output.e_tag.unwrap_or_default().replace('"', "");
                Ok(CompactLeaseGuard { holder_id, etag, lease_key, expires_at_unix_ms })
            },
            Err(sdk_err) => {
                // 412/409: a lease object already exists at this key. Decide
                // whether it's still fresh or has expired (and is therefore
                // takeable).
                let is_conflict = sdk_err.raw_response()
                    .map(|r| matches!(r.status().as_u16(), 412 | 409))
                    .unwrap_or(false);
                if is_conflict {
                    return try_take_over_compact_lease(
                        self, &lease_key, &holder_id, ttl_ms, now_ms).await;
                }
                let err_str = format!(
                    "PutObject (acquire_compact_lease) s3://{}/{} error: {}",
                    self.bucket, lease_key, sdk_err);
                error!("{}", err_str);
                Err(Error::other(err_str))
            },
        }
    }

    async fn release_compact_lease(&self, guard: CompactLeaseGuard) -> Result<()> {
        let res = self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&guard.lease_key)
            .if_match(&guard.etag)
            .send()
            .await;

        match res {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                if let Some(resp) = sdk_err.raw_response() {
                    if resp.status().as_u16() == 412 {
                        // Our etag no longer matches: the lease was taken
                        // over after expiration. Not an error from our side.
                        warn!("compact lease s3://{}/{} was taken over before release (holder={})",
                              self.bucket, guard.lease_key, guard.holder_id);
                        return Ok(());
                    }
                }
                // Any other error: log but don't fail. The lease will expire
                // naturally on TTL; failing here would mask the actual
                // compact result.
                warn!("DeleteObject (release_compact_lease) s3://{}/{} error: {} (lease will expire on TTL)",
                      self.bucket, guard.lease_key, sdk_err);
                Ok(())
            },
        }
    }
}

/// Helper for [`<S3Staging as DirStaging>::acquire_compact_lease`]: an object
/// already exists at the lease key; check its expiration and either take it
/// over (via `If-Match`) or report busy.
///
/// Lives as a free function rather than an inherent method on `S3Staging`
/// because that type is defined in the `hyperfile` crate; Rust forbids
/// inherent impls on out-of-crate types.
async fn try_take_over_compact_lease(
    staging: &S3Staging,
    lease_key: &str,
    holder_id: &str,
    ttl_ms: u64,
    now_ms: i64,
) -> Result<CompactLeaseGuard> {
    // Read the existing lease to see when it expires.
    let res = staging.client
        .get_object()
        .bucket(&staging.bucket)
        .key(lease_key)
        .send()
        .await;
    let (existing_etag, existing_body) = match res {
        Ok(out) => {
            let etag = out.e_tag.clone().unwrap_or_default().replace('"', "");
            let body = out.body.collect().await
                .map_err(|e| Error::other(
                    format!("read compact lease body s3://{}/{}: {}", staging.bucket, lease_key, e)))?
                .into_bytes();
            (etag, body)
        },
        Err(sdk_err) => {
            // The lease object disappeared between our PUT-412 and GET.
            // This is a race; report busy and let the caller retry on
            // the next compaction cycle.
            let err_str = format!(
                "GetObject (try_take_over_compact_lease) s3://{}/{} error: {}",
                staging.bucket, lease_key, sdk_err);
            warn!("{}", err_str);
            return Err(Error::new(ErrorKind::ResourceBusy, err_str));
        },
    };

    let (existing_holder, existing_expires) = parse_lease_body(&existing_body)?;

    if existing_expires > now_ms {
        // Fresh lease held by someone else; back off.
        return Err(Error::new(ErrorKind::ResourceBusy,
            format!("compact lease s3://{}/{} held by {} until {}ms",
                    staging.bucket, lease_key, existing_holder, existing_expires)));
    }

    // Lease has expired: try to take it over with If-Match on the etag.
    let new_expires = now_ms.saturating_add(ttl_ms as i64);
    let body_str = format_lease_body(holder_id, new_expires);
    let body = SdkBody::from(body_str.as_bytes());

    let res = staging.client
        .put_object()
        .bucket(&staging.bucket)
        .key(lease_key)
        .body(body.into())
        .if_match(&existing_etag)
        .send()
        .await;

    match res {
        Ok(output) => {
            let new_etag = output.e_tag.unwrap_or_default().replace('"', "");
            warn!("compact lease s3://{}/{} taken over from expired holder {} (new holder {})",
                  staging.bucket, lease_key, existing_holder, holder_id);
            Ok(CompactLeaseGuard {
                holder_id: holder_id.to_string(),
                etag: new_etag,
                lease_key: lease_key.to_string(),
                expires_at_unix_ms: new_expires,
            })
        },
        Err(sdk_err) => {
            let is_conflict = sdk_err.raw_response()
                .map(|r| matches!(r.status().as_u16(), 412 | 409))
                .unwrap_or(false);
            if is_conflict {
                return Err(Error::new(ErrorKind::ResourceBusy,
                    format!("lost race to take over expired compact lease s3://{}/{}",
                            staging.bucket, lease_key)));
            }
            let err_str = format!(
                "PutObject (take_over_compact_lease) s3://{}/{} error: {}",
                staging.bucket, lease_key, sdk_err);
            error!("{}", err_str);
            Err(Error::other(err_str))
        },
    }
}
