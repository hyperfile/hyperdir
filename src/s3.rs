use std::io::{Result, Error, ErrorKind};
use std::time::SystemTime;
use bytes::Bytes;
use log::error;
use aws_sdk_s3::primitives::SdkBody;
use hyperfile::staging::config::StagingConfig;
use hyperfile::staging::{Staging, s3::S3Staging};
use super::{DirStaging, DirScatterInode, DirScatterInodeOp};
use super::{DEFAULT_DIR_SUFFIX, DEFAULT_DIR_FILE_FOLDER, DEFAUTL_DIR_INODE_MARKER, DEFAULT_DIR_FILE_SUFFIX};

impl DirStaging for S3Staging {
    async fn list_scatter_inodes(&self) -> Result<Vec<DirScatterInode>> {
        let top_path = self.root_path.strip_suffix(DEFAULT_DIR_FILE_FOLDER).expect("invalid staging root path");
        let mut stream = self.client.list_objects_v2()
            .bucket(&self.bucket)
            .prefix(top_path)
            .delimiter("/")
            .into_paginator()
            .send();

        let mut scatters = Vec::new();
        while let Some(page) = stream.next().await {
            if let Ok(list_res) = page {
                if let Some(objects) = list_res.contents {
                    for obj in objects.iter() {
                        if let Some(key) = obj.key() {
                            if key.contains(DEFAUTL_DIR_INODE_MARKER) {
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
            	    return Err(Error::new(ErrorKind::Other, err_str));
            	},
        	}
        }
        Ok(v_res)
    }

    async fn remove_scatter_inodes(&self, delete_keys: Vec<String>) -> Result<()> {
        let mut err = false;
        for keys in delete_keys.chunks(1000).into_iter() {
            let obj_ids = keys.into_iter().map(|k| {
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

    fn to_dir_staging(staging: &S3Staging) -> S3Staging {
        let mut staging = staging.clone();
        let path = std::path::Path::new(&staging.root_path);
        let top_dir = path.parent()
            .expect("invalid top dir, dir staging must have parent")
            .to_str()
            .expect("unable to decode parent to utf8 string")
            .to_owned();

        staging.root_path = format!("{}{}/{}", top_dir, DEFAULT_DIR_SUFFIX, DEFAULT_DIR_FILE_FOLDER);
        staging.root_path_slash = format!("{}/", staging.root_path);
        staging.inode_file = format!("{}/inode", staging.root_path);
        staging
    }

    fn to_dir_staging_config(config: &StagingConfig) -> StagingConfig {
        let mut config = config.clone();
        assert!(!config.root_uri.ends_with('/'));

        config.root_uri = format!("{}{}/{}", config.root_uri, DEFAULT_DIR_SUFFIX, DEFAULT_DIR_FILE_FOLDER);
        config.inode_file_uri = format!("{}/inode", config.root_uri);
        config
    }

    async fn emit_scatter_event(&self, buf: &[u8], op: DirScatterInodeOp) -> Result<()> {
        let (dir, filename) = self.dir_filename();
        // NOTE: dir staging return filename carry with dir file suffix
        // trim end suffix if we have
        let filename = if filename.ends_with(DEFAULT_DIR_FILE_SUFFIX) {
            filename.trim_end_matches(DEFAULT_DIR_FILE_SUFFIX)
        } else {
            filename
        };
        let key = DirScatterInode::path_encode(dir, filename, op.clone() as u8);
        let builder = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key);
        let builder = match op {
            DirScatterInodeOp::Create | DirScatterInodeOp::Update => {
                let body = SdkBody::from(buf);
                builder.body(body.into())
            },
            DirScatterInodeOp::Delete => {
                builder
            },
            _ => {
                panic!("unkown DirScatterInodeOp {:?}", op);
            },
        };
        match builder.send().await {
            Ok(_) => {},
            Err(sdk_err) => {
                let err_str = format!("PutObject s3://{}/{} error: {}", self.bucket, key, sdk_err);
                error!("{}", err_str);
                return Err(Error::new(ErrorKind::Other, err_str));
            },
        }
        Ok(())
    }
}
