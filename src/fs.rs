use std::io::Result;
use log::debug;
use aws_sdk_s3::Client;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging};
use hyperfile::file::HyperTrait;
use crate::hyper::HyperDir;
use crate::file::{EntryNameHash, DirFileEntry};

impl<'a> HyperDir<'a>
{
    pub async fn fs_open(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        debug!("fs_open - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_opt(client: &Client, uri: &str, flags: FileFlags, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_open_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_or_create_with_default_opt(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        debug!("fs_open_or_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::do_open_or_create(client.clone(), file_config, f, true).await;
    }

    pub async fn fs_unlink(client: &Client, uri: &str) -> Result<()>
    {
        debug!("fs_unlink - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let staging = S3Staging::from(client, staging_config, HyperFileRuntimeConfig::default()).await?;
        staging.unlink().await
    }

    pub async fn fs_release(&mut self) -> Result<u64>
    {
        debug!("fs_release - ");
        self.inner.release().await
    }

    pub async fn fs_flush(&mut self) -> Result<u64>
    {
        debug!("fs_flush - ");
        self.inner.flush().await
    }

    pub fn fs_getattr(&self) -> Result<libc::stat>
    {
        debug!("fs_getattr - ");
        Ok(self.inner.stat())
    }

    pub async fn fs_getattr_fast(client: &Client, uri: &str) -> Result<libc::stat>
    {
        debug!("fs_getattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .build();
        return Self::stat_fast(client.clone(), file_config).await;
    }

    pub async fn fs_read_entry(&self, hash: &EntryNameHash) -> Result<DirFileEntry>
    {
        debug!("fs_read_entry - ");
        self.inner.read_entry(hash).await
    }

    pub async fn fs_read_dir(&mut self) -> Result<Vec<DirFileEntry>>
    {
        debug!("fs_readdir - ");
        self.inner.read_dir().await
    }
}
