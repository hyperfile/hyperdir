use std::io::{Result, ErrorKind};
use aws_sdk_s3::Client;
use hyperfile::staging::{Staging, StagingIntercept, config::StagingConfig, s3::S3Staging};
use hyperfile::meta_loader::s3_batch::S3BlockLoader;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::config::HyperFileConfig;
use crate::DirStaging;
use crate::file::HyperDirFile;

pub struct HyperDir<'a> {
    pub(crate) inner: HyperDirFile<'a, S3Staging, S3BlockLoader>
}

impl<'a> HyperDir<'a> {
    pub(crate) async fn do_open_or_create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, create: bool) -> Result<Self>
    {
        match Self::open(client.clone(), file_config.clone(), flags.clone()).await {
            Ok(hyper) => {
                return Ok(hyper);
            },
            Err(e) => {
                if create && e.kind() == ErrorKind::NotFound {
                    return Self::create(client, file_config, flags).await;
                }
                return Err(e);
            }
        }
    }

    pub async fn open(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let dir_staging_config = S3Staging::to_dir_staging_config(&file_config.staging);
        let staging = S3Staging::from(&client, dir_staging_config, file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperDirFile::<S3Staging, S3BlockLoader>::open(staging, loader, file_config, flags).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let dir_staging_config = S3Staging::to_dir_staging_config(&file_config.staging);
        let staging = S3Staging::create(&client, dir_staging_config, file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperDirFile::<S3Staging, S3BlockLoader>::new(staging, loader, file_config, flags).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn stat_fast(client: Client, file_config: HyperFileConfig) -> Result<libc::stat>
    {
        let dir_staging_config = S3Staging::to_dir_staging_config(&file_config.staging);
        let staging = S3Staging::from(&client, dir_staging_config, file_config.runtime.clone()).await?;
        HyperDirFile::<S3Staging, S3BlockLoader>::stat_fast(staging).await
    }
}

/// expose helper fn
impl<'a> HyperDir<'a> {
    pub fn staging_config(&self) -> &StagingConfig {
        self.inner.staging_config()
    }

    pub fn with_staging_interceptor(&mut self, i: impl StagingIntercept<S3Staging> + 'static) {
        self.inner.staging_interceptor(i)
    }
}
