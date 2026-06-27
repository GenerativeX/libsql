//! `object_store`-backed storage for bottomless.
//!
//! Replaces the previous direct `aws-sdk-s3` usage with the `object_store`
//! crate so the same WAL backup/restore logic works against both S3 and
//! Azure Blob Storage. The backend is selected by `LIBSQL_BOTTOMLESS_PROVIDER`
//! (`s3` — default — or `azure`).
//!
//! Why this exists: shipping WAL to S3-compatible gateways in front of Azure
//! Blob (s3proxy / versitygw) backs up correctly but FAILS on restore, because
//! bottomless paginates `ListObjects` with the v1 `marker` parameter which those
//! gateways do not translate to Azure listing. Going through `object_store`'s
//! native `MicrosoftAzure` backend removes the S3 wire entirely, so listing,
//! ranged reads and multipart all use Azure-native semantics.

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutPayload, WriteMultipart};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

/// A storage backend (S3 or Azure) scoped to a single bucket / container.
/// Keys are object paths *within* that bucket (no bucket prefix), matching the
/// `object_store` model.
#[derive(Clone)]
pub struct BlobStore {
    inner: Arc<dyn ObjectStore>,
    /// Bucket (S3) or container (Azure) name — for logs / diagnostics only.
    pub bucket: String,
}

/// Minimal object listing entry, decoupled from the SDK response types.
#[derive(Clone, Debug)]
pub struct ObjMeta {
    pub key: String,
    pub last_modified: DateTime<Utc>,
    pub size: usize,
}

#[inline]
fn obj_path(key: &str) -> Path {
    // Treat the bottomless key as an already-formed object path. `Path::from`
    // percent-encodes characters object_store considers unsafe; the same
    // encoding is applied on read-back (see `list_all`), so round-trips are
    // internally consistent for a freshly-created bucket.
    Path::from(key)
}

impl BlobStore {
    pub fn s3(
        endpoint: Option<String>,
        region: String,
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> Result<Self> {
        use object_store::aws::AmazonS3Builder;
        let mut b = AmazonS3Builder::new()
            .with_region(region)
            .with_bucket_name(&bucket)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .with_allow_http(true);
        if let Some(ep) = endpoint {
            // Custom endpoint (MinIO / localstack) implies path-style addressing.
            b = b.with_endpoint(ep).with_virtual_hosted_style_request(false);
        }
        if let Some(t) = session_token {
            b = b.with_token(t);
        }
        Ok(Self {
            inner: Arc::new(b.build().context("build S3 object store")?),
            bucket,
        })
    }

    pub fn azure(
        account: String,
        access_key: String,
        container: String,
        endpoint: Option<String>,
    ) -> Result<Self> {
        use object_store::azure::MicrosoftAzureBuilder;
        let mut b = MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_access_key(access_key)
            .with_container_name(&container)
            .with_allow_http(true);
        if let Some(ep) = endpoint {
            b = b.with_endpoint(ep);
        }
        Ok(Self {
            inner: Arc::new(b.build().context("build Azure object store")?),
            bucket: container,
        })
    }

    /// Fetch a whole object into memory. `None` if the key does not exist.
    pub async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>> {
        match self.inner.get(&obj_path(key)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("get {key}")),
        }
    }

    /// Ranged read (used to peek at fixed-size headers without fetching the body).
    pub async fn get_range(&self, key: &str, range: Range<usize>) -> Result<Bytes> {
        Ok(self.inner.get_range(&obj_path(key), range).await?)
    }

    /// Streaming reader for large objects (db snapshots, frame batches).
    /// `None` if the key does not exist.
    pub async fn get_reader(&self, key: &str) -> Result<Option<impl AsyncRead + Unpin>> {
        match self.inner.get(&obj_path(key)).await {
            Ok(r) => {
                let stream = r.into_stream().map(|res| {
                    res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                });
                Ok(Some(StreamReader::new(stream)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Upload an in-memory object.
    pub async fn put_bytes(&self, key: &str, data: Bytes) -> Result<()> {
        self.inner
            .put(&obj_path(key), PutPayload::from_bytes(data))
            .await?;
        Ok(())
    }

    /// Upload a file streamed in 8 MiB parts so large snapshots never buffer
    /// entirely in RAM.
    pub async fn put_file(&self, key: &str, file: &std::path::Path) -> Result<()> {
        let mut f = tokio::fs::File::open(file)
            .await
            .with_context(|| format!("open {} for upload", file.display()))?;
        let upload = self.inner.put_multipart(&obj_path(key)).await?;
        let mut writer = WriteMultipart::new(upload);
        let mut buf = vec![0u8; 8 * 1024 * 1024];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            writer.write(&buf[..n]);
        }
        writer.finish().await?;
        Ok(())
    }

    /// List every object under `prefix`, lexicographically sorted (the order
    /// `object_store` and both S3/Azure guarantee). Replaces the old manual
    /// `marker` pagination — the backend handles continuation internally.
    ///
    /// * `max` — stop after N entries (callers that only need the newest).
    /// * `start_after` — skip keys `<= start_after` (resume semantics).
    pub async fn list_all(
        &self,
        prefix: &str,
        max: Option<usize>,
        start_after: Option<&str>,
    ) -> Result<Vec<ObjMeta>> {
        let mut out = Vec::new();
        let mut stream = self.inner.list(Some(&obj_path(prefix)));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let key = meta.location.as_ref().to_string();
            if let Some(sa) = start_after {
                if key.as_str() <= sa {
                    continue;
                }
            }
            out.push(ObjMeta {
                key,
                last_modified: meta.last_modified,
                size: meta.size as usize,
            });
            if let Some(mx) = max {
                if out.len() >= mx {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(&obj_path(key)).await?;
        Ok(())
    }

    /// Best-effort reachability probe for the bucket/container.
    pub async fn accessible(&self) -> bool {
        let mut s = self.inner.list(None);
        match s.next().await {
            Some(Ok(_)) | None => true,
            Some(Err(_)) => false,
        }
    }
}
