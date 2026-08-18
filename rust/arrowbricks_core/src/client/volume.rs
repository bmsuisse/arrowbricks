//! Databricks Unity Catalog volume file upload/delete via the Files API
//! (`/api/2.0/fs/files{volume_path}`) -- unrelated to either SQL backend,
//! just another authenticated HTTP operation this client exposes.

use bytes::Bytes;
use reqwest::StatusCode;

use super::DbClient;
use super::error::ApiError;

impl DbClient {
    /// Uploads `data` to a Unity Catalog volume path via the Files API,
    /// overwriting anything already there. `volume_path` is caller-supplied
    /// in full (e.g. `/Volumes/my_catalog/my_schema/my_volume/some/file.parquet`)
    /// -- this crate has no knowledge of any specific catalog/schema/volume.
    /// `Bytes` (not `Vec<u8>`) so a retry re-sends the same buffer via a
    /// cheap refcount clone, not a real copy.
    pub async fn upload_volume_file(&self, volume_path: &str, data: Bytes) -> Result<(), ApiError> {
        let url = format!("{}/api/2.0/fs/files{}", self.host, volume_path);
        self.retry_call(|| async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .put(&url)
                .query(&[("overwrite", "true")])
                .bearer_auth(&token)
                .header("Content-Type", "application/octet-stream")
                .body(data.clone())
                .timeout(self.http_timeout)
                .send()
                .await
                // `overwrite=true` means retrying this exact PUT is safe --
                // same idempotency reasoning as a GET.
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text, true));
            }
            Ok(())
        })
        .await
    }

    /// Deletes a file at `volume_path` (see `upload_volume_file`). A 404 is
    /// treated as success -- the file is already gone, which is fine for
    /// idempotent staging cleanup.
    pub async fn delete_volume_file(&self, volume_path: &str) -> Result<(), ApiError> {
        let url = format!("{}/api/2.0/fs/files{}", self.host, volume_path);
        self.retry_call(|| async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .delete(&url)
                .bearer_auth(&token)
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND || status.is_success() {
                return Ok(());
            }
            let text = resp.text().await.unwrap_or_default();
            Err(ApiError::from_status(status, &text, true))
        })
        .await
    }
}
