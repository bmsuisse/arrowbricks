//! Port of `client.py`'s statement submit/poll/retry logic. No Arrow
//! dependency here on purpose, same reasoning as the Python original: chunk
//! bytes are handed off raw, decoding happens in `pipeline.rs`.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Typed response shapes -- replaces navigating a dynamic `serde_json::Value`
/// tree with `.get("...").and_then(|v| v.as_str())` chains everywhere. Same
/// data, but serde deserializes straight into these instead of building an
/// intermediate `Value` tree first; matters most for a large manifest (a
/// result with thousands of chunks), which otherwise means allocating a
/// generic map/array node per chunk before ever extracting `chunk_index`.
#[derive(Deserialize)]
struct WarehouseStatusBody {
    state: String,
}

#[derive(Deserialize)]
struct StatementStatusBody {
    state: String,
    #[serde(default)]
    error: Option<StatementErrorBody>,
}

#[derive(Deserialize)]
struct StatementErrorBody {
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Deserialize)]
struct ChunkMetaRaw {
    chunk_index: i64,
    #[serde(default)]
    row_count: Option<i64>,
}

#[derive(Deserialize, Default)]
struct ManifestBody {
    #[serde(default)]
    chunks: Vec<ChunkMetaRaw>,
}

#[derive(Deserialize)]
struct StatementResponseBody {
    statement_id: String,
    status: StatementStatusBody,
    #[serde(default)]
    manifest: Option<ManifestBody>,
}

#[derive(Deserialize)]
struct ExternalLinkBody {
    external_link: String,
}

#[derive(Deserialize, Default)]
struct ChunkLinksBody {
    #[serde(default)]
    external_links: Vec<ExternalLinkBody>,
}

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const RETRY_ATTEMPTS: u32 = 6;
const RETRY_MAX_WAIT_S: f64 = 20.0;

#[derive(Debug)]
pub struct ApiError {
    pub message: String,
    /// 401/403/408/429/5xx -- mirrors `_is_transient_error`. Transport-level
    /// timeouts/connect errors are never transient, same reasoning as the
    /// Python original: a genuinely stalled connection should fail fast on
    /// the caller's own timeout, not be retried here.
    pub transient: bool,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for ApiError {}

impl ApiError {
    fn permanent(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), transient: false }
    }

    fn from_reqwest(e: reqwest::Error) -> Self {
        Self { message: e.to_string(), transient: false }
    }

    fn from_status(status: StatusCode, body: &str) -> Self {
        let transient = matches!(status.as_u16(), 401 | 403 | 408 | 429) || status.is_server_error();
        Self { message: format!("HTTP {status}: {body}"), transient }
    }
}

/// Converts a `JoinError` (a spawned task panicked, or was cancelled) into an
/// `ApiError` instead of letting it be silently dropped. Shared by both the
/// fetch-worker join in `fetch_chunks_with_backpressure` and the decode
/// `spawn_blocking` join in `pipeline.rs` -- a panicking task must surface as
/// an error, not as a quietly-truncated result set.
pub(crate) fn join_error(e: tokio::task::JoinError) -> ApiError {
    ApiError { message: format!("task panicked: {e}"), transient: false }
}

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub chunk_index: i64,
    pub row_count: Option<i64>,
}

#[derive(Debug)]
pub struct ChunkItem {
    /// `Bytes` (not `Vec<u8>`) so the bytes reqwest already received off the
    /// socket ride all the way to the Arrow-IPC decoder with zero copies --
    /// `Bytes` is a refcounted view over the same allocation, cheap to clone
    /// and to hand across the `spawn_blocking` boundary in pipeline.rs.
    pub blob: Bytes,
    pub row_count: Option<i64>,
    pub chunk_index: i64,
}

async fn retry_call<F, Fut, T>(mut f: F) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ApiError>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt == RETRY_ATTEMPTS - 1 || !e.transient {
                    return Err(e);
                }
                let wait = 2f64.powi(attempt as i32).min(RETRY_MAX_WAIT_S);
                tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                attempt += 1;
            }
        }
    }
}

pub struct DbClient {
    http: Client,
    host: String,
    warehouse_id: String,
    token: String,
    http_timeout: Duration,
    wait_timeout: String,
    pub chunk_fetch_concurrency: usize,
    warehouse_start_timeout: Duration,
    warehouse_confirmed_running_ttl: Duration,
    warehouse_confirmed_running_at: Mutex<Option<Instant>>,
}

impl DbClient {
    pub fn new(host: &str, warehouse_id: &str, token: &str) -> Self {
        let host = host.trim_end_matches('/');
        // Only force https:// when no scheme was given at all -- same as
        // Python's check, but relaxed to not clobber an explicit http://
        // (needed to point this at a local wiremock instance for testing;
        // real Databricks workspaces are https-only regardless).
        let host = if host.starts_with("https://") || host.starts_with("http://") {
            host.to_string()
        } else {
            format!("https://{host}")
        };
        Self {
            http: Client::builder().build().expect("failed to build reqwest client"),
            host,
            warehouse_id: warehouse_id.to_string(),
            token: token.to_string(),
            http_timeout: Duration::from_secs(60),
            wait_timeout: "30s".to_string(),
            // Python's DatabricksClient defaults to 6, tuned for asyncio+GIL
            // where higher concurrency stops paying off past single digits
            // (see its own comment). Ad hoc benchmarking against a mocked
            // warehouse (80 chunks, 8ms simulated per-chunk latency) showed
            // this Rust core's real OS-thread parallelism keeps improving up
            // to ~64 concurrent fetches (119ms@8, 70ms@16, 48ms@32, 42ms@64),
            // regressing slightly at 128 once concurrency exceeds the chunk
            // count -- not reproducible from a script in this repo, so take
            // the specific numbers as directional, not a checked-in
            // benchmark. 32 is a reasonable default headroom below that peak
            // without chasing a number that's workload-shaped; callers with
            // very large chunk counts
            // may still want to raise it further.
            chunk_fetch_concurrency: 32,
            warehouse_start_timeout: Duration::from_secs(300),
            warehouse_confirmed_running_ttl: Duration::from_secs(30),
            warehouse_confirmed_running_at: Mutex::new(None),
        }
    }

    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.chunk_fetch_concurrency = n.max(1);
        self
    }

    async fn authed_json<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
    ) -> Result<T, ApiError> {
        retry_call(|| async {
            let mut req = self
                .http
                .request(method.clone(), url)
                .bearer_auth(&self.token)
                .timeout(self.http_timeout);
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = req.send().await.map_err(ApiError::from_reqwest)?;
            let status = resp.status();
            let text = resp.text().await.map_err(ApiError::from_reqwest)?;
            if !status.is_success() {
                return Err(ApiError::from_status(status, &text));
            }
            serde_json::from_str::<T>(&text).map_err(|e| ApiError::permanent(format!("bad JSON body: {e}")))
        })
        .await
    }

    /// Unauthenticated -- external links are presigned blob-storage URLs,
    /// same as `_fetch_link_bytes` in the Python client.
    async fn fetch_link_bytes(&self, url: &str) -> Result<Bytes, ApiError> {
        retry_call(|| async {
            let resp = self.http.get(url).timeout(self.http_timeout).send().await.map_err(ApiError::from_reqwest)?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text));
            }
            resp.bytes().await.map_err(ApiError::from_reqwest)
        })
        .await
    }

    async fn ensure_warehouse_running(&self) -> Result<(), ApiError> {
        {
            let confirmed = *self.warehouse_confirmed_running_at.lock().unwrap();
            if let Some(at) = confirmed {
                if at.elapsed() < self.warehouse_confirmed_running_ttl {
                    return Ok(());
                }
            }
        }

        let url = format!("{}/api/2.0/sql/warehouses/{}", self.host, self.warehouse_id);
        let data: WarehouseStatusBody = self.authed_json(reqwest::Method::GET, &url, None).await?;
        if data.state == "RUNNING" {
            *self.warehouse_confirmed_running_at.lock().unwrap() = Some(Instant::now());
            return Ok(());
        }
        if data.state == "STOPPED" {
            self.authed_json::<IgnoredAny>(reqwest::Method::POST, &format!("{url}/start"), None).await?;
        }

        let deadline = Instant::now() + self.warehouse_start_timeout;
        while Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
            let data: WarehouseStatusBody = self.authed_json(reqwest::Method::GET, &url, None).await?;
            if data.state == "RUNNING" {
                *self.warehouse_confirmed_running_at.lock().unwrap() = Some(Instant::now());
                return Ok(());
            }
        }
        // Falls through, same as Python: let statement submission surface
        // whatever's actually wrong instead of raising here.
        Ok(())
    }

    /// Submit + poll an EXTERNAL_LINKS/ARROW_STREAM statement to terminal
    /// state. Returns (statement_id, chunk metas) -- manifest fields beyond
    /// `chunks` (e.g. `schema`) aren't consumed anywhere downstream, so
    /// there's nothing to gain from carrying the rest of the manifest along.
    pub async fn execute_arrow_statement(
        &self,
        statement: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Result<(String, Vec<ChunkMeta>), ApiError> {
        let mut body = json!({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "EXTERNAL_LINKS",
            "format": "ARROW_STREAM",
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        });
        if let Some(c) = catalog {
            body["catalog"] = json!(c);
        }
        if let Some(s) = schema {
            body["schema"] = json!(s);
        }

        self.ensure_warehouse_running().await?;
        let url = format!("{}/api/2.0/sql/statements", self.host);
        let mut data: StatementResponseBody = self.authed_json(reqwest::Method::POST, &url, Some(&body)).await?;

        while !matches!(data.status.state.as_str(), "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED") {
            tokio::time::sleep(POLL_INTERVAL).await;
            let poll_url = format!("{}/api/2.0/sql/statements/{}", self.host, data.statement_id);
            data = self.authed_json(reqwest::Method::GET, &poll_url, None).await?;
        }

        match data.status.state.as_str() {
            "FAILED" => {
                let err = data.status.error.unwrap_or(StatementErrorBody { error_code: None, message: None });
                return Err(ApiError::permanent(format!(
                    "Databricks statement failed [{}]: {}",
                    err.error_code.as_deref().unwrap_or(""),
                    err.message.as_deref().unwrap_or(""),
                )));
            }
            "CANCELED" => return Err(ApiError::permanent("Databricks statement was canceled")),
            _ => {}
        }

        let chunk_metas = data
            .manifest
            .unwrap_or_default()
            .chunks
            .into_iter()
            .map(|c| ChunkMeta { chunk_index: c.chunk_index, row_count: c.row_count })
            .collect();
        Ok((data.statement_id, chunk_metas))
    }

    async fn fetch_chunk_index(&self, statement_id: &str, chunk_index: i64) -> Result<Vec<Bytes>, ApiError> {
        let url = format!("{}/api/2.0/sql/statements/{}/result/chunks/{}", self.host, statement_id, chunk_index);
        let data: ChunkLinksBody = self.authed_json(reqwest::Method::GET, &url, None).await?;

        let mut blobs = Vec::with_capacity(data.external_links.len());
        for link in data.external_links {
            blobs.push(self.fetch_link_bytes(&link.external_link).await?);
        }
        Ok(blobs)
    }

    /// Bounded-concurrency worker pool, mirroring
    /// `_fetch_chunks_with_backpressure`: a fixed pool of workers pulls from
    /// a shared queue of chunk metas, each pushing its blobs into a bounded
    /// mpsc channel one at a time -- `Sender::send` blocks (backpressure)
    /// until the consumer takes the previous item, so peak buffered chunks
    /// stays at ~concurrency, not O(whole result). Errors don't cancel
    /// sibling workers; every worker runs to completion, successful chunks
    /// already fetched are still yielded, and the first error (if any) is
    /// delivered as the channel's last item -- same trade-off as Python's
    /// `close_when_done`.
    pub fn fetch_chunks_with_backpressure(
        self: std::sync::Arc<Self>,
        statement_id: String,
        chunk_metas: Vec<ChunkMeta>,
    ) -> mpsc::Receiver<Result<ChunkItem, ApiError>> {
        let concurrency = self.chunk_fetch_concurrency.max(1);
        let (tx, rx) = mpsc::channel::<Result<ChunkItem, ApiError>>(concurrency);
        let queue = std::sync::Arc::new(Mutex::new(VecDeque::from(chunk_metas)));

        tokio::spawn(async move {
            let mut handles = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let client = self.clone();
                let queue = queue.clone();
                let worker_tx = tx.clone();
                let statement_id = statement_id.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        let meta = { queue.lock().unwrap().pop_front() };
                        let Some(meta) = meta else { return Ok(()) };
                        match client.fetch_chunk_index(&statement_id, meta.chunk_index).await {
                            Ok(blobs) => {
                                for blob in blobs {
                                    let item = ChunkItem { blob, row_count: meta.row_count, chunk_index: meta.chunk_index };
                                    if worker_tx.send(Ok(item)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }));
            }

            // `tx` itself (not a clone) stays alive across the join below, so
            // the channel can't close until we've had a chance to deliver a
            // terminal error -- dropped implicitly at the end of this block.
            if let Some(e) = join_first_error(handles).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        rx
    }
}

/// Joins every handle, returning the first error -- whether the task
/// returned `Err(ApiError)` or the task itself panicked (`Err(JoinError)`,
/// e.g. from a poisoned mutex after a sibling panicked first). Missing the
/// panic case would let that worker's unfetched work vanish with no error at
/// all: the channel closing normally looks to the consumer exactly like a
/// complete, successful result instead of a truncated one.
async fn join_first_error(handles: Vec<tokio::task::JoinHandle<Result<(), ApiError>>>) -> Option<ApiError> {
    let mut first_err = None;
    for h in handles {
        let outcome = match h.await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(join_err) => Some(join_error(join_err)),
        };
        if first_err.is_none() {
            first_err = outcome;
        }
    }
    first_err
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_task() -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async { Ok(()) })
    }

    fn err_task(msg: &'static str) -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async move { Err(ApiError { message: msg.to_string(), transient: false }) })
    }

    fn panicking_task() -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async { panic!("simulated worker panic (e.g. poisoned mutex)") })
    }

    #[tokio::test]
    async fn join_first_error_none_when_all_succeed() {
        let handles = vec![ok_task(), ok_task(), ok_task()];
        assert!(join_first_error(handles).await.is_none());
    }

    #[tokio::test]
    async fn join_first_error_surfaces_returned_error() {
        let handles = vec![ok_task(), err_task("boom"), ok_task()];
        let err = join_first_error(handles).await.expect("expected an error");
        assert_eq!(err.message, "boom");
    }

    /// Regression test for the bug found in code review: a worker task that
    /// *panics* (not returns Err) must still surface as an error, not vanish
    /// silently. Before the fix, `Err(JoinError)` matched neither `Ok(Err(_))`
    /// nor anything else and was dropped -- the caller would have gotten a
    /// clean, silently-truncated result instead of an error.
    #[tokio::test]
    async fn join_first_error_surfaces_panic_not_silence() {
        let handles = vec![ok_task(), panicking_task(), ok_task()];
        let err = join_first_error(handles).await.expect("a panicking task must surface as an error, not vanish");
        assert!(err.message.contains("panicked"), "error message should mention the panic: {}", err.message);
    }
}
