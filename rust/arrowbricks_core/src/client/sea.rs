//! The REST Statement-Execution-API (SEA) backend: its own wire response
//! shapes (typed, not a dynamic `serde_json::Value` tree -- see this crate's
//! own reasoning below), session pooling (`create_session`/`checkout_session`/
//! `checkin_session`/`close_all_sessions`), statement submit/poll
//! (`execute_arrow_statement`/`execute_arrow_statement_prefer_inline`/
//! `submit_and_poll`/`execute_statement`), and chunk-manifest resolution plus
//! the bounded-concurrency chunk-fetch worker pool
//! (`fetch_chunk_index`/`fetch_pre_resolved_links`/`fetch_chunks_with_backpressure`).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::DbClient;
use super::POLL_INTERVAL;
use super::error::{ApiError, join_error};
use super::model::{
    CancelHandle, ChunkItem, ChunkMeta, ColumnDescription, InlineOrExternal, QueryStatsAccumulator,
    StatementSubmitResult,
};

/// Typed response shapes -- replaces navigating a dynamic `serde_json::Value`
/// tree with `.get("...").and_then(|v| v.as_str())` chains everywhere. Same
/// data, but serde deserializes straight into these instead of building an
/// intermediate `Value` tree first; matters most for a large manifest (a
/// result with thousands of chunks), which otherwise means allocating a
/// generic map/array node per chunk before ever extracting `chunk_index`.
#[derive(Deserialize)]
struct SessionCreateBody {
    session_id: String,
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
struct ManifestSchemaBody {
    #[serde(default)]
    columns: Vec<ColumnDescription>,
}

#[derive(Deserialize, Default)]
struct ManifestBody {
    #[serde(default)]
    chunks: Vec<ChunkMetaRaw>,
    #[serde(default)]
    schema: Option<ManifestSchemaBody>,
    /// Echoes back whether the server actually honored our
    /// `result_compression: "LZ4_FRAME"` request (see `execute_statement`) --
    /// decompression is driven by this, not by what we asked for, in case a
    /// disposition/format combination ever doesn't honor it.
    #[serde(default)]
    result_compression: Option<String>,
}

#[derive(Deserialize)]
struct StatementResponseBody {
    statement_id: String,
    status: StatementStatusBody,
    #[serde(default)]
    manifest: Option<ManifestBody>,
    /// SEA embeds whichever chunks are already ready straight in the
    /// submit/poll response body -- confirmed against a real workspace: a
    /// SUCCEEDED response's `result.external_links` already contained chunk
    /// 0's presigned URL, disposition EXTERNAL_LINKS same as always. Used to
    /// skip that chunk's own `GET .../result/chunks/{i}` resolution request
    /// entirely (see `ChunkMeta::pre_resolved_links`) -- a full round trip
    /// saved for whichever chunks land here, which for a fast/small query is
    /// a meaningful fraction of total latency.
    #[serde(default)]
    result: Option<ResultBody>,
}

#[derive(Deserialize)]
struct ResultLinkBody {
    // `#[serde(default)]` (not `Option<i64>`) deliberately: an omitempty-style
    // server serializer would drop a zero-valued `chunk_index` field entirely
    // rather than emit `0` -- exactly the chunk this optimization targets
    // most (chunk 0), and exactly the case that would otherwise turn one
    // optional fast-path field into a hard parse failure for the *whole*
    // statement response (`authed_json` fails the entire `StatementResponseBody`
    // deserialize on any missing required field, no retry). Defaulting to 0
    // reconstructs the omitted value correctly either way.
    #[serde(default)]
    chunk_index: i64,
    // Same reasoning, plus an empty link is filtered out where this is
    // consumed (`execute_statement`) rather than trusted -- a link that
    // somehow came through empty is worse than just re-resolving normally.
    #[serde(default)]
    external_link: String,
}

#[derive(Deserialize, Default)]
struct ResultBody {
    #[serde(default)]
    external_links: Vec<ResultLinkBody>,
    /// Only present for `disposition: INLINE` + `format: JSON_ARRAY` -- each
    /// row a `Vec<Option<String>>` (every non-null value a string,
    /// Databricks' own JSON_ARRAY contract). Consumed by
    /// `json_convert::json_array_to_record_batch` on the `prefer_inline`
    /// fast path.
    #[serde(default)]
    data_array: Option<Vec<Vec<Option<String>>>>,
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

impl DbClient {
    async fn create_session(&self, catalog: Option<&str>, schema: Option<&str>) -> Result<String, ApiError> {
        let url = format!("{}/api/2.0/sql/sessions", self.host);
        let mut body = json!({ "warehouse_id": self.warehouse_id });
        if let Some(c) = catalog {
            body["catalog_name"] = json!(c);
        }
        if let Some(s) = schema {
            body["schema_name"] = json!(s);
        }
        let data: SessionCreateBody = self.authed_json(reqwest::Method::POST, &url, Some(&body), None).await?;
        Ok(data.session_id)
    }

    async fn delete_session(&self, session_id: &str) {
        let url = format!("{}/api/2.0/sql/sessions/{session_id}", self.host);
        let body = json!({ "warehouse_id": self.warehouse_id });
        // Best-effort: a failed delete just leaves the session to be reaped
        // by Databricks' own server-side TTL -- not worth surfacing an error
        // for, since this only ever runs during pool cleanup/discard, well
        // after the statement it backed already reached a terminal state.
        let _: Result<IgnoredAny, ApiError> = self.authed_json(reqwest::Method::DELETE, &url, Some(&body), None).await;
    }

    /// Hands back a pooled session for (`catalog`, `schema`) if one's idle,
    /// creates one if the pool for that key isn't at `MAX_SESSIONS_PER_KEY`
    /// yet, or `None` if neither -- the caller falls back to a plain
    /// session-less submission in that case, see `session_pool`'s own doc
    /// comment for why this never blocks instead.
    async fn checkout_session(&self, catalog: Option<&str>, schema: Option<&str>) -> Option<String> {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        if let Some(id) = self.session_pool.take(&key) {
            return Some(id);
        }
        if !self.session_pool.reserve(&key) {
            return None;
        }
        match self.create_session(catalog, schema).await {
            Ok(id) => Some(id),
            Err(_) => {
                self.session_pool.release(&key);
                None
            }
        }
    }

    /// Returns a session to the pool for reuse (`keep = true`, the statement
    /// it backed reached SUCCEEDED/FAILED/CANCELED cleanly) or discards it
    /// (`keep = false`) -- see `session_pool`'s doc comment for why any error
    /// discards rather than reuses.
    fn checkin_session(&self, catalog: Option<&str>, schema: Option<&str>, session_id: String, keep: bool) {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        self.session_pool.checkin(key, session_id, keep);
    }

    /// Best-effort cleanup of every currently-idle pooled session -- meant
    /// to be called once, from the Python-facing client's own close/aclose.
    /// A session still checked out (an in-flight statement) at the time this
    /// runs isn't in `idle` and so isn't closed here -- acceptable, same
    /// server-side TTL reaping as a discarded/errored session above; calling
    /// this before every pending statement has finished is a caller
    /// ordering issue, not something this method can fix from inside.
    pub async fn close_all_sessions(&self) {
        for id in self.session_pool.drain_idle() {
            self.delete_session(&id).await;
        }
    }

    // ---- Thrift backend (opt-in `protocol="thrift"`) --------------------

    /// Submit + poll an EXTERNAL_LINKS/ARROW_STREAM statement to terminal
    /// state. `parameters`, if given, is Databricks' own named-parameter
    /// format ([{"name":..., "value":..., "type":...}] bound against `:name`
    /// markers in `statement`) -- passed through to the request body
    /// verbatim, same as the Python original does no validation of its own
    /// shape either.
    pub async fn execute_arrow_statement(
        &self,
        statement: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementSubmitResult, ApiError> {
        self.execute_statement(statement, "ARROW_STREAM", catalog, schema, parameters, stats)
            .await
    }

    /// Tries `disposition: INLINE` + `format: JSON_ARRAY` first -- for a
    /// small result, Databricks embeds the whole result directly in this
    /// same submit/poll response (`result.data_array`), skipping the
    /// separate chunk-resolution-and-blob-fetch round trip the normal
    /// `EXTERNAL_LINKS` path always needs. Confirmed against a real
    /// workspace: exceeding INLINE's byte limit (26,214,400 bytes / 25MiB)
    /// fails the statement cleanly with a specific, matchable error
    /// message -- never silent truncation -- and `INLINE_OR_EXTERNAL_LINKS`
    /// ("HYBRID", which would let the server choose per-query) returned a
    /// clean "not a supported disposition" 400 on that same workspace, so
    /// isn't used here. `format` must be `JSON_ARRAY` for `INLINE` --
    /// Databricks rejects `INLINE`+`ARROW_STREAM` outright (also confirmed).
    /// This module stays Arrow-agnostic on purpose (see its own module doc
    /// comment) -- `InlineOrExternal::Inline` carries the raw JSON_ARRAY
    /// rows straight through; converting them into a `RecordBatch` (and
    /// falling back to a fresh `EXTERNAL_LINKS` submission if that
    /// conversion hits a column type it doesn't handle) is `pipeline/sea.rs`'s
    /// job via `json_convert`, same division of responsibility as every
    /// other decode step in this crate.
    ///
    /// Falls back to a **fresh, independent** `execute_arrow_statement` call
    /// (not a retry of this same submission) whenever INLINE doesn't pan
    /// out at the HTTP/statement level: the byte limit was exceeded, or
    /// `data_array` is unexpectedly absent despite SUCCEEDED. This is a
    /// second, distinct statement execution, not a retry of a possibly-
    /// already-run one, so it carries none of the double-execution risk
    /// that made POST retries unsafe in the first place -- see
    /// `client/error.rs`'s `ApiError::from_reqwest`/`from_status` for that
    /// idempotency reasoning -- the first attempt's outcome is fully known
    /// (it reached a terminal state)
    /// before the second one is ever submitted. A genuine query error (bad
    /// SQL, permission denied) propagates immediately instead, same as the
    /// normal path -- no reason to mask it behind a pointless second
    /// attempt.
    ///
    /// Not the default -- opt-in only (`prefer_inline` on the Python-facing
    /// `execute()`/`Cursor.execute()`), since a caller who doesn't expect a
    /// small result pays for two full statement executions on the (common,
    /// for them) fallback path instead of one.
    pub async fn execute_arrow_statement_prefer_inline(
        &self,
        statement: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<InlineOrExternal, ApiError> {
        let mut body = json!({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        });
        if let Some(p) = parameters.clone() {
            body["parameters"] = p;
        }

        let outcome = self.submit_and_poll(body, catalog, schema, stats).await;
        let data = match outcome {
            Ok(d) => d,
            Err(e) if e.message.contains("Inline byte limit exceeded") => {
                // Same `stats` accumulator, not a fresh one -- this fallback
                // is invisible to the caller (one `execute()` call in, one
                // result out), so its own submit/poll/retry activity counts
                // toward the same `QueryStats` event as the INLINE attempt
                // that preceded it, not a second, separate one.
                return self
                    .execute_arrow_statement(statement, catalog, schema, parameters, stats)
                    .await
                    .map(InlineOrExternal::External);
            }
            Err(e) => return Err(e),
        };

        let manifest = data.manifest.unwrap_or_default();
        let columns = manifest.schema.map(|s| s.columns).unwrap_or_default();
        let data_array = data.result.and_then(|r| r.data_array);
        match data_array {
            Some(rows) => Ok(InlineOrExternal::Inline {
                statement_id: data.statement_id,
                rows,
                columns,
            }),
            // No data_array despite SUCCEEDED -- shouldn't happen given a
            // non-error status, but this crate never guesses at a missing
            // field. **Does not** fall back to a fresh EXTERNAL_LINKS
            // submission the way the byte-limit-exceeded case above does --
            // found in code review (2026-08-11, alongside the identical bug
            // in `pipeline/sea.rs`'s own JSON-conversion-failure fallback): the
            // byte-limit case is safe to resubmit because that statement
            // reached a FAILED state (nothing committed server-side); this
            // one only runs after SUCCEEDED, meaning any DML side effects
            // already happened, so blindly resubmitting the identical SQL
            // would risk silently duplicating them for non-idempotent SQL.
            // A clean error is the safe choice here regardless of how
            // unlikely this branch is to ever actually fire.
            None => Err(ApiError::permanent(format!(
                "statement {} succeeded with disposition=INLINE but the response had no data_array -- refusing \
                 to automatically re-run the query to avoid duplicating any write it performed",
                data.statement_id
            ))),
        }
    }

    /// Shared by `execute_statement` and `execute_arrow_statement_prefer_inline`:
    /// POST the statement, poll until a terminal state, and turn FAILED/
    /// CANCELED into an `Err` -- everything both callers need before they
    /// diverge on how to interpret a SUCCEEDED response's `result`/`manifest`.
    ///
    /// `body` must *not* already carry `catalog`/`schema`/`session_id` --
    /// this method owns that decision: a pooled session for (`catalog`,
    /// `schema`) if one's available (see `checkout_session`),
    /// falling back to setting `catalog`/`schema` directly on the body
    /// otherwise (Databricks rejects `session_id` combined with either
    /// field). The session, if any, is returned to the pool on a clean
    /// terminal state and discarded on any error.
    async fn submit_and_poll(
        &self,
        mut body: Value,
        catalog: Option<&str>,
        schema: Option<&str>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementResponseBody, ApiError> {
        // Timed and recorded here -- the *only* place this call happens for
        // SEA -- so a caller (e.g. `pipeline/sea.rs`'s `execute_lazy`) reads
        // `stats.warehouse_wait_s()` back afterward instead of also timing
        // its own, second, now-cache-warm call to the same method. See
        // `QueryStatsAccumulator::warehouse_wait_bits`'s own doc comment for
        // the real double-call bug this replaced.
        let warehouse_t0 = Instant::now();
        self.ensure_warehouse_running().await?;
        stats.add_warehouse_wait_s(warehouse_t0.elapsed().as_secs_f64());
        let session_id = self.checkout_session(catalog, schema).await;
        match &session_id {
            Some(id) => body["session_id"] = json!(id),
            None => {
                if let Some(c) = catalog {
                    body["catalog"] = json!(c);
                }
                if let Some(s) = schema {
                    body["schema"] = json!(s);
                }
            }
        }

        let mut checkin = SessionCheckin {
            client: self,
            catalog,
            schema,
            session_id,
        };
        let result = self.submit_and_poll_inner(body, stats).await;
        checkin.finish(result.is_ok());
        result
    }

    async fn submit_and_poll_inner(
        &self,
        body: Value,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementResponseBody, ApiError> {
        let url = format!("{}/api/2.0/sql/statements", self.host);
        let mut data: StatementResponseBody = self
            .authed_json(reqwest::Method::POST, &url, Some(&body), Some(stats))
            .await?;

        stats.set_in_flight(CancelHandle::Sea {
            statement_id: data.statement_id.clone(),
        });
        while !matches!(
            data.status.state.as_str(),
            "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED"
        ) {
            tokio::time::sleep(POLL_INTERVAL).await;
            let poll_url = format!("{}/api/2.0/sql/statements/{}", self.host, data.statement_id);
            data = self
                .authed_json(reqwest::Method::GET, &poll_url, None, Some(stats))
                .await?;
        }
        stats.clear_in_flight();

        match data.status.state.as_str() {
            "FAILED" => {
                let err = data.status.error.unwrap_or(StatementErrorBody {
                    error_code: None,
                    message: None,
                });
                Err(ApiError::statement_failed(format!(
                    "Databricks statement failed [{}]: {}",
                    err.error_code.as_deref().unwrap_or(""),
                    err.message.as_deref().unwrap_or(""),
                )))
            }
            "CANCELED" => Err(ApiError::statement_failed("Databricks statement was canceled")),
            _ => Ok(data),
        }
    }

    async fn execute_statement(
        &self,
        statement: &str,
        format: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementSubmitResult, ApiError> {
        let mut body = json!({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "EXTERNAL_LINKS",
            "format": format,
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        });
        if self.compress_results {
            // Matches databricks-sql-python's default
            // (enable_query_result_lz4_compression=True) -- trades a cheap
            // client-side LZ4 decompress for meaningfully less data over the
            // wire, which is the actual bottleneck for a large result (local
            // Arrow-IPC decode is already fast; network transfer isn't).
            // Runtime-toggleable via `compress_results=False` -- see
            // `with_compress_results`.
            body["result_compression"] = json!("LZ4_FRAME");
        }
        if let Some(p) = parameters {
            body["parameters"] = p;
        }

        let data = self.submit_and_poll(body, catalog, schema, stats).await?;
        let manifest = data.manifest.unwrap_or_default();
        let compressed = manifest.result_compression.as_deref() == Some("LZ4_FRAME");
        // `Vec` per index, not a plain map entry -- see `ChunkMeta::pre_resolved_links`'s
        // doc for why collapsing to one would silently lose rows.
        let mut pre_resolved: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
        if let Some(r) = data.result {
            for link in r.external_links {
                if link.external_link.is_empty() {
                    continue;
                }
                pre_resolved
                    .entry(link.chunk_index)
                    .or_default()
                    .push(link.external_link);
            }
        }
        let chunk_metas = manifest
            .chunks
            .into_iter()
            .map(|c| ChunkMeta {
                pre_resolved_links: pre_resolved.remove(&c.chunk_index).unwrap_or_default(),
                chunk_index: c.chunk_index,
                row_count: c.row_count,
            })
            .collect();
        let columns = manifest.schema.map(|s| s.columns).unwrap_or_default();
        Ok(StatementSubmitResult {
            statement_id: data.statement_id,
            chunk_metas,
            columns,
            compressed,
        })
    }

    async fn fetch_chunk_index(
        &self,
        statement_id: &str,
        chunk_index: i64,
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Vec<Bytes>, ApiError> {
        let url = format!(
            "{}/api/2.0/sql/statements/{}/result/chunks/{}",
            self.host, statement_id, chunk_index
        );
        let data: ChunkLinksBody = self.authed_json(reqwest::Method::GET, &url, None, Some(stats)).await?;
        let links: Vec<String> = data.external_links.into_iter().map(|l| l.external_link).collect();
        self.fetch_pre_resolved_links(&links, compressed, stats).await
    }

    /// Same shape as `fetch_chunk_index`, but for links the statement submit/
    /// poll response already embedded -- no resolution GET needed first.
    async fn fetch_pre_resolved_links(
        &self,
        links: &[String],
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Vec<Bytes>, ApiError> {
        let mut blobs = Vec::with_capacity(links.len());
        for link in links {
            blobs.push(self.fetch_link_bytes(link, compressed, stats).await?);
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
        compressed: bool,
        stats: Arc<QueryStatsAccumulator>,
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
                let stats = stats.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        let meta = { queue.lock().unwrap().pop_front() };
                        let Some(meta) = meta else { return Ok(()) };
                        let fetched = if meta.pre_resolved_links.is_empty() {
                            client
                                .fetch_chunk_index(&statement_id, meta.chunk_index, compressed, &stats)
                                .await
                        } else {
                            client
                                .fetch_pre_resolved_links(&meta.pre_resolved_links, compressed, &stats)
                                .await
                        };
                        match fetched {
                            Ok(blobs) => {
                                // One blob ⇒ `meta.row_count` (the whole chunk_index's
                                // declared count) and "this blob's count" are the same
                                // number -- see `ChunkItem::truncate_to`'s own doc comment.
                                let truncate_to = if blobs.len() == 1 { meta.row_count } else { None };
                                for blob in blobs {
                                    let item = ChunkItem {
                                        blob,
                                        row_count: meta.row_count,
                                        chunk_index: meta.chunk_index,
                                        truncate_to,
                                    };
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
/// Returns `submit_and_poll`'s session to the pool even when its future is
/// dropped mid-poll (a timeout or cancellation) -- without it the pool's
/// reservation for that key leaks, and after `MAX_SESSIONS_PER_KEY` such
/// drops every later query for the key runs session-less.
struct SessionCheckin<'a> {
    client: &'a DbClient,
    catalog: Option<&'a str>,
    schema: Option<&'a str>,
    session_id: Option<String>,
}

impl SessionCheckin<'_> {
    fn finish(&mut self, keep: bool) {
        if let Some(id) = self.session_id.take() {
            self.client.checkin_session(self.catalog, self.schema, id, keep);
        }
    }
}

impl Drop for SessionCheckin<'_> {
    fn drop(&mut self) {
        self.finish(false);
    }
}

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
    use crate::client::ApiErrorKind;

    fn ok_task() -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async { Ok(()) })
    }

    fn err_task(msg: &'static str) -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async move {
            Err(ApiError {
                message: msg.to_string(),
                transient: false,
                kind: ApiErrorKind::Other,
            })
        })
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
        let err = join_first_error(handles)
            .await
            .expect("a panicking task must surface as an error, not vanish");
        assert!(
            err.message.contains("panicked"),
            "error message should mention the panic: {}",
            err.message
        );
    }
}
