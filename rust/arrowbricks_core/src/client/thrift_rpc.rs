//! The Thrift/TCLIService backend: raw Thrift-over-HTTP RPC framing/dispatch
//! (`thrift_call`, on top of the same `retry_call_tracked` every other
//! backend uses), session pooling (mirroring the SEA session pool with one
//! necessary difference -- see `thrift_checkout_session`'s own doc comment),
//! and the RPCs `pipeline::thrift_exec` drives a statement's submit/poll/
//! fetch loop through.

use bytes::Bytes;
use serde_json::Value;

use super::error::ApiError;
use super::model::QueryStatsAccumulator;
use super::{DbClient, THRIFT_DIRECT_RESULTS_MAX_BYTES, THRIFT_DIRECT_RESULTS_MAX_ROWS};
use crate::thrift;

impl DbClient {
    fn thrift_url(&self) -> String {
        format!("{}/sql/1.0/warehouses/{}", self.host, self.warehouse_id)
    }

    /// Raw Thrift-over-HTTP POST: `Content-Type: application/x-thrift`, the
    /// same Bearer-token auth header as every other request in this crate,
    /// no framing beyond HTTP itself (confirmed against
    /// `databricks-sql-connector`'s own `THttpClient` -- see `thrift.rs`'s
    /// own module doc comment). `idempotent` gates transience exactly like
    /// `authed_json`'s -- `OpenSession`/`ExecuteStatement`/`FetchResults`
    /// (which advances a server-side cursor with `orientation: FETCH_NEXT`,
    /// so blindly replaying it on an ambiguous failure risks silently
    /// skipping or double-fetching rows) must never be retried blindly;
    /// `GetOperationStatus`/`CloseOperation`/`CloseSession`/`CancelOperation`
    /// are safe to retry (read-only or naturally idempotent).
    pub(crate) async fn thrift_call(
        &self,
        body: Bytes,
        idempotent: bool,
        stats: Option<&QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        let url = self.thrift_url();
        self.retry_call_tracked(stats, || async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .post(&url)
                .bearer_auth(&token)
                .header("Content-Type", "application/x-thrift")
                .header("User-Agent", "PyDatabricksSqlConnector/4.4.0 arrowbricks-thrift-rs")
                .body(body.clone())
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            let status = resp.status();
            let bytes = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            if !status.is_success() {
                let text = String::from_utf8_lossy(&bytes).to_string();
                return Err(ApiError::from_status(status, &text, idempotent));
            }
            Ok(bytes)
        })
        .await
    }

    fn thrift_parse_error(e: thrift::ThriftError) -> ApiError {
        ApiError::permanent(format!("bad thrift response: {e}"))
    }

    pub(crate) async fn thrift_open_session_raw(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Result<thrift::SessionHandle, ApiError> {
        let namespace = if catalog.is_some() || schema.is_some() {
            Some(thrift::Namespace {
                catalog_name: catalog,
                schema_name: schema,
            })
        } else {
            None
        };
        let req = thrift::OpenSessionReq {
            namespace,
            configuration: &[("spark.thriftserver.arrowBasedRowSet.timestampAsString", "false")],
        };
        let body = Bytes::from(thrift::build_open_session(&req));
        // Not idempotent: an ambiguous failure here may have already
        // created a session server-side; blindly retrying would leak it
        // (harmless correctness-wise, but not "safe to replay" in the
        // sense this flag means elsewhere in this file).
        let resp_bytes = self.thrift_call(body, false, None).await?;
        let resp = thrift::parse_open_session(&resp_bytes).map_err(Self::thrift_parse_error)?;
        if let Some(e) = resp.status.error() {
            return Err(ApiError::permanent(format!("Thrift OpenSession failed: {e}")));
        }
        resp.session_handle
            .ok_or_else(|| ApiError::permanent("Thrift OpenSession succeeded with no sessionHandle".to_string()))
    }

    pub(crate) async fn thrift_close_session_raw(&self, session: &thrift::SessionHandle) {
        let body = Bytes::from(thrift::build_close_session(session));
        // Best-effort, same as SEA's `delete_session` -- a failed close just
        // leaves the session for Databricks' own server-side TTL to reap.
        let _ = self.thrift_call(body, true, None).await;
    }

    /// Same checkout contract as SEA's `checkout_session` (see
    /// `thrift_session_pool`'s doc comment): `None` means the caller must
    /// open its own throwaway session for this one call (Thrift has no
    /// session-less submission mode to fall back to).
    pub(crate) async fn thrift_checkout_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Option<thrift::SessionHandle> {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        if let Some(handle) = self.thrift_session_pool.take(&key) {
            return Some(handle);
        }
        if !self.thrift_session_pool.reserve(&key) {
            return None;
        }
        match self.thrift_open_session_raw(catalog, schema).await {
            Ok(handle) => Some(handle),
            Err(_) => {
                self.thrift_session_pool.release(&key);
                None
            }
        }
    }

    pub(crate) fn thrift_checkin_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session: thrift::SessionHandle,
        keep: bool,
    ) {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        self.thrift_session_pool.checkin(key, session, keep);
    }

    /// Best-effort close of every currently-idle pooled Thrift session --
    /// same contract as `close_all_sessions` (SEA).
    pub async fn close_all_thrift_sessions(&self) {
        for s in self.thrift_session_pool.drain_idle() {
            self.thrift_close_session_raw(&s).await;
        }
    }

    pub(crate) async fn thrift_execute_statement_raw(
        &self,
        session: &thrift::SessionHandle,
        statement: &str,
        parameters: Option<&Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::ExecuteStatementResp, ApiError> {
        let params_vec = parameters.map(thrift::parameters_from_json).unwrap_or_default();
        let req = thrift::ExecuteStatementReq {
            session_handle: session,
            statement,
            can_decompress_lz4: self.compress_results,
            direct_results_max_rows: THRIFT_DIRECT_RESULTS_MAX_ROWS,
            direct_results_max_bytes: THRIFT_DIRECT_RESULTS_MAX_BYTES,
            parameters: &params_vec,
        };
        let body = Bytes::from(thrift::build_execute_statement(&req));
        // Not idempotent -- same double-execution risk as SEA's
        // statement-submit POST (arbitrary caller SQL, e.g. INSERT/MERGE).
        let resp_bytes = self.thrift_call(body, false, Some(stats)).await?;
        let resp = thrift::parse_execute_statement(&resp_bytes).map_err(Self::thrift_parse_error)?;
        if let Some(e) = resp.status.error() {
            return Err(ApiError::permanent(format!("Thrift ExecuteStatement failed: {e}")));
        }
        Ok(resp)
    }

    pub(crate) async fn thrift_get_operation_status_raw(
        &self,
        op: &thrift::OperationHandle,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::OperationStatusResp, ApiError> {
        let body = Bytes::from(thrift::build_get_operation_status(op));
        let resp_bytes = self.thrift_call(body, true, Some(stats)).await?;
        thrift::parse_get_operation_status(&resp_bytes).map_err(Self::thrift_parse_error)
    }

    pub(crate) async fn thrift_fetch_results_raw(
        &self,
        op: &thrift::OperationHandle,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::FetchResultsResp, ApiError> {
        let body = Bytes::from(thrift::build_fetch_results(
            op,
            THRIFT_DIRECT_RESULTS_MAX_ROWS,
            THRIFT_DIRECT_RESULTS_MAX_BYTES,
        ));
        // Not idempotent -- see `thrift_call`'s own doc comment: this
        // advances a server-side cursor (`orientation: FETCH_NEXT`).
        let resp_bytes = self.thrift_call(body, false, Some(stats)).await?;
        thrift::parse_fetch_results(&resp_bytes).map_err(Self::thrift_parse_error)
    }

    /// Best-effort -- mirrors `delete_session`'s reasoning: a failed close
    /// just leaves the operation for Databricks' own server-side cleanup.
    pub(crate) async fn thrift_close_operation_best_effort(&self, op: &thrift::OperationHandle) {
        let body = Bytes::from(thrift::build_close_operation(op));
        let _ = self.thrift_call(body, true, None).await;
    }
}
