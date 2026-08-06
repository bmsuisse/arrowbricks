//! End-to-end proof over a real local HTTP server (wiremock), mirroring
//! `tests/conftest.py`'s `mock_warehouse` fixture: warehouse status, statement
//! submit, chunk-link resolution, and external-link byte download. Reorder
//! correctness itself is unit-tested in `pipeline.rs`; this only proves the
//! HTTP submit->poll->fetch->decode path is wired correctly end to end.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrowbricks_core::client::DbClient;
use arrowbricks_core::pipeline::{execute_lazy, run_json_pipeline, run_pipeline};
use serde_json::json;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const WAREHOUSE_ID: &str = "wh-test-123";
const STATEMENT_ID: &str = "stmt-abc";

fn build_chunk_bytes(lo: i64, hi: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (lo..hi).collect();
    let labels: Vec<String> = ids.iter().map(|i| format!("row_{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(labels))],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

async fn install_mock_warehouse(server: &MockServer, n_chunks: i64, rows_per_chunk: i64, delay_reverse: bool) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;

    let chunks: Vec<_> = (0..n_chunks)
        .map(|i| json!({"chunk_index": i, "row_count": rows_per_chunk}))
        .collect();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": chunks,
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
        })))
        .mount(server)
        .await;

    for i in 0..n_chunks {
        let uri = server.uri();
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{i}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [{"external_link": format!("{uri}/_data/chunk-{i}")}]
            })))
            .mount(server)
            .await;

        let bytes = build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk);
        let delay_ms = if delay_reverse { ((n_chunks - i) * 10) as u64 } else { 0 };
        Mock::given(method("GET"))
            .and(path_regex(format!(r"^/_data/chunk-{i}$")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(bytes, "application/vnd.apache.arrow.stream")
                    .set_delay(std::time::Duration::from_millis(delay_ms)),
            )
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn execute_statement_requests_lz4_frame_compression() {
    let server = MockServer::start().await;
    let captured_body: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(None));

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let captured_for_responder = captured_body.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            *captured_for_responder.lock().unwrap() = Some(req.body.clone());
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT_ID,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": []},
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    let body = captured_body
        .lock()
        .unwrap()
        .take()
        .expect("statement submission never captured");
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        parsed["result_compression"], "LZ4_FRAME",
        "expected the submitted statement to request LZ4_FRAME cloud-fetch compression, got: {parsed}"
    );
}

#[tokio::test]
async fn execute_statement_omits_compression_when_disabled() {
    let server = MockServer::start().await;
    let captured_body: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(None));

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let captured_for_responder = captured_body.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            *captured_for_responder.lock().unwrap() = Some(req.body.clone());
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT_ID,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": []},
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_compress_results(false));
    run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    let body = captured_body
        .lock()
        .unwrap()
        .take()
        .expect("statement submission never captured");
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        parsed.get("result_compression").is_none(),
        "expected no result_compression field when compress_results=False, got: {parsed}"
    );
}

/// Regression test for the SEA "embedded first chunk" optimization: a real
/// workspace's SUCCEEDED statement response already carries chunk 0's
/// presigned URL directly in `result.external_links`, so the client must
/// skip that chunk's own `GET .../result/chunks/0` resolution call entirely
/// and go straight to downloading the blob. `.expect(0)` on that mock fails
/// the test (on `MockServer` drop) if it's ever hit -- proves the round trip
/// is actually skipped, not just that the pipeline still produces correct
/// rows some other way. Chunk 1 has no embedded link, so it must still go
/// through the normal resolution path -- proves both branches coexist
/// correctly in the same statement.
#[tokio::test]
async fn pre_resolved_chunk0_link_skips_the_extra_resolution_get() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [{"chunk_index": 0, "row_count": 5}, {"chunk_index": 1, "row_count": 5}],
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
            "result": {
                "external_links": [{"chunk_index": 0, "external_link": format!("{uri}/_data/chunk-0")}]
            },
        })))
        .mount(&server)
        .await;

    // Never mounted for chunk 0 -- `pre_resolved_link` must mean the fetch
    // worker never even attempts this request.
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"external_links": []})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/1")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links": [{"external_link": format!("{uri}/_data/chunk-1")}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-0$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-1$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(5, 10), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.num_chunks, 2);
    assert_eq!(summary.num_rows(), 10);
    assert_ids_in_order(&summary.batches, 10);
}

/// Regression test for a coverage gap found in code review: the test above
/// only proves the pre-resolved-link skip through `run_pipeline`, which
/// became unreachable from Python once `Client.execute_arrow` was removed
/// as unused surface -- `execute_lazy`/`ResultStream` (backing
/// `Cursor.fetchall_arrow`, the path every real caller actually takes) is a
/// separate implementation that had zero coverage of this specific
/// optimization. Same mock setup (including the `.expect(0)` proving the
/// resolution GET is genuinely skipped, not just that the pipeline still
/// produces correct rows some other way), the actually-used path.
#[tokio::test]
async fn lazy_pipeline_skips_the_extra_resolution_get_for_a_pre_resolved_chunk() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [{"chunk_index": 0, "row_count": 5}, {"chunk_index": 1, "row_count": 5}],
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
            "result": {
                "external_links": [{"chunk_index": 0, "external_link": format!("{uri}/_data/chunk-0")}]
            },
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"external_links": []})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/1")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links": [{"external_link": format!("{uri}/_data/chunk-1")}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-0$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-1$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(5, 10), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);
    assert_ids_in_order(&batches, 10);
}

/// Regression test for a bug caught in code review before it shipped:
/// `pre_resolved` was originally a plain `HashMap<i64, String>`, which keeps
/// only the *last* entry when `result.external_links` has more than one for
/// the same `chunk_index` (a real, supported shape -- `fetch_chunk_index`'s
/// own resolution path already returns `Vec<Bytes>` per chunk for exactly
/// this reason). Two links for chunk_index 0 here -- if the fix regresses
/// back to losing all but the last, this comes back with 5 rows instead of
/// 10.
#[tokio::test]
async fn pre_resolved_links_supports_multiple_links_for_the_same_chunk_index() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [{"chunk_index": 0, "row_count": 10}],
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
            "result": {
                "external_links": [
                    {"chunk_index": 0, "external_link": format!("{uri}/_data/chunk-0a")},
                    {"chunk_index": 0, "external_link": format!("{uri}/_data/chunk-0b")},
                ]
            },
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"external_links": []})))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-0a$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-0b$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(5, 10), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(
        summary.num_rows(),
        10,
        "both pre-resolved links for chunk_index 0 must be fetched, not just the last one"
    );
}

/// Regression test for a bug caught in code review before it shipped: an
/// omitempty-style server serializer could drop a zero-valued `chunk_index`
/// field entirely rather than emit `0` -- exactly chunk 0, the chunk this
/// optimization targets most. Before `chunk_index` had `#[serde(default)]`,
/// this would either fail the whole `StatementResponseBody` parse (turning
/// an optional fast path into a hard failure for the entire statement) or
/// -- depending on the exact fix -- misattribute the link to the wrong
/// chunk. Omitting `chunk_index` here must still resolve to chunk 0.
#[tokio::test]
async fn pre_resolved_link_with_omitted_chunk_index_defaults_to_chunk_zero() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [{"chunk_index": 0, "row_count": 5}],
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
            // "chunk_index" deliberately omitted, not set to 0.
            "result": {
                "external_links": [{"external_link": format!("{uri}/_data/chunk-0")}]
            },
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"external_links": []})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/chunk-0$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.num_rows(), 5);
    assert_ids_in_order(&summary.batches, 5);
}

#[tokio::test]
async fn full_pipeline_happy_path() {
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 3, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.statement_id, STATEMENT_ID);
    assert_eq!(summary.num_chunks, 3);
    assert_eq!(summary.num_batches(), 3);
    assert_eq!(summary.num_rows(), 15);
    assert_ids_in_order(&summary.batches, 15);
}

#[tokio::test]
async fn full_pipeline_survives_reverse_arrival() {
    let server = MockServer::start().await;
    // Later chunk indices resolve *faster* than earlier ones, forcing
    // genuine out-of-order completion over real async I/O timing.
    install_mock_warehouse(&server, 5, 4, true).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_concurrency(4));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.num_chunks, 5);
    assert_eq!(summary.num_batches(), 5);
    assert_eq!(summary.num_rows(), 20);
    // The real payoff of the reorder buffer: despite chunks completing out
    // of (chunk_index) order over real async I/O, the assembled row data
    // itself comes back in the original 0..20 order, not arrival order.
    assert_ids_in_order(&summary.batches, 20);
}

#[tokio::test]
async fn lazy_fetchmany_never_pulls_more_chunks_than_consumed() {
    let server = MockServer::start().await;
    // 5 rows/chunk, requested in batches of 7 -- deliberately misaligned
    // with the chunk boundary so a `fetchmany` call must sometimes split a
    // batch mid-chunk (RecordBatch::slice) and buffer the remainder for the
    // next call.
    install_mock_warehouse(&server, 6, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    assert_eq!(stream.num_chunks, 6);

    let mut all_batches = Vec::new();
    let mut total_rows = 0;
    loop {
        let (batches, _schema) = stream.fetchmany_arrow(7).await.unwrap();
        let got: usize = batches.iter().map(|b| b.num_rows()).sum();
        if got == 0 {
            break;
        }
        assert!(got <= 7, "fetchmany_arrow(7) returned more than asked for: {got}");
        total_rows += got;
        all_batches.extend(batches);
    }

    assert_eq!(total_rows, 30);
    assert_ids_in_order(&all_batches, 30);
}

#[tokio::test]
async fn lazy_fetchall_drains_beyond_the_per_batch_chunk_cap() {
    // Regression test: fetch_at_least pulls chunks in bounded batches
    // (MAX_CHUNKS_PER_FETCH_BATCH = 32 per round) -- a single round isn't
    // enough to satisfy a request spanning more chunks than that. An
    // earlier version of this code only ran one bounded batch per
    // fetch_at_least call, so fetchall_arrow() on a result with more than
    // 32 chunks silently returned just the first 32 chunks' worth of rows
    // instead of everything. 50 chunks here deliberately exceeds that cap.
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 50, 3, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();

    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 150,
        "fetchall_arrow must drain all 50 chunks, not just the first batch-cap's worth"
    );
    assert_ids_in_order(&batches, 150);
}

/// Regression test for a bug found in code review: `fetch_at_least` pulls
/// chunks off the reorder buffer into a *local* `decode_handles` list before
/// appending decoded batches to `self.pending`/`self.pending_rows` -- if the
/// whole `fetchall_arrow()` future is dropped mid-flight (exactly what
/// happens when a Python caller's `asyncio.wait_for`/`task.cancel()` fires,
/// simulated here with a real `tokio::time::timeout` racing a real delayed
/// mock response), whatever was in `decode_handles` at that moment is lost
/// -- already consumed out of the reorder buffer, but never recorded.
/// Before the fix, a second `fetchall_arrow()` call on the *same* stream
/// then silently returned a real but truncated row count with no error at
/// all (fewer rows than the query actually has). The fix poisons the stream
/// on any early exit from `fetch_at_least`, so this second call must now
/// error instead of quietly under-reporting.
#[tokio::test]
async fn lazy_fetchall_errors_instead_of_silently_truncating_after_a_cancelled_fetch() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    let n_chunks = 4i64;
    let rows_per_chunk = 5i64;
    let chunks: Vec<_> = (0..n_chunks)
        .map(|i| json!({"chunk_index": i, "row_count": rows_per_chunk}))
        .collect();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"chunks": chunks},
        })))
        .mount(&server)
        .await;

    for i in 0..n_chunks {
        let uri = server.uri();
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{i}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [{"external_link": format!("{uri}/_data/chunk-{i}")}]
            })))
            .mount(&server)
            .await;

        let bytes = build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk);
        // Every chunk is slow -- with concurrency capped at 1 below, this
        // guarantees the short timeout below fires mid-fetch (after at most
        // one chunk has landed), not before anything started or after
        // everything already finished.
        Mock::given(method("GET"))
            .and(path_regex(format!(r"^/_data/chunk-{i}$")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(bytes, "application/vnd.apache.arrow.stream")
                    .set_delay(std::time::Duration::from_millis(80)),
            )
            .mount(&server)
            .await;
    }

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_concurrency(1));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();

    // Real cancellation, not a simulated flag -- `timeout` drops the
    // `fetchall_arrow()` future the instant it elapses, exactly like
    // pyo3-async-runtimes propagating a Python-side `task.cancel()` into the
    // Rust future it wraps.
    let first = tokio::time::timeout(std::time::Duration::from_millis(20), stream.fetchall_arrow()).await;
    assert!(
        first.is_err(),
        "expected the first fetchall_arrow() to be cancelled by the short timeout"
    );

    let second = stream.fetchall_arrow().await;
    assert!(
        second.is_err(),
        "a second fetchall_arrow() on the same stream after a cancelled fetch must error, not silently return a truncated result: {second:?}"
    );
}

#[tokio::test]
async fn lazy_fetchmany_survives_reverse_arrival() {
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 5, 4, true).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_concurrency(4));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();

    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20);
    assert_ids_in_order(&batches, 20);
}

async fn install_mock_warehouse_json(server: &MockServer, n_chunks: i64, rows_per_chunk: i64, delay_reverse: bool) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;

    let chunks: Vec<_> = (0..n_chunks)
        .map(|i| json!({"chunk_index": i, "row_count": rows_per_chunk}))
        .collect();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"chunks": chunks},
        })))
        .mount(server)
        .await;

    for i in 0..n_chunks {
        let uri = server.uri();
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{i}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [{"external_link": format!("{uri}/_data/chunk-{i}")}]
            })))
            .mount(server)
            .await;

        // JSON_ARRAY's own contract: each row is an array of *strings*
        // (nulls stay null) regardless of real column type -- not this
        // crate's choice, Databricks'.
        let rows: Vec<_> = (i * rows_per_chunk..(i + 1) * rows_per_chunk)
            .map(|id| json!([id.to_string(), format!("row_{id}")]))
            .collect();
        let delay_ms = if delay_reverse { ((n_chunks - i) * 10) as u64 } else { 0 };
        Mock::given(method("GET"))
            .and(path_regex(format!(r"^/_data/chunk-{i}$")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(rows)
                    .set_delay(std::time::Duration::from_millis(delay_ms)),
            )
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn json_pipeline_happy_path() {
    let server = MockServer::start().await;
    install_mock_warehouse_json(&server, 3, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let result = run_json_pipeline(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();

    assert_eq!(result.statement_id, STATEMENT_ID);
    assert_eq!(result.num_chunks, 3);
    assert_eq!(result.rows.len(), 15);
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| r[0].as_ref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(ids, (0..15).collect::<Vec<_>>());
}

#[tokio::test]
async fn json_pipeline_survives_reverse_arrival() {
    let server = MockServer::start().await;
    install_mock_warehouse_json(&server, 5, 4, true).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_concurrency(4));
    let result = run_json_pipeline(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 20);
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| r[0].as_ref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(
        ids,
        (0..20).collect::<Vec<_>>(),
        "row order must survive out-of-order chunk arrival for JSON too"
    );
}

/// Splits `data` into `frame_size`-byte pieces and LZ4-frame-compresses each
/// independently, concatenating the results -- matches what a real
/// Databricks warehouse actually sends (confirmed against a live workspace:
/// a single chunk's compressed bytes were 18 separate concatenated frames,
/// not one frame wrapping the whole payload). A small `frame_size` here
/// exercises the same multi-frame path through the real fetch_link_bytes
/// (not just the isolated client.rs unit test), which the single-frame
/// version of this helper never did.
fn compress_lz4_frame_multi(data: &[u8], frame_size: usize) -> Vec<u8> {
    use std::io::Write;
    let mut out = Vec::new();
    for piece in data.chunks(frame_size.max(1)) {
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(piece).unwrap();
        out.extend(encoder.finish().unwrap());
    }
    out
}

/// Same shape as `install_mock_warehouse`, but the manifest echoes back
/// `result_compression: "LZ4_FRAME"` (confirming the server honored our
/// request, see `execute_statement`) and each chunk's bytes are actually
/// LZ4-frame-compressed on the wire, exactly as a real Databricks warehouse
/// would send them with cloud-fetch compression enabled -- proves
/// `decompress_lz4_frame` in client.rs round-trips real data, not just that
/// the plumbing compiles.
async fn install_mock_warehouse_compressed(server: &MockServer, n_chunks: i64, rows_per_chunk: i64) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;

    let chunks: Vec<_> = (0..n_chunks)
        .map(|i| json!({"chunk_index": i, "row_count": rows_per_chunk}))
        .collect();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": chunks,
                "result_compression": "LZ4_FRAME",
                "schema": {"columns": [
                    {"name": "id", "type_name": "LONG"},
                    {"name": "label", "type_name": "STRING"},
                ]},
            },
        })))
        .mount(server)
        .await;

    for i in 0..n_chunks {
        let uri = server.uri();
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{i}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [{"external_link": format!("{uri}/_data/chunk-{i}")}]
            })))
            .mount(server)
            .await;

        let bytes = build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk);
        // Deliberately tiny -- forces several frames out of a small test
        // payload, exercising the same multi-frame path a real (much
        // larger) chunk takes.
        let compressed = compress_lz4_frame_multi(&bytes, 64);
        Mock::given(method("GET"))
            .and(path_regex(format!(r"^/_data/chunk-{i}$")))
            .respond_with(ResponseTemplate::new(200).set_body_raw(compressed, "application/octet-stream"))
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn compressed_pipeline_decompresses_lz4_frame_chunks() {
    let server = MockServer::start().await;
    install_mock_warehouse_compressed(&server, 3, 5).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.num_chunks, 3);
    assert_eq!(summary.num_rows(), 15);
    assert_ids_in_order(&summary.batches, 15);
}

/// Regression test for a coverage gap found in code review: `run_pipeline`
/// (used by the test above) became unreachable from Python once
/// `Client.execute_arrow` was removed as unused surface (nothing in the
/// shipped package called it) -- `execute_lazy`/`ResultStream` (backing
/// `Cursor.fetchall_arrow`, the path every real caller actually takes) is a
/// separate implementation that happened to have zero integration coverage
/// of LZ4 decompression at all. Same mock setup, the actually-used path.
#[tokio::test]
async fn lazy_pipeline_decompresses_lz4_frame_chunks() {
    let server = MockServer::start().await;
    install_mock_warehouse_compressed(&server, 3, 5).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 15);
    assert_ids_in_order(&batches, 15);
}

/// Concatenates every batch's `id` column and checks it runs 0..n_rows in
/// order -- proves the reorder buffer's effect on the *actual data*, not
/// just on chunk counts.
fn assert_ids_in_order(batches: &[RecordBatch], n_rows: i64) {
    let mut ids = Vec::with_capacity(n_rows as usize);
    for batch in batches {
        let col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(col.values().iter().copied());
    }
    assert_eq!(ids, (0..n_rows).collect::<Vec<_>>());
}
