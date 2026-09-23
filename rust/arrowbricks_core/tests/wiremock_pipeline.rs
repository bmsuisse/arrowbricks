//! End-to-end proof over a real local HTTP server (wiremock), mirroring
//! `tests/conftest.py`'s `mock_warehouse` fixture: warehouse status, statement
//! submit, chunk-link resolution, and external-link byte download. Reorder
//! correctness itself is unit-tested in `pipeline.rs`; this only proves the
//! HTTP submit->poll->fetch->decode path is wired correctly end to end.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_array::{Int64Array, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrowbricks_core::client::{DbClient, MAX_SESSIONS_PER_KEY, Protocol, QueryStatsAccumulator};
use arrowbricks_core::heartbeat::{HeartbeatWait, Tick};
use arrowbricks_core::pipeline::{cancel_hook, execute_lazy, execute_lazy_prefer_inline, run_pipeline};
use common::wait_for_calls;
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(
        DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token")
            .with_compress_results(false)
            .with_protocol(Protocol::Sea),
    );
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);
    assert_ids_in_order(&batches, 10);
}

/// Regression test for the same "server can over-deliver past its declared
/// row count" behavior Thrift's resultLinks/arrowBatches paths already
/// guard against (see AGENTS.md) -- found missing on this SEA path during a
/// later review pass, not yet observed to actually trigger against a real
/// warehouse (unlike the Thrift case, which was), but structurally
/// identical and cheap to close defensively: the manifest declares 3 rows
/// for chunk_index 0, the actual file backing it encodes 5. A single link
/// per chunk_index is the common case where `fetch_chunks_with_backpressure`
/// now passes the declared count through as `ChunkItem::truncate_to`.
#[tokio::test]
async fn lazy_pipeline_truncates_a_sea_chunk_that_overshoots_its_declared_row_count() {
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
                "chunks": [{"chunk_index": 0, "row_count": 3}],
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
        .and(path_regex(r"^/_data/chunk-0$"))
        .respond_with(
            // Declares 3 rows above, but the file itself has 5 -- must be
            // sliced down to 3, not returned in full.
            ResponseTemplate::new(200).set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream"),
        )
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "must keep exactly the manifest's declared row count, not however many rows the file actually encoded"
    );
    assert_ids_in_order(&batches, 3);
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None, None).await.unwrap();

    assert_eq!(summary.num_rows(), 5);
    assert_ids_in_order(&summary.batches, 5);
}

#[tokio::test]
async fn full_pipeline_happy_path() {
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 3, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(
        DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token")
            .with_concurrency(4)
            .with_protocol(Protocol::Sea),
    );
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(
        DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token")
            .with_concurrency(1)
            .with_protocol(Protocol::Sea),
    );
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

    let client = Arc::new(
        DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token")
            .with_concurrency(4)
            .with_protocol(Protocol::Sea),
    );
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();

    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20);
    assert_ids_in_order(&batches, 20);
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
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

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None, None).await.unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 15);
    assert_ids_in_order(&batches, 15);
}

/// Regression test for the `prefer_inline` fast path: a SUCCEEDED response
/// carrying `result.data_array` directly (no `external_links`, no
/// `manifest.chunks`) must be converted straight to Arrow with *zero*
/// further HTTP calls -- `.expect(0)` on both the chunk-resolution and
/// blob-fetch mocks fails the test if either is ever hit, proving the round
/// trip is genuinely skipped, not just that the data happens to come out
/// right some other way.
#[tokio::test]
async fn prefer_inline_uses_the_embedded_data_array_with_zero_further_requests() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            assert_eq!(parsed["disposition"], "INLINE");
            assert_eq!(parsed["format"], "JSON_ARRAY");
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT_ID,
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "chunks": [],
                    "schema": {"columns": [
                        {"name": "id", "type_name": "LONG"},
                        {"name": "label", "type_name": "STRING"},
                    ]},
                },
                "result": {
                    "data_array": [["0", "row_0"], ["1", "row_1"], ["2", "row_2"]],
                },
            }))
        })
        .mount(&server)
        .await;

    // Never mounted a real handler for either -- `.expect(0)` fails the
    // test (on `MockServer` drop) if a request ever arrives here.
    Mock::given(method("GET"))
        .and(path_regex(format!(
            r"^/api/2\.0/sql/statements/{STATEMENT_ID}/result/chunks/.*$"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"external_links": []})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/.*$"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let mut stream = execute_lazy_prefer_inline(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    assert_ids_in_order(&batches, 3);
}

/// Matches only a POST whose JSON body has `"disposition": "INLINE"` --
/// lets a mock built with this matcher coexist with `install_mock_warehouse`'s
/// own unconditional POST /statements mock (mounted separately) rather than
/// depending on wiremock's mount-order/priority rules (default priority is
/// equal for both, and ties go to whichever was mounted *first* -- the
/// opposite of what an earlier version of these two tests assumed, which is
/// why they need this matcher instead of a response-closure "fall through"
/// that wiremock doesn't actually support that way).
struct HasDisposition(&'static str);

impl wiremock::Match for HasDisposition {
    fn matches(&self, request: &wiremock::Request) -> bool {
        serde_json::from_slice::<serde_json::Value>(&request.body)
            .ok()
            .and_then(|v| v.get("disposition").and_then(|d| d.as_str().map(str::to_string)))
            .as_deref()
            == Some(self.0)
    }
}

/// Regression test: exceeding INLINE's byte limit must fall back to a
/// **fresh** EXTERNAL_LINKS submission (a distinct statement execution, not
/// a retry -- see `execute_arrow_statement_prefer_inline`'s doc comment),
/// and still return the correct, complete result via that normal path.
///
/// **Mount order bug found and fixed alongside the prefer_inline
/// resubmission fix above (2026-08-11):** this test used to call
/// `install_mock_warehouse` (whose own POST /statements mock matches
/// unconditionally, no `HasDisposition` filter) *before* mounting the
/// INLINE-tagged mock below -- per `MountedMockSet::handle_request`'s own
/// stable sort by priority (confirmed directly in wiremock 0.6.5's source,
/// `mock_set.rs`), a tie between two equally-matching mocks goes to
/// whichever was *mounted first*. That meant `install_mock_warehouse`'s
/// generic mock -- not the INLINE-tagged one -- actually won the very first
/// (INLINE) submission too, since it matches unconditionally and was
/// mounted first; the "byte limit exceeded" response below was never
/// actually served. This test still passed regardless, for the wrong
/// reason: the *old*, unsafe `None => resubmit` fallback this session
/// removed from `execute_arrow_statement_prefer_inline` (see that
/// function's own doc comment) silently caught the resulting "SUCCEEDED
/// with no data_array" case and resubmitted anyway, landing on the same
/// generic mock a second time and coincidentally producing the same
/// correct-looking 15-row result via a completely untested code path.
/// Removing that unsafe fallback surfaced this mount-order bug immediately
/// (a hard `unwrap()` panic, not a silent pass) -- fixed by mounting the
/// INLINE-tagged mock *first*, so it correctly wins the tie for the first
/// request; `install_mock_warehouse`'s generic mock then only ever matches
/// the *second* (EXTERNAL_LINKS, no `"disposition":"INLINE"` in the body)
/// submission, which the INLINE-tagged mock's own `HasDisposition` filter
/// no longer matches at all -- no more tie to break.
#[tokio::test]
async fn prefer_inline_falls_back_to_external_links_on_byte_limit_exceeded() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(HasDisposition("INLINE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-inline-too-big",
            "status": {
                "state": "FAILED",
                "error": {
                    "error_code": "BAD_REQUEST",
                    "message": "Inline byte limit exceeded. Statements executed with disposition=INLINE can have a result size of at most 26214400 bytes. Please execute the statement with disposition=EXTERNAL_LINKS if you want to download the full result.",
                },
            },
        })))
        .mount(&server)
        .await;
    // Mounted *after* the INLINE-tagged mock above -- see this test's own
    // doc comment for why the order matters. Its unconditional POST
    // /statements matcher only ever gets a chance to serve the *second*
    // (EXTERNAL_LINKS) submission now.
    install_mock_warehouse(&server, 3, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let mut stream = execute_lazy_prefer_inline(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 15,
        "must fall back to the normal EXTERNAL_LINKS path and still return everything"
    );
    assert_ids_in_order(&batches, 15);
}

/// Regression test for a real data-safety bug found in code review
/// (2026-08-11): an INLINE response that already reached SUCCEEDED, whose
/// schema contains a column type `json_convert` doesn't handle (ARRAY,
/// here), must **not** silently resubmit the identical SQL as a fresh
/// EXTERNAL_LINKS statement -- that statement already ran and (for
/// non-idempotent SQL) may have already committed a write; resubmitting it
/// would duplicate that write with nothing surfaced to the caller. Unlike
/// `prefer_inline_falls_back_to_external_links_on_byte_limit_exceeded`
/// below (a genuinely safe fallback, since that statement reached FAILED,
/// not SUCCEEDED -- nothing committed), this must return a clear `Err`
/// instead. Only the INLINE-tagged mock is mounted (no generic/EXTERNAL_LINKS
/// fallback route at all) with `.expect(1)` -- if the fixed code ever
/// resubmitted again, this test would fail on that expectation with no
/// matching mock for the second request, not just on the wrong return type.
#[tokio::test]
async fn prefer_inline_on_unsupported_column_type_errors_instead_of_resubmitting() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(HasDisposition("INLINE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-inline-unsupported",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [],
                "schema": {"columns": [{"name": "arr", "type_name": "ARRAY"}]},
            },
            "result": {"data_array": [["[\"1\",\"2\"]"]]},
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    // Not `.expect_err(...)` -- `ResultStream` (the `Ok` side) doesn't
    // implement `Debug`.
    let Err(err) = execute_lazy_prefer_inline(client, "INSERT INTO t VALUES (1)", None, None, None).await else {
        panic!("a JSON-conversion failure after SUCCEEDED must error, not silently resubmit the statement");
    };

    assert!(
        err.message.contains("stmt-inline-unsupported") && err.message.to_lowercase().contains("re-run"),
        "error should name the statement and explain why it isn't being re-run: {}",
        err.message
    );
}

/// Companion regression test for the identical bug fixed one layer down, in
/// `DbClient::execute_arrow_statement_prefer_inline` itself: a SUCCEEDED
/// INLINE response with no `result.data_array` at all (a defensive,
/// shouldn't-normally-happen case -- see that match arm's own doc comment)
/// used to fall back to a fresh EXTERNAL_LINKS resubmission the same
/// unsafe way the unsupported-column-type case above did. Only the
/// INLINE-tagged mock is mounted, `.expect(1)`, so a regression back to
/// resubmitting would fail this test on the missing second mock, not just
/// on the wrong `Result` variant.
#[tokio::test]
async fn prefer_inline_on_missing_data_array_errors_instead_of_resubmitting() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(HasDisposition("INLINE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-inline-no-data-array",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"chunks": [], "schema": {"columns": []}},
            "result": {},
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea);
    let stats = QueryStatsAccumulator::default();
    // Not `.expect_err(...)` -- `InlineOrExternal` (the `Ok` side) doesn't
    // implement `Debug`.
    let Err(err) = client
        .execute_arrow_statement_prefer_inline("INSERT INTO t VALUES (1)", None, None, None, &stats)
        .await
    else {
        panic!("a SUCCEEDED INLINE response with no data_array must error, not silently resubmit");
    };

    assert!(
        err.message.contains("stmt-inline-no-data-array") && err.message.to_lowercase().contains("re-run"),
        "error should name the statement and explain why it isn't being re-run: {}",
        err.message
    );
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

async fn mount_warehouse_running(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;
}

#[tokio::test]
async fn session_is_created_once_and_reused_across_sequential_statements() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;

    let session_calls = Arc::new(AtomicUsize::new(0));
    let session_calls_for_mock = session_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({"session_id": format!("sess-{n}")}))
        })
        .mount(&server)
        .await;

    let submitted_bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_for_mock = submitted_bodies.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            submitted_for_mock.lock().unwrap().push(parsed);
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT_ID,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": []},
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    run_pipeline(client.clone(), "SELECT 1", Some("cat1"), None, None)
        .await
        .unwrap();
    run_pipeline(client, "SELECT 2", Some("cat1"), None, None)
        .await
        .unwrap();

    assert_eq!(
        session_calls.load(Ordering::SeqCst),
        1,
        "a second statement on the same (catalog, schema) key must reuse the pooled session, not create a new one"
    );
    let bodies = submitted_bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    for body in bodies.iter() {
        assert_eq!(body["session_id"], "sess-0");
        assert!(
            body.get("catalog").is_none(),
            "session_id and catalog must never both be set: {body}"
        );
        assert!(
            body.get("schema").is_none(),
            "session_id and schema must never both be set: {body}"
        );
    }
}

#[tokio::test]
async fn session_creation_failure_falls_back_to_catalog_on_the_statement_body() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;
    // Deliberately no mock for POST /api/2.0/sql/sessions -- wiremock 404s
    // any unmatched request, exercising `checkout_session`'s creation-failure
    // fallback exactly the same as a real transient session-service error.

    let submitted_bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_for_mock = submitted_bodies.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            submitted_for_mock.lock().unwrap().push(parsed);
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT_ID,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": []},
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    run_pipeline(client, "SELECT 1", Some("cat1"), Some("sch1"), None)
        .await
        .expect("a failed session creation must fall back transparently, not surface an error");

    let bodies = submitted_bodies.lock().unwrap();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["catalog"], "cat1");
    assert_eq!(bodies[0]["schema"], "sch1");
    assert!(bodies[0].get("session_id").is_none());
}

#[tokio::test]
async fn concurrent_statements_on_the_same_key_each_get_their_own_pooled_session() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;

    let session_calls = Arc::new(AtomicUsize::new(0));
    let session_calls_for_mock = session_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({"session_id": format!("sess-{n}")}))
        })
        .mount(&server)
        .await;

    let submitted_session_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_for_mock = submitted_session_ids.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            if let Some(id) = parsed.get("session_id").and_then(|v| v.as_str()) {
                submitted_for_mock.lock().unwrap().push(id.to_string());
            }
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "statement_id": STATEMENT_ID,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"chunks": []},
                }))
                .set_delay(std::time::Duration::from_millis(80))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move { run_pipeline(client, &format!("SELECT {i}"), Some("cat1"), None, None).await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        session_calls.load(Ordering::SeqCst),
        3,
        "3 genuinely concurrent statements on one key must each get a distinct session, not share one"
    );
    let ids = submitted_session_ids.lock().unwrap();
    assert_eq!(ids.len(), 3);
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        3,
        "no two concurrent statements may have used the same session_id: {ids:?}"
    );
}

#[tokio::test]
async fn session_pool_exhaustion_falls_back_to_session_less_submission() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;

    let session_calls = Arc::new(AtomicUsize::new(0));
    let session_calls_for_mock = session_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({"session_id": format!("sess-{n}")}))
        })
        .mount(&server)
        .await;

    let submitted_bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_for_mock = submitted_bodies.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            submitted_for_mock.lock().unwrap().push(parsed);
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "statement_id": STATEMENT_ID,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"chunks": []},
                }))
                .set_delay(std::time::Duration::from_millis(150))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let n = MAX_SESSIONS_PER_KEY + 1;
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move { run_pipeline(client, &format!("SELECT {i}"), Some("cat1"), None, None).await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        session_calls.load(Ordering::SeqCst),
        MAX_SESSIONS_PER_KEY,
        "must never create more than MAX_SESSIONS_PER_KEY sessions for one key"
    );
    let bodies = submitted_bodies.lock().unwrap();
    assert_eq!(bodies.len(), n);
    let with_session = bodies.iter().filter(|b| b.get("session_id").is_some()).count();
    let with_catalog_fallback = bodies.iter().filter(|b| b.get("catalog").is_some()).count();
    assert_eq!(with_session, MAX_SESSIONS_PER_KEY);
    assert_eq!(
        with_catalog_fallback, 1,
        "the one caller that couldn't get a pooled session must fall back to a plain catalog-bearing submission"
    );
}

#[tokio::test]
async fn a_failed_statement_discards_its_session_instead_of_returning_it_to_the_pool() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;

    let session_calls = Arc::new(AtomicUsize::new(0));
    let session_calls_for_mock = session_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({"session_id": format!("sess-{n}")}))
        })
        .mount(&server)
        .await;

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_for_mock = call_count.clone();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = call_count_for_mock.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(200).set_body_json(json!({
                    "statement_id": "stmt-fail",
                    "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "statement_id": STATEMENT_ID,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"chunks": []},
                }))
            }
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let result = run_pipeline(client.clone(), "not valid sql", Some("cat1"), None, None).await;
    let err = match result {
        Ok(_) => panic!("expected the FAILED statement to surface as an error"),
        Err(e) => e,
    };
    assert!(err.message.contains("SYNTAX_ERROR"));

    run_pipeline(client, "SELECT 1", Some("cat1"), None, None)
        .await
        .unwrap();

    assert_eq!(
        session_calls.load(Ordering::SeqCst),
        2,
        "the session behind a FAILED statement must be discarded, not handed to the next caller on the same key"
    );
}

#[tokio::test]
async fn close_all_sessions_deletes_every_idle_pooled_session() {
    let server = MockServer::start().await;
    mount_warehouse_running(&server).await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"session_id": "sess-0"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"chunks": []},
        })))
        .mount(&server)
        .await;

    let delete_calls: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let delete_calls_for_mock = delete_calls.clone();
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/sess-0"))
        .respond_with(move |req: &wiremock::Request| {
            let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            delete_calls_for_mock.lock().unwrap().push(parsed);
            ResponseTemplate::new(200).set_body_json(json!({}))
        })
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    run_pipeline(client.clone(), "SELECT 1", Some("cat1"), None, None)
        .await
        .unwrap();
    client.close_all_sessions().await;

    let calls = delete_calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "the idle pooled session must be closed exactly once");
    assert_eq!(calls[0]["warehouse_id"], WAREHOUSE_ID);
}

// ============================================================================
// Server-side cancellation (2026-08-11 design doc): `heartbeat::HeartbeatWait`'s
// two trigger points (`tick()`'s own `total_timeout_s` branch, and `Drop`
// while still genuinely in flight) each fire `POST .../cancel` via
// `pipeline::cancel_hook` -- exactly the mechanism `lib.rs`'s
// `PyResultSet::fetchall_arrow_streamed` wires up, but constructed directly
// here so it's testable with no PyO3/Python involved at all. See
// `wiremock_thrift.rs`'s identically-shaped pair of tests for the Thrift
// (`CancelOperation`) side.
// ============================================================================

/// One statement, one deliberately slow (500ms) cloud-fetch chunk -- long
/// enough that either trigger below reliably fires while the download is
/// still genuinely in flight.
async fn mount_slow_single_chunk_statement(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"chunks": [{"chunk_index": 0, "row_count": 5}]},
        })))
        .mount(server)
        .await;

    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links": [{"external_link": format!("{uri}/_data/slow-chunk")}]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/_data/slow-chunk$"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(build_chunk_bytes(0, 5), "application/vnd.apache.arrow.stream")
                .set_delay(std::time::Duration::from_millis(500)),
        )
        .mount(server)
        .await;
}

/// Mounts `POST .../cancel` and hands back a counter of how many times it
/// was actually hit.
async fn mount_cancel_statement_ok(server: &MockServer) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_mock = calls.clone();
    Mock::given(method("POST"))
        .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/cancel")))
        .respond_with(move |_req: &wiremock::Request| {
            calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({}))
        })
        .mount(server)
        .await;
    calls
}

#[tokio::test]
async fn sea_total_timeout_fires_cancel_statement() {
    let server = MockServer::start().await;
    mount_slow_single_chunk_statement(&server).await;
    let cancel_calls = mount_cancel_statement_ok(&server).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let stream = execute_lazy(client.clone(), "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let cancel_handle = stream.cancel_handle.clone();
    let stats = stream.stats.clone();
    let inner = Arc::new(tokio::sync::Mutex::new(stream));
    let fut = {
        let inner = inner.clone();
        async move { inner.lock().await.fetchall_arrow().await }
    };
    let mut wait = HeartbeatWait::with_interval(fut, Some(0.05), std::time::Duration::from_millis(20))
        .with_cancel(cancel_hook(client.clone(), cancel_handle, stats.clone()));

    let mut last_err = None;
    for _ in 0..50 {
        match wait.tick().await {
            Ok(Some(Tick::Heartbeat)) => continue,
            Ok(other) => panic!(
                "must not complete before total_timeout_s fires -- the chunk download is deliberately slow: {other:?}"
            ),
            Err(e) => {
                last_err = Some(e);
                break;
            }
        }
    }
    let err = last_err.expect("total_timeout_s must fire within 50 ticks of a 20ms interval against a 50ms deadline");
    assert!(
        err.message.contains("0.05"),
        "error should mention the configured timeout: {}",
        err.message
    );

    wait_for_calls(&cancel_calls, 1).await;
    assert_eq!(
        cancel_calls.load(Ordering::SeqCst),
        1,
        "POST .../cancel must fire exactly once when total_timeout_s elapses"
    );
    assert_eq!(
        stats.pending_outcome(),
        Some("timeout"),
        "the total_timeout_s trigger must record outcome=timeout, not cancelled"
    );
}

#[tokio::test]
async fn sea_dropping_the_heartbeat_wait_mid_fetch_fires_cancel_statement() {
    let server = MockServer::start().await;
    mount_slow_single_chunk_statement(&server).await;
    let cancel_calls = mount_cancel_statement_ok(&server).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let stream = execute_lazy(client.clone(), "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let cancel_handle = stream.cancel_handle.clone();
    let stats = stream.stats.clone();
    let inner = Arc::new(tokio::sync::Mutex::new(stream));
    let fut = {
        let inner = inner.clone();
        async move { inner.lock().await.fetchall_arrow().await }
    };
    // No `total_timeout_s` at all -- matches `task.cancel()`/`asyncio.wait_for`
    // dropping the *surrounding* coroutine, the second of the two triggers
    // this design covers.
    let mut wait: HeartbeatWait<(Vec<RecordBatch>, Option<SchemaRef>)> = HeartbeatWait::with_interval(
        fut,
        None,
        std::time::Duration::from_millis(20),
    )
    .with_cancel(cancel_hook(client.clone(), cancel_handle, stats.clone()));

    match wait.tick().await.unwrap() {
        Some(Tick::Heartbeat) => {}
        other => panic!("expected a heartbeat while the 500ms chunk download is still in flight: {other:?}"),
    }

    drop(wait);

    wait_for_calls(&cancel_calls, 1).await;
    assert_eq!(
        cancel_calls.load(Ordering::SeqCst),
        1,
        "POST .../cancel must fire exactly once when the wait is dropped mid-fetch"
    );
    assert_eq!(
        stats.pending_outcome(),
        Some("cancelled"),
        "a bare drop (not through tick()'s own timeout branch) must record outcome=cancelled, not timeout"
    );
}

async fn mount_running_warehouse_and_statement(server: &MockServer, state: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/warehouses/{WAREHOUSE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "RUNNING"})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": STATEMENT_ID,
            "status": {"state": state, "error": {"error_code": "BAD_SQL", "message": "nope"}},
        })))
        .mount(server)
        .await;
}

/// Abandoning the submit/poll wait itself (a `total_timeout_s` or a Python
/// `task.cancel()` drops this future mid-poll) must cancel the statement
/// server-side -- `heartbeat.rs`'s hooks don't exist yet at this point.
#[tokio::test]
async fn sea_abandoning_the_submit_poll_wait_fires_cancel_statement() {
    let server = MockServer::start().await;
    mount_running_warehouse_and_statement(&server, "PENDING").await;
    let cancel_calls = mount_cancel_statement_ok(&server).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        execute_lazy(client, "SELECT * FROM t", None, None, None),
    )
    .await;
    assert!(
        result.is_err(),
        "the statement never leaves PENDING, so the wait must time out"
    );

    wait_for_calls(&cancel_calls, 1).await;
    assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sea_terminal_statement_does_not_fire_cancel_statement() {
    let server = MockServer::start().await;
    mount_running_warehouse_and_statement(&server, "FAILED").await;
    let cancel_calls = mount_cancel_statement_ok(&server).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Sea));
    let result = execute_lazy(client, "SELECT * FROM t", None, None, None).await;
    assert!(result.is_err());

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        cancel_calls.load(Ordering::SeqCst),
        0,
        "an already-terminal statement has nothing left to cancel"
    );
}
