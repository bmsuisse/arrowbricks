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

fn compress_lz4_frame(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
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
        let compressed = compress_lz4_frame(&bytes);
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
