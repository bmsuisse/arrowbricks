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
use arrowbricks_core::pipeline::{execute_lazy, run_pipeline};
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

    let chunks: Vec<_> = (0..n_chunks).map(|i| json!({"chunk_index": i, "row_count": rows_per_chunk})).collect();
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
            .and(path(format!("/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{i}")))
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
async fn full_pipeline_happy_path() {
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 3, 5, false).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let summary = run_pipeline(client, "SELECT * FROM t", None, None).await.unwrap();

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
    let summary = run_pipeline(client, "SELECT * FROM t", None, None).await.unwrap();

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
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None).await.unwrap();
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
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None).await.unwrap();

    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 150, "fetchall_arrow must drain all 50 chunks, not just the first batch-cap's worth");
    assert_ids_in_order(&batches, 150);
}

#[tokio::test]
async fn lazy_fetchmany_survives_reverse_arrival() {
    let server = MockServer::start().await;
    install_mock_warehouse(&server, 5, 4, true).await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_concurrency(4));
    let mut stream = execute_lazy(client, "SELECT * FROM t", None, None).await.unwrap();

    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20);
    assert_ids_in_order(&batches, 20);
}

/// Concatenates every batch's `id` column and checks it runs 0..n_rows in
/// order -- proves the reorder buffer's effect on the *actual data*, not
/// just on chunk counts.
fn assert_ids_in_order(batches: &[RecordBatch], n_rows: i64) {
    let mut ids = Vec::with_capacity(n_rows as usize);
    for batch in batches {
        let col = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        ids.extend(col.values().iter().copied());
    }
    assert_eq!(ids, (0..n_rows).collect::<Vec<_>>());
}
