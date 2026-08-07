//! End-to-end proof over a real local HTTP server (wiremock) for the
//! `protocol="thrift"` backend -- the Thrift-speaking analogue of
//! `wiremock_pipeline.rs`. Every Thrift RPC hits the exact same HTTP path
//! (`/sql/1.0/warehouses/{id}`, POST only, see `client.rs`'s `thrift_url`),
//! so routing can't use wiremock's `method`/`path` matchers alone --
//! `IsThriftRpc` (below) parses the Thrift message name out of the request
//! body (via `thrift::Reader::read_message_begin`, the same reader
//! `client.rs` itself uses to parse real responses) and matches on that,
//! mirroring this file's sibling's own `HasDisposition` custom matcher for
//! the same "one path, dispatch on body content" problem.
//!
//! Response bytes for every RPC are built directly with `thrift::Writer`
//! (the exact same primitives `thrift.rs` uses to build *requests*, since
//! the wire format is symmetric) -- field IDs below are copied straight out
//! of `thrift.rs`'s own `read_*` functions, not re-derived.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrowbricks_core::client::{DbClient, MAX_SESSIONS_PER_KEY, Protocol};
use arrowbricks_core::pipeline::execute_lazy_thrift;
use arrowbricks_core::thrift::{Reader, Writer, operation_state, status_code, ttype};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const WAREHOUSE_ID: &str = "wh-test-123";
/// Thrift's own `TMessageType::REPLY` -- `thrift.rs` only names `CALL`/
/// `EXCEPTION` (the two it needs to write/branch on), but any non-EXCEPTION
/// value here is accepted by `parse_reply`, which only special-cases
/// `MESSAGE_TYPE_EXCEPTION`.
const MESSAGE_TYPE_REPLY: i32 = 2;

fn thrift_path() -> String {
    format!("/sql/1.0/warehouses/{WAREHOUSE_ID}")
}

// ---- RPC routing: parse the Thrift message name out of the body ----------

struct IsThriftRpc(&'static str);

impl wiremock::Match for IsThriftRpc {
    fn matches(&self, request: &Request) -> bool {
        let mut r = Reader::new(&request.body);
        match r.read_message_begin() {
            Ok((name, _msg_type, _seqid)) => name == self.0,
            Err(_) => false,
        }
    }
}

// ---- Response builders (mock server -> client), using thrift::Writer directly ----

fn wrap_reply(name: &str, write_result: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut w = Writer::new();
    w.write_message_begin(name, MESSAGE_TYPE_REPLY, 0);
    w.write_field_begin(ttype::STRUCT, 0);
    write_result(&mut w);
    w.write_field_stop();
    w.into_bytes()
}

fn write_status_ok(w: &mut Writer) {
    w.write_field_begin(ttype::I32, 1);
    w.write_i32(status_code::SUCCESS);
    w.write_field_stop();
}

fn write_status_error(w: &mut Writer, message: &str) {
    w.write_field_begin(ttype::I32, 1);
    w.write_i32(status_code::ERROR);
    w.write_field_begin(ttype::STRING, 6); // displayMessage
    w.write_string(message);
    w.write_field_stop();
}

fn write_handle_id(w: &mut Writer, guid: &[u8], secret: &[u8]) {
    w.write_field_begin(ttype::STRING, 1);
    w.write_binary(guid);
    w.write_field_begin(ttype::STRING, 2);
    w.write_binary(secret);
    w.write_field_stop();
}

fn write_session_handle(w: &mut Writer, guid: &[u8], secret: &[u8]) {
    w.write_field_begin(ttype::STRUCT, 1);
    write_handle_id(w, guid, secret);
    w.write_field_stop();
}

fn write_operation_handle(w: &mut Writer, guid: &[u8], secret: &[u8]) {
    w.write_field_begin(ttype::STRUCT, 1);
    write_handle_id(w, guid, secret);
    w.write_field_begin(ttype::I32, 2);
    w.write_i32(0);
    w.write_field_begin(ttype::BOOL, 3);
    w.write_bool(true);
    w.write_field_stop();
}

fn build_open_session_resp(guid: &[u8], secret: &[u8]) -> Vec<u8> {
    wrap_reply("OpenSession", |w| {
        // `TOpenSessionResp.status` (field 1) is itself a nested `TStatus`
        // STRUCT, not the status fields inlined directly -- unlike
        // `CloseSession`/`CloseOperation` below, whose own "success" field
        // (id 0 in the outer envelope) *is* the `TStatus` struct itself,
        // with no extra nesting.
        w.write_field_begin(ttype::STRUCT, 1);
        write_status_ok(w);
        w.write_field_begin(ttype::STRUCT, 3);
        write_session_handle(w, guid, secret);
        w.write_field_stop();
    })
}

fn build_close_session_resp() -> Vec<u8> {
    wrap_reply("CloseSession", write_status_ok)
}

fn build_close_operation_resp() -> Vec<u8> {
    wrap_reply("CloseOperation", write_status_ok)
}

fn write_result_set_metadata(w: &mut Writer, lz4_compressed: bool, arrow_schema: Option<&[u8]>) {
    w.write_field_begin(ttype::BOOL, 1282);
    w.write_bool(lz4_compressed);
    if let Some(s) = arrow_schema {
        w.write_field_begin(ttype::STRING, 1283);
        w.write_binary(s);
    }
    w.write_field_stop();
}

fn write_arrow_batch(w: &mut Writer, batch: &[u8], row_count: i64) {
    w.write_field_begin(ttype::STRING, 1);
    w.write_binary(batch);
    w.write_field_begin(ttype::I64, 2);
    w.write_i64(row_count);
    w.write_field_stop();
}

fn write_result_link(w: &mut Writer, file_link: &str, row_count: i64) {
    w.write_field_begin(ttype::STRING, 1);
    w.write_string(file_link);
    w.write_field_begin(ttype::I64, 4);
    w.write_i64(row_count);
    w.write_field_stop();
}

#[derive(Default, Clone)]
struct FetchSpec {
    has_more_rows: bool,
    arrow_batches: Vec<(Vec<u8>, i64)>,
    result_links: Vec<(String, i64)>,
    metadata: Option<(bool, Option<Vec<u8>>)>, // (lz4_compressed, arrow_schema)
    status_error: Option<String>,
}

fn write_fetch_struct(w: &mut Writer, spec: &FetchSpec) {
    w.write_field_begin(ttype::STRUCT, 1);
    match &spec.status_error {
        Some(msg) => write_status_error(w, msg),
        None => write_status_ok(w),
    }
    w.write_field_begin(ttype::BOOL, 2);
    w.write_bool(spec.has_more_rows);
    if !spec.arrow_batches.is_empty() || !spec.result_links.is_empty() {
        w.write_field_begin(ttype::STRUCT, 3);
        if !spec.arrow_batches.is_empty() {
            w.write_field_begin(ttype::LIST, 1281);
            w.write_list_begin(ttype::STRUCT, spec.arrow_batches.len() as i32);
            for (b, rc) in &spec.arrow_batches {
                write_arrow_batch(w, b, *rc);
            }
        }
        if !spec.result_links.is_empty() {
            w.write_field_begin(ttype::LIST, 1282);
            w.write_list_begin(ttype::STRUCT, spec.result_links.len() as i32);
            for (link, rc) in &spec.result_links {
                write_result_link(w, link, *rc);
            }
        }
        w.write_field_stop();
    }
    if let Some((lz4, schema)) = &spec.metadata {
        w.write_field_begin(ttype::STRUCT, 1281);
        write_result_set_metadata(w, *lz4, schema.as_deref());
    }
    w.write_field_stop();
}

fn build_fetch_results_resp(spec: &FetchSpec) -> Vec<u8> {
    wrap_reply("FetchResults", |w| write_fetch_struct(w, spec))
}

fn write_operation_status_struct(w: &mut Writer, state: i32, message: Option<&str>) {
    w.write_field_begin(ttype::STRUCT, 1);
    write_status_ok(w);
    w.write_field_begin(ttype::I32, 2);
    w.write_i32(state);
    if let Some(m) = message {
        // Field 1281, not 12 -- see thrift.rs's own `OperationStatusResp::read`
        // fix (this session) for why: 12 was a real, now-fixed bug, found by
        // cross-checking databricks-sql-connector's real ttypes.py.
        w.write_field_begin(ttype::STRING, 1281); // displayMessage
        w.write_string(m);
    }
    w.write_field_stop();
}

fn build_get_operation_status_resp(state: i32, message: Option<&str>) -> Vec<u8> {
    wrap_reply("GetOperationStatus", |w| {
        write_operation_status_struct(w, state, message)
    })
}

#[derive(Default, Clone)]
struct DirectResultsSpec {
    operation_state: Option<i32>,
    operation_error: Option<String>,
    metadata: Option<(bool, Option<Vec<u8>>)>,
    fetch: Option<FetchSpec>,
}

fn write_direct_results(w: &mut Writer, spec: &DirectResultsSpec) {
    if let Some(state) = spec.operation_state {
        w.write_field_begin(ttype::STRUCT, 1);
        write_operation_status_struct(w, state, spec.operation_error.as_deref());
    }
    if let Some((lz4, schema)) = &spec.metadata {
        w.write_field_begin(ttype::STRUCT, 2);
        write_result_set_metadata(w, *lz4, schema.as_deref());
    }
    if let Some(fetch) = &spec.fetch {
        w.write_field_begin(ttype::STRUCT, 3);
        write_fetch_struct(w, fetch);
    }
    w.write_field_stop();
}

fn build_execute_statement_resp(op_guid: &[u8], op_secret: &[u8], direct: Option<DirectResultsSpec>) -> Vec<u8> {
    wrap_reply("ExecuteStatement", |w| {
        // Same `status` (field 1) nesting note as `build_open_session_resp`.
        w.write_field_begin(ttype::STRUCT, 1);
        write_status_ok(w);
        w.write_field_begin(ttype::STRUCT, 2);
        write_operation_handle(w, op_guid, op_secret);
        if let Some(d) = direct {
            w.write_field_begin(ttype::STRUCT, 1281);
            write_direct_results(w, &d);
        }
        w.write_field_stop();
    })
}

// ---- Arrow-IPC byte helpers ------------------------------------------------

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

fn make_batch(schema: &SchemaRef, lo: i64, hi: i64) -> RecordBatch {
    let ids: Vec<i64> = (lo..hi).collect();
    let labels: Vec<String> = ids.iter().map(|i| format!("row_{i}")).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(labels))],
    )
    .unwrap()
}

/// A full, standalone, valid Arrow-IPC stream (schema + one record batch +
/// EOS) -- what a cloud-fetch `resultLinks` download must be (decoded
/// directly by `decode_chunk`, same shape SEA's own external-link chunks
/// use). Mirrors `wiremock_pipeline.rs`'s own `build_chunk_bytes`.
fn build_full_stream_bytes(schema: &SchemaRef, lo: i64, hi: i64) -> Vec<u8> {
    let batch = make_batch(schema, lo, hi);
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

/// Splits a schema + N batches into (schema-message-only bytes, one
/// batch-message-only-bytes per batch) -- exactly what `pipeline.rs`'s
/// `build_inline_blob` expects to concatenate for the `arrowBatches`
/// (direct-results / inline `FetchResults`) path: the schema message is
/// carried once, separately, in `TGetResultSetMetadataResp.arrowSchema`,
/// and never repeated per-batch, and no EOS marker is expected at all
/// (`arrow_ipc::reader::StreamDecoder::finish()` accepts ending cleanly on
/// a message boundary with no explicit EOS -- confirmed directly against
/// its own source: `Header { read: 0, continuation: false }` is one of the
/// two states `finish()` treats as success, not just `Finished`).
fn build_schema_and_batch_messages(schema: &SchemaRef, batches: &[(i64, i64)]) -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut schema_only = Vec::new();
    {
        let _w = StreamWriter::try_new(&mut schema_only, schema).unwrap();
    }
    let mut batch_bytes = Vec::new();
    for &(lo, hi) in batches {
        let batch = make_batch(schema, lo, hi);
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        w.write(&batch).unwrap();
        assert!(
            buf.starts_with(&schema_only),
            "schema message prefix must be deterministic"
        );
        batch_bytes.push(buf[schema_only.len()..].to_vec());
    }
    (schema_only, batch_bytes)
}

fn compress_lz4_frame(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// Concatenates every batch's `id` column and checks it runs 0..n_rows in
/// order -- same helper as `wiremock_pipeline.rs`'s own.
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

/// `ResultStream` (the `Ok` side of `execute_lazy_thrift`'s return type)
/// doesn't implement `Debug`, so `Result::expect_err` can't be used directly
/// -- same reason `wiremock_pipeline.rs`'s own FAILED-statement test
/// (`a_failed_statement_discards_its_session_...`) matches manually instead.
fn expect_execute_err(
    result: Result<arrowbricks_core::pipeline::ResultStream, arrowbricks_core::client::ApiError>,
    panic_msg: &str,
) -> arrowbricks_core::client::ApiError {
    match result {
        Ok(_) => panic!("{panic_msg}"),
        Err(e) => e,
    }
}

fn thrift_client(server: &MockServer) -> Arc<DbClient> {
    Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token").with_protocol(Protocol::Thrift))
}

async fn mount_open_session_always(server: &MockServer, guid: &'static [u8]) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_mock = calls.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("OpenSession"))
        .respond_with(move |_req: &Request| {
            let n = calls_for_mock.fetch_add(1, Ordering::SeqCst);
            let mut guid_n = guid.to_vec();
            guid_n.extend_from_slice(format!("-{n}").as_bytes());
            ResponseTemplate::new(200).set_body_raw(build_open_session_resp(&guid_n, b"secret"), "application/x-thrift")
        })
        .mount(server)
        .await;
    calls
}

async fn mount_close_operation_ok(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("CloseOperation"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(build_close_operation_resp(), "application/x-thrift"))
        .mount(server)
        .await;
}

// ============================================================================
// Happy path: small query returns data inline via `getDirectResults`, zero
// `FetchResults` calls.
// ============================================================================

#[tokio::test]
async fn thrift_small_query_returns_data_inline_with_zero_fetch_results_calls() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    let (schema_bytes, batch_bytes) = build_schema_and_batch_messages(&schema, &[(0, 3)]);

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(
                b"op-a",
                b"opsecret-a",
                Some(DirectResultsSpec {
                    operation_state: Some(operation_state::FINISHED),
                    operation_error: None,
                    metadata: Some((false, Some(schema_bytes))),
                    fetch: Some(FetchSpec {
                        has_more_rows: false,
                        arrow_batches: vec![(batch_bytes[0].clone(), 3)],
                        ..Default::default()
                    }),
                }),
            ),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    // `.expect(0)` proves the small-query path genuinely never calls these --
    // not just that the final result happens to be correct some other way.
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("FetchResults"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("GetOperationStatus"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    assert_ids_in_order(&batches, 3);
}

/// Regression test for a real bug found via a real-warehouse variety pass:
/// `SELECT id, name FROM dim_article WHERE is_one_off = true LIMIT 5000`
/// came back with 5037 rows end to end (databricks-sql-connector, on
/// either protocol, correctly returned exactly 5000 for the identical
/// SQL) -- the server encoded more rows into the inline arrow batch than
/// its own declared `rowCount` said, and `run_thrift_fetch_loop` passed
/// `truncate_to: None` for the inline `arrowBatches` path, only ever
/// truncating `resultLinks` chunks to their declared count. Same server
/// behavior, same fix, just missing on this second path.
#[tokio::test]
async fn thrift_inline_arrow_batch_is_truncated_to_its_declared_row_count() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    let (schema_bytes, batch_bytes) = build_schema_and_batch_messages(&schema, &[(0, 5)]);

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(
                b"op-truncate",
                b"opsecret-truncate",
                Some(DirectResultsSpec {
                    operation_state: Some(operation_state::FINISHED),
                    operation_error: None,
                    metadata: Some((false, Some(schema_bytes))),
                    fetch: Some(FetchSpec {
                        has_more_rows: false,
                        // The wire batch actually encodes 5 rows (ids
                        // 0..5), but its own declared `rowCount` says 3 --
                        // exactly the "server over-delivers" shape found
                        // against the real warehouse.
                        arrow_batches: vec![(batch_bytes[0].clone(), 3)],
                        ..Default::default()
                    }),
                }),
            ),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "must keep exactly the declared row count, not however many rows the wire batch actually encoded"
    );
    assert_ids_in_order(&batches, 3);
}

// ============================================================================
// Happy path: multi-batch query, several sequential `FetchResults` calls,
// `resultLinks` in each, downloaded concurrently (out-of-order completion),
// order preserved end to end.
// ============================================================================

#[tokio::test]
async fn thrift_multi_batch_fetch_loop_preserves_order_across_concurrent_downloads() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    const N_BATCHES: i64 = 3;
    const CHUNKS_PER_BATCH: i64 = 2;
    const ROWS_PER_CHUNK: i64 = 5;
    let total_chunks = N_BATCHES * CHUNKS_PER_BATCH;
    let total_rows = total_chunks * ROWS_PER_CHUNK;

    // ExecuteStatement returns only the operation handle, no direct results --
    // forces the poll (`GetOperationStatus`) + sequential `FetchResults` loop.
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(b"op-b", b"opsecret-b", None),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("GetOperationStatus"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_get_operation_status_resp(operation_state::FINISHED, None),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    // Mount every chunk's download route -- reverse delay so later-indexed
    // chunks finish *first*, forcing genuine out-of-order completion across
    // batches (not just within one `FetchResults` response).
    for i in 0..total_chunks {
        let bytes = build_full_stream_bytes(&schema, i * ROWS_PER_CHUNK, (i + 1) * ROWS_PER_CHUNK);
        let delay_ms = ((total_chunks - i) * 15) as u64;
        Mock::given(method("GET"))
            .and(path(format!("/_data/chunk-{i}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(bytes, "application/vnd.apache.arrow.stream")
                    .set_delay(std::time::Duration::from_millis(delay_ms)),
            )
            .mount(&server)
            .await;
    }

    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetch_calls_for_mock = fetch_calls.clone();
    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("FetchResults"))
        .respond_with(move |_req: &Request| {
            let n = fetch_calls_for_mock.fetch_add(1, Ordering::SeqCst) as i64;
            let base = n * CHUNKS_PER_BATCH;
            let links: Vec<(String, i64)> = (0..CHUNKS_PER_BATCH)
                .map(|j| (format!("{uri}/_data/chunk-{}", base + j), ROWS_PER_CHUNK))
                .collect();
            let has_more = n + 1 < N_BATCHES;
            ResponseTemplate::new(200).set_body_raw(
                build_fetch_results_resp(&FetchSpec {
                    has_more_rows: has_more,
                    result_links: links,
                    // Explicitly confirms lz4_compressed=false -- without
                    // this, `client.compress_results()`'s default-true
                    // guess (see `submit_and_await_thrift_statement`'s own
                    // doc comment) never gets corrected by a real
                    // `TGetResultSetMetadataResp`, and the fetch workers
                    // then try to LZ4-decompress plain uncompressed test
                    // data and fail.
                    metadata: Some((false, None)),
                    ..Default::default()
                }),
                "application/x-thrift",
            )
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows as i64, total_rows);
    assert_ids_in_order(&batches, total_rows);
    assert_eq!(
        fetch_calls.load(Ordering::SeqCst) as i64,
        N_BATCHES,
        "must issue exactly one FetchResults call per discovered batch"
    );
}

// ============================================================================
// A FAILED statement surfaces as an error -- both the immediate
// (getDirectResults) and polled (GetOperationStatus) failure paths.
// ============================================================================

#[tokio::test]
async fn thrift_direct_results_error_state_fails_the_query_immediately() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(
                b"op-c",
                b"opsecret-c",
                Some(DirectResultsSpec {
                    operation_state: Some(operation_state::ERROR),
                    operation_error: Some("SYNTAX_ERROR: bad sql".to_string()),
                    ..Default::default()
                }),
            ),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    // Must never be called: a statement that already failed inside
    // `ExecuteStatement` itself has nothing to poll or close.
    for rpc in ["GetOperationStatus", "FetchResults", "CloseOperation"] {
        Mock::given(method("POST"))
            .and(path(thrift_path()))
            .and(IsThriftRpc(rpc))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
    }

    let client = thrift_client(&server);
    let result = execute_lazy_thrift(client, "not valid sql", None, None, None).await;
    let err = expect_execute_err(
        result,
        "a FAILED direct-results operation status must surface as an error",
    );
    assert!(
        err.message.contains("SYNTAX_ERROR"),
        "unexpected message: {}",
        err.message
    );
}

#[tokio::test]
async fn thrift_get_operation_status_reports_a_failed_statement_via_polling() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(b"op-d", b"opsecret-d", None),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    let poll_calls = Arc::new(AtomicUsize::new(0));
    let poll_calls_for_mock = poll_calls.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("GetOperationStatus"))
        .respond_with(move |_req: &Request| {
            poll_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_raw(
                build_get_operation_status_resp(operation_state::ERROR, Some("Query failed: table not found")),
                "application/x-thrift",
            )
        })
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("FetchResults"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let result = execute_lazy_thrift(client, "SELECT * FROM missing", None, None, None).await;
    let err = expect_execute_err(result, "a polled ERROR operation state must surface as an error");
    assert!(
        err.message.contains("table not found"),
        "unexpected message: {}",
        err.message
    );
    assert_eq!(
        poll_calls.load(Ordering::SeqCst),
        1,
        "must not keep polling after a terminal error"
    );
}

// ============================================================================
// LZ4 compression -- both the cloud-fetch `resultLinks` path and the inline
// `arrowBatches` (direct results) path honor `lz4_compressed`, not just
// assume it because `can_decompress_lz4` was requested.
// ============================================================================

#[tokio::test]
async fn thrift_lz4_compressed_result_links_are_decompressed() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(b"op-e", b"opsecret-e", None),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("GetOperationStatus"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_get_operation_status_resp(operation_state::FINISHED, None),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    let uri = server.uri();
    let raw = build_full_stream_bytes(&schema, 0, 5);
    let compressed = compress_lz4_frame(&raw);
    Mock::given(method("GET"))
        .and(path("/_data/chunk-0"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(compressed, "application/octet-stream"))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("FetchResults"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_fetch_results_resp(&FetchSpec {
                has_more_rows: false,
                result_links: vec![(format!("{uri}/_data/chunk-0"), 5)],
                metadata: Some((true, None)),
                ..Default::default()
            }),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "must decompress the LZ4-frame-compressed link before decoding");
    assert_ids_in_order(&batches, 5);
}

#[tokio::test]
async fn thrift_lz4_compressed_inline_direct_results_batch_is_decompressed() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    let (schema_bytes, batch_bytes) = build_schema_and_batch_messages(&schema, &[(0, 4)]);
    let compressed_batch = compress_lz4_frame(&batch_bytes[0]);

    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            build_execute_statement_resp(
                b"op-f",
                b"opsecret-f",
                Some(DirectResultsSpec {
                    operation_state: Some(operation_state::FINISHED),
                    metadata: Some((true, Some(schema_bytes))),
                    fetch: Some(FetchSpec {
                        has_more_rows: false,
                        arrow_batches: vec![(compressed_batch, 4)],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            ),
            "application/x-thrift",
        ))
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 4,
        "must decompress the LZ4-compressed inline batch before decoding"
    );
    assert_ids_in_order(&batches, 4);
}

// ============================================================================
// Thrift session pool (`client::ThriftSessionPool`/`thrift_checkout_session`/
// `thrift_checkin_session`) -- mirrors the SEA session pool tests in
// `wiremock_pipeline.rs` (`session_is_created_once_and_reused_across_
// sequential_statements`, `concurrent_statements_on_the_same_key_each_get_
// their_own_pooled_session`, `session_pool_exhaustion_falls_back_to_
// session_less_submission`, `a_failed_statement_discards_its_session_
// instead_of_returning_it_to_the_pool`, `close_all_sessions_deletes_every_
// idle_pooled_session`), adapted for Thrift's one real difference: there is
// no session-less fallback, so "pool exhaustion" falls back to a throwaway
// session (opened and closed just for that one call) instead of a
// session-less statement body.
// ============================================================================

fn small_execute_statement_success(op_guid: Vec<u8>, schema: &SchemaRef) -> Vec<u8> {
    let (schema_bytes, batch_bytes) = build_schema_and_batch_messages(schema, &[(0, 1)]);
    build_execute_statement_resp(
        &op_guid,
        b"opsecret",
        Some(DirectResultsSpec {
            operation_state: Some(operation_state::FINISHED),
            metadata: Some((false, Some(schema_bytes))),
            fetch: Some(FetchSpec {
                has_more_rows: false,
                arrow_batches: vec![(batch_bytes[0].clone(), 1)],
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
}

#[tokio::test]
async fn thrift_session_is_created_once_and_reused_across_sequential_statements() {
    let server = MockServer::start().await;
    let open_session_calls = mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    let exec_calls = Arc::new(AtomicUsize::new(0));
    let exec_calls_for_mock = exec_calls.clone();
    let schema_for_mock = schema.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(move |_req: &Request| {
            let n = exec_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_raw(
                small_execute_statement_success(format!("op-{n}").into_bytes(), &schema_for_mock),
                "application/x-thrift",
            )
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    for sql in ["SELECT 1", "SELECT 2"] {
        let mut stream = execute_lazy_thrift(client.clone(), sql, Some("cat1"), None, None)
            .await
            .unwrap();
        stream.fetchall_arrow().await.unwrap();
    }

    assert_eq!(
        open_session_calls.load(Ordering::SeqCst),
        1,
        "a second statement on the same (catalog, schema) key must reuse the pooled session, not open a new one"
    );
}

#[tokio::test]
async fn thrift_pool_exhaustion_falls_back_to_a_throwaway_session_that_still_succeeds() {
    let server = MockServer::start().await;
    let open_session_calls = mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let close_session_calls = Arc::new(AtomicUsize::new(0));
    let close_session_calls_for_mock = close_session_calls.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("CloseSession"))
        .respond_with(move |_req: &Request| {
            close_session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_raw(build_close_session_resp(), "application/x-thrift")
        })
        .mount(&server)
        .await;

    let schema = test_schema();
    let exec_calls = Arc::new(AtomicUsize::new(0));
    let exec_calls_for_mock = exec_calls.clone();
    let schema_for_mock = schema.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(move |_req: &Request| {
            let n = exec_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_body_raw(
                    small_execute_statement_success(format!("op-{n}").into_bytes(), &schema_for_mock),
                    "application/x-thrift",
                )
                // Held "checked out" long enough that every concurrently
                // spawned task's own checkout attempt races for the pool
                // before any of the first MAX_SESSIONS_PER_KEY complete --
                // same trick `wiremock_pipeline.rs`'s own
                // `session_pool_exhaustion_falls_back_to_session_less_
                // submission` uses (delaying the statement response, not
                // session creation).
                .set_delay(std::time::Duration::from_millis(120))
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let n = MAX_SESSIONS_PER_KEY + 1;
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut stream = execute_lazy_thrift(client, &format!("SELECT {i}"), Some("cat1"), None, None)
                    .await
                    .unwrap();
                stream.fetchall_arrow().await.unwrap();
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(
        open_session_calls.load(Ordering::SeqCst),
        n,
        "every pooled session (up to MAX_SESSIONS_PER_KEY) plus the one throwaway session for the caller that \
         couldn't get a pooled slot must each open exactly one Thrift session"
    );
    // `drive_thrift_fetch_loop`'s cleanup RPCs (CloseOperation, then a
    // throwaway session's CloseSession) run on a detached `tokio::spawn`
    // task that outlives `fetchall_arrow()` returning to the caller --
    // that's the fix for the channel-close-blocks-on-cleanup bug (see
    // `drive_thrift_fetch_loop`'s own doc comment). So the close call isn't
    // guaranteed to have landed the instant every `h.await` above returns;
    // poll briefly instead of asserting immediately.
    for _ in 0..100 {
        if close_session_calls.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        close_session_calls.load(Ordering::SeqCst),
        1,
        "only the one throwaway session may be closed -- every pooled session stays open, checked back into the idle pool"
    );
}

#[tokio::test]
async fn thrift_a_failed_statement_discards_its_session_instead_of_returning_it_to_the_pool() {
    let server = MockServer::start().await;
    let open_session_calls = mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    let exec_calls = Arc::new(AtomicUsize::new(0));
    let exec_calls_for_mock = exec_calls.clone();
    let schema_for_mock = schema.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(move |_req: &Request| {
            let n = exec_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(200).set_body_raw(
                    build_execute_statement_resp(
                        b"op-fail",
                        b"opsecret-fail",
                        Some(DirectResultsSpec {
                            operation_state: Some(operation_state::ERROR),
                            operation_error: Some("bad sql".to_string()),
                            ..Default::default()
                        }),
                    ),
                    "application/x-thrift",
                )
            } else {
                ResponseTemplate::new(200).set_body_raw(
                    small_execute_statement_success(format!("op-{n}").into_bytes(), &schema_for_mock),
                    "application/x-thrift",
                )
            }
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let result = execute_lazy_thrift(client.clone(), "not valid sql", Some("cat1"), None, None).await;
    let err = expect_execute_err(result, "expected the FAILED statement to surface as an error");
    assert!(err.message.contains("bad sql"));

    let mut stream = execute_lazy_thrift(client, "SELECT 1", Some("cat1"), None, None)
        .await
        .unwrap();
    stream.fetchall_arrow().await.unwrap();

    assert_eq!(
        open_session_calls.load(Ordering::SeqCst),
        2,
        "the session behind a FAILED statement must be discarded, not handed to the next caller on the same key"
    );
}

#[tokio::test]
async fn thrift_close_all_sessions_closes_every_idle_pooled_session() {
    let server = MockServer::start().await;
    mount_open_session_always(&server, b"sess").await;
    mount_close_operation_ok(&server).await;

    let schema = test_schema();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_raw(
                small_execute_statement_success(b"op-0".to_vec(), &schema),
                "application/x-thrift",
            )
        })
        .mount(&server)
        .await;

    let close_session_calls = Arc::new(AtomicUsize::new(0));
    let close_session_calls_for_mock = close_session_calls.clone();
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("CloseSession"))
        .respond_with(move |_req: &Request| {
            close_session_calls_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_raw(build_close_session_resp(), "application/x-thrift")
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client.clone(), "SELECT 1", Some("cat1"), None, None)
        .await
        .unwrap();
    stream.fetchall_arrow().await.unwrap();

    client.close_all_thrift_sessions().await;

    assert_eq!(
        close_session_calls.load(Ordering::SeqCst),
        1,
        "the idle pooled session must be closed exactly once"
    );
}

// ============================================================================
// `DbClient::fetch_link_bytes_split`/`download_slots` -- the parallel-Range
// cloud-fetch download. Only reachable end-to-end (both are `pub(crate)`),
// so these mock a blob-storage GET that actually honors `Range` instead of
// the existing tests' plain 200-ignoring-Range stand-in.
// ============================================================================

/// Parses a `Range: bytes=start-end` request header, same syntax
/// `fetch_link_bytes_split` itself only ever sends.
fn parse_range(req: &Request) -> Option<(u64, u64)> {
    let raw = req.headers.get("range")?.to_str().ok()?;
    let (start, end) = raw.strip_prefix("bytes=")?.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

/// Mounts a blob-storage GET that genuinely honors `Range`: a request
/// carrying one gets a `206 Partial Content` with a real `Content-Range`
/// header and just that byte slice; a request with none (nothing exercises
/// this today, but it's the documented fallback) gets the whole body as a
/// plain `200`. Returns the number of GET requests actually received, so a
/// test can prove the download really did split into more than one.
async fn mount_ranged_blob(server: &MockServer, path_str: &'static str, body: Vec<u8>) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_mock = calls.clone();
    let total = body.len() as u64;
    Mock::given(method("GET"))
        .and(path(path_str))
        .respond_with(move |req: &Request| {
            calls_for_mock.fetch_add(1, Ordering::SeqCst);
            match parse_range(req) {
                Some((start, end)) => {
                    let end = end.min(total - 1);
                    let slice = body[start as usize..=end as usize].to_vec();
                    ResponseTemplate::new(206)
                        .insert_header("Content-Range", format!("bytes {start}-{end}/{total}"))
                        .set_body_raw(slice, "application/octet-stream")
                }
                None => ResponseTemplate::new(200).set_body_raw(body.clone(), "application/octet-stream"),
            }
        })
        .mount(server)
        .await;
    calls
}

/// Mounts the fixed Thrift-side scaffolding a single-resultLink query
/// needs (`ExecuteStatement` returning direct results with one link,
/// `GetOperationStatus` reporting `FINISHED`, `CloseOperation` ok) so each
/// test below only has to mount its own blob-storage GET.
async fn mount_single_link_statement(server: &MockServer, link_path: &'static str, row_count: i64) {
    mount_open_session_always(server, b"sess").await;
    mount_close_operation_ok(server).await;
    let link_url = format!("{}{link_path}", server.uri());
    Mock::given(method("POST"))
        .and(path(thrift_path()))
        .and(IsThriftRpc("ExecuteStatement"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_raw(
                build_execute_statement_resp(
                    b"op-split",
                    b"opsecret-split",
                    Some(DirectResultsSpec {
                        operation_state: Some(operation_state::FINISHED),
                        operation_error: None,
                        metadata: Some((false, None)),
                        fetch: Some(FetchSpec {
                            has_more_rows: false,
                            arrow_batches: vec![],
                            result_links: vec![(link_url.clone(), row_count)],
                            metadata: Some((false, None)),
                            status_error: None,
                        }),
                    }),
                ),
                "application/x-thrift",
            )
        })
        .mount(server)
        .await;
}

#[tokio::test]
async fn thrift_single_link_downloads_via_parallel_range_requests() {
    let server = MockServer::start().await;
    // 60,000 rows of `id: i64, label: "row_{i}"` comfortably clears the
    // 1MiB part size `fetch_link_bytes_budgeted` uses, so a lone in-flight
    // link (the only query in flight -- the full `download_slots` budget
    // is free) must split into more than one request to cover it.
    let schema = test_schema();
    let body = build_full_stream_bytes(&schema, 0, 60_000);
    assert!(body.len() > 1 << 20, "test body must exceed the 1MiB part size to prove splitting");

    mount_single_link_statement(&server, "/_data/big-link", 60_000).await;
    let get_calls = mount_ranged_blob(&server, "/_data/big-link", body).await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let (batches, _schema) = stream.fetchall_arrow().await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 60_000, "every row must survive a split-and-reassembled download");
    assert_ids_in_order(&batches, 60_000);
    assert!(
        get_calls.load(Ordering::SeqCst) > 1,
        "a lone in-flight link with the full download_slots budget available must actually split \
         into more than one Range request, not silently fall back to one"
    );
}

#[tokio::test]
async fn thrift_split_download_fails_loud_on_unparseable_content_range() {
    let server = MockServer::start().await;
    mount_single_link_statement(&server, "/_data/bad-range", 60_000).await;

    // Answers 206 (Range honored) but with a Content-Range header this
    // crate can't parse -- must be a hard failure, not a silent return of
    // just the first probed part.
    Mock::given(method("GET"))
        .and(path("/_data/bad-range"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("Content-Range", "not-a-real-content-range-header")
                .set_body_raw(vec![0u8; 1 << 20], "application/octet-stream"),
        )
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let err = stream.fetchall_arrow().await.expect_err(
        "a 206 with an unparseable Content-Range must fail loudly, not silently truncate the result",
    );
    assert!(
        err.message.contains("refusing to silently return a truncated file"),
        "got a different error than expected: {}",
        err.message
    );
}

#[tokio::test]
async fn thrift_split_download_fails_on_assembled_length_mismatch() {
    let server = MockServer::start().await;
    mount_single_link_statement(&server, "/_data/short-parts", 60_000).await;

    // Every part -- including the probe -- honestly declares a 5MiB total
    // via Content-Range, but actually returns far fewer bytes than its own
    // declared range covers (a misbehaving server). The declared total
    // guarantees more than one part gets requested; every part coming up
    // short must be caught by the final length check, not silently
    // accepted as a truncated-but-otherwise-fine result.
    const DECLARED_TOTAL: u64 = 5 * (1 << 20);
    Mock::given(method("GET"))
        .and(path("/_data/short-parts"))
        .respond_with(move |req: &Request| {
            let (start, _end) = parse_range(req).expect("client always sends Range on this path");
            let end = (start + 9).min(DECLARED_TOTAL - 1);
            ResponseTemplate::new(206)
                .insert_header("Content-Range", format!("bytes {start}-{end}/{DECLARED_TOTAL}"))
                .set_body_raw(vec![0u8; 10], "application/octet-stream")
        })
        .mount(&server)
        .await;

    let client = thrift_client(&server);
    let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
        .await
        .unwrap();
    let err = stream
        .fetchall_arrow()
        .await
        .expect_err("a server that under-delivers every part must fail loudly, not return a truncated blob");
    assert!(
        err.message.contains("expected"),
        "got a different error than expected: {}",
        err.message
    );
}

#[tokio::test]
async fn thrift_concurrent_single_link_queries_stay_correct_under_split_contention() {
    let server = MockServer::start().await;
    let schema = test_schema();
    let body = build_full_stream_bytes(&schema, 0, 60_000);
    mount_single_link_statement(&server, "/_data/concurrent-link", 60_000).await;
    mount_ranged_blob(&server, "/_data/concurrent-link", body).await;

    let client = thrift_client(&server);
    // 20 single-link queries fired at once all compete for the same
    // `download_slots` budget (sized to `chunk_fetch_concurrency`, default
    // 64) -- proves the budget accounting degrades to "some queries get a
    // smaller split, none deadlock or corrupt data" under real contention,
    // not just in the uncontended single-query case above.
    let handles: Vec<_> = (0..20)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut stream = execute_lazy_thrift(client, "SELECT * FROM t", None, None, None)
                    .await
                    .unwrap();
                stream.fetchall_arrow().await.unwrap()
            })
        })
        .collect();

    for h in handles {
        let (batches, _schema) = tokio::time::timeout(std::time::Duration::from_secs(30), h)
            .await
            .expect("must not deadlock or hang under concurrent split-download contention")
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 60_000);
        assert_ids_in_order(&batches, 60_000);
    }
}
