use std::sync::Arc;

use arrowbricks_core::agent::{AgentOptions, query};
use arrowbricks_core::client::{DbClient, Protocol};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn warehouse(server: &MockServer) -> Arc<DbClient> {
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/warehouses/wh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state":"RUNNING"})))
        .mount(server)
        .await;
    Arc::new(DbClient::new(&server.uri(), "wh", "token").with_protocol(Protocol::Sea))
}

#[derive(Default)]
struct Events(std::sync::Mutex<Vec<&'static str>>);

impl arrowbricks_core::client::EventSink for Events {
    fn on_event(&self, stats: arrowbricks_core::client::QueryStatsData) {
        self.0.lock().unwrap().push(stats.outcome);
    }
}

#[tokio::test]
async fn render_failure_is_reported_once_after_source_exhaustion() {
    let server = MockServer::start().await;
    let _ = warehouse(&server).await;
    let events = Arc::new(Events::default());
    let client = Arc::new(
        DbClient::new(&server.uri(), "wh", "token")
            .with_protocol(Protocol::Sea)
            .with_on_event(events.clone()),
    );
    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id":"rows", "status":{"state":"SUCCEEDED"},
            "manifest":{"chunks":[{"chunk_index":0,"row_count":1},{"chunk_index":1,"row_count":1}]}
        })))
        .mount(&server)
        .await;
    for index in 0..2 {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            format!("field_{index}"),
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![index]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        Mock::given(method("GET"))
            .and(path(format!("/api/2.0/sql/statements/rows/result/chunks/{index}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"external_links":[{"external_link":format!("{uri}/data{index}")}]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/data{index}")))
            .respond_with(ResponseTemplate::new(200).set_body_raw(bytes, "application/octet-stream"))
            .mount(&server)
            .await;
    }
    let output = query(client, "select id", None, None, None, AgentOptions::default()).await;
    let diagnostic: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(diagnostic["category"], "execution_error");
    assert_eq!(*events.0.lock().unwrap(), vec!["error"]);
}

#[tokio::test]
async fn empty_agent_result_retains_manifest_schema() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id":"empty", "status":{"state":"SUCCEEDED"},
            "manifest":{"chunks":[], "schema":{"columns":[{"name":"answer","type_name":"LONG"}]}}
        })))
        .mount(&server)
        .await;
    let output = query(
        client,
        "select 1 where false",
        None,
        None,
        None,
        AgentOptions::default(),
    )
    .await;
    assert!(output.contains("answer"), "{output}");
    assert!(output.contains("total_rows=0"), "{output}");
    assert!(output.len() <= 10_000);
}

#[tokio::test]
async fn statement_diagnostics_are_json_and_preserve_suggestions() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id":"bad", "status":{"state":"FAILED", "error":{
                "error_code":"UNRESOLVED_COLUMN.WITH_SUGGESTION",
                "message":"[UNRESOLVED_COLUMN.WITH_SUGGESTION] missing nope. Did you mean `name`?"
            }}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let output = query(client, "select nope", None, None, None, AgentOptions::default()).await;
    let diagnostic: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(diagnostic["category"], "missing_column");
    assert!(diagnostic["message"].as_str().unwrap().contains("Did you mean `name`?"));
    assert_eq!(diagnostic["error_class"], "UNRESOLVED_COLUMN.WITH_SUGGESTION");
}

#[tokio::test]
async fn total_deadline_covers_submit() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(500)))
        .mount(&server)
        .await;
    let options = AgentOptions {
        total_timeout_s: 0.03,
        ..AgentOptions::default()
    };
    let start = std::time::Instant::now();
    let output = query(client, "select 1", None, None, None, options).await;
    assert!(start.elapsed().as_secs_f64() < 0.4);
    let diagnostic: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(diagnostic["category"], "timeout");
}

async fn chunks(server: &MockServer, rows: usize, corrupt: bool, delay_ms: u64) {
    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id":"rows", "status":{"state":"SUCCEEDED"},
            "manifest":{"chunks":[{"chunk_index":0,"row_count":rows}],
                "schema":{"columns":[{"name":"id","type_name":"LONG"}]}},
            "result":{"external_links":[{"external_link":format!("{uri}/data"),"chunk_index":0,"row_count":rows}]}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/rows/result/chunks/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links":[{"external_link":format!("{uri}/data")}]
        })))
        .mount(server)
        .await;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(
            (0..rows as i64).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    if corrupt {
        bytes.clear();
    }
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(bytes, "application/octet-stream")
                .set_delay(std::time::Duration::from_millis(delay_ms)),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/rows/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(server)
        .await;
}

#[tokio::test]
async fn row_limit_uses_lookahead_and_never_claims_exact_prefix_total() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    chunks(&server, 3, false, 0).await;
    let options = AgentOptions {
        max_rows: 2,
        ..AgentOptions::default()
    };
    let output = query(client, "select id", None, None, None, options).await;
    assert!(output.contains("rows_shown=2"), "{output}");
    assert!(output.contains("complete=false"), "{output}");
    assert!(!output.contains("total_rows=2"), "{output}");
}

#[tokio::test]
async fn exactly_row_limit_requires_exhaustion_to_claim_complete() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    chunks(&server, 2, false, 0).await;
    let options = AgentOptions {
        max_rows: 2,
        ..AgentOptions::default()
    };
    let output = query(client, "select id", None, None, None, options).await;
    assert!(output.contains("total_rows=2"), "{output}");
    assert!(output.contains("complete=true"), "{output}");
}

#[tokio::test]
async fn corrupt_download_is_an_error_instead_of_successful_empty_result() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    chunks(&server, 2, true, 0).await;
    let output = query(client, "select id", None, None, None, AgentOptions::default()).await;
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["category"], "execution_error");
    assert!(value["message"].as_str().unwrap().contains("empty Arrow IPC"));
}

#[tokio::test]
async fn total_download_deadline_fires_cancel_request() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    chunks(&server, 2, false, 500).await;
    let options = AgentOptions {
        total_timeout_s: 0.05,
        ..AgentOptions::default()
    };
    let output = query(client, "select id", None, None, None, options).await;
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["category"], "timeout");
    for _ in 0..100 {
        if server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path().ends_with("/cancel"))
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("deadline did not cancel the server statement");
}

#[tokio::test]
async fn preview_does_not_start_the_clients_full_download_pool() {
    let server = MockServer::start().await;
    let client = warehouse(&server).await;
    chunks(&server, 3, false, 0).await;
    let uri = server.uri();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id":"rows", "status":{"state":"SUCCEEDED"},
            "manifest":{"chunks":(0..20).map(|index| json!({"chunk_index":index,"row_count":3})).collect::<Vec<_>>(),
                "schema":{"columns":[{"name":"id","type_name":"LONG"}]}}
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(
            "/api/2.0/sql/statements/rows/result/chunks/[0-9]+",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links":[{"external_link":format!("{uri}/data")}]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    let output = query(
        client,
        "select id",
        None,
        None,
        None,
        AgentOptions {
            max_rows: 1,
            ..AgentOptions::default()
        },
    )
    .await;
    assert!(output.contains("rows_shown=1"), "{output}");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let calls = server.received_requests().await.unwrap();
    let downloads = calls.iter().filter(|request| request.url.path() == "/data").count();
    assert!(downloads <= 3, "preview started {downloads} downloads");
}
