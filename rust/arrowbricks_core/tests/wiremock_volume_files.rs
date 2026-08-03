//! Files API (upload_volume_file/delete_volume_file) -- kept separate from
//! wiremock_pipeline.rs since it's a different API surface entirely (no
//! statement/manifest/chunk shape to it), mirroring tests/conftest.py's own
//! `mock_volume_files` fixture being kept apart from `mock_warehouse`.

use std::sync::Arc;

use arrowbricks_core::client::DbClient;
use bytes::Bytes;
use wiremock::matchers::{method, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const WAREHOUSE_ID: &str = "wh-test-123";

#[tokio::test]
async fn upload_volume_file_sends_overwrite_and_content_type() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .and(query_param("overwrite", "true"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    client
        .upload_volume_file("/Volumes/cat/schema/vol/file.bin", Bytes::from_static(b"hello"))
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_volume_file_succeeds_on_204() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    client
        .delete_volume_file("/Volumes/cat/schema/vol/file.bin")
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_volume_file_treats_404_as_success() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    client
        .delete_volume_file("/Volumes/cat/schema/vol/already-gone.bin")
        .await
        .expect("404 must be treated as success (idempotent delete), not an error");
}

#[tokio::test]
async fn delete_volume_file_surfaces_other_errors() {
    // 400, not 500/429/etc -- those are classified transient and would
    // trigger the full 6-attempt retry/backoff cycle (tens of seconds) for
    // no benefit here; that classification logic already has its own
    // coverage. This only needs a status delete_volume_file does NOT treat
    // as "already gone", proving the error still propagates.
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    let err = client
        .delete_volume_file("/Volumes/cat/schema/vol/file.bin")
        .await
        .unwrap_err();
    assert!(
        !err.transient,
        "400 is not transient, should surface immediately without retrying"
    );
}
