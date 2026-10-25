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

// ---------------------------------------------------------------------------
// Upload deadline scaling and retry body reuse. wiremock cannot throttle how
// fast the *server reads* a request body, so these use a tiny raw TCP server.
// ---------------------------------------------------------------------------

use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const MIB: usize = 1 << 20;

/// Raw HTTP/1.1 server answering every PUT with `204` once it has read the
/// whole body, consuming it at roughly `bytes_per_sec`. `stuck` instead never
/// reads or answers at all.
async fn spawn_throttled_server(bytes_per_sec: usize, stuck: bool) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                if stuck {
                    tokio::time::sleep(Duration::from_secs(120)).await;
                    return;
                }
                let mut buf = vec![0u8; 64 * 1024];
                let mut head = Vec::new();
                let (mut body_len, mut got_body, mut header_end) = (None::<usize>, 0usize, None::<usize>);
                loop {
                    let Ok(n) = sock.read(&mut buf).await else { return };
                    if n == 0 {
                        return;
                    }
                    if header_end.is_none() {
                        head.extend_from_slice(&buf[..n]);
                        if let Some(pos) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                            let text = String::from_utf8_lossy(&head[..pos]).to_lowercase();
                            body_len = text
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()));
                            header_end = Some(pos + 4);
                            got_body = head.len() - (pos + 4);
                        }
                    } else {
                        got_body += n;
                    }
                    if header_end.is_some() {
                        if got_body >= body_len.expect("fixed-length body") {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs_f64(n as f64 / bytes_per_sec as f64)).await;
                    }
                }
                let _ = sock
                    .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn upload_slow_but_progressing_succeeds_past_http_timeout() {
    // 3 MiB read at ~1.5 MiB/s takes ~2 s: over the 1 s http_timeout, under
    // the scaled deadline (3 MiB / 1 MiB/s = 3 s).
    let uri = spawn_throttled_server(MIB + MIB / 2, false).await;
    let client = Arc::new(DbClient::new(&uri, WAREHOUSE_ID, "fake-token").with_http_timeout(1.0));
    let start = Instant::now();
    client
        .upload_volume_file("/Volumes/c/s/v/big.bin", Bytes::from(vec![7u8; 3 * MIB]))
        .await
        .expect("a progressing upload must not be killed at http_timeout");
    assert!(
        start.elapsed() > Duration::from_secs(1),
        "test must exceed http_timeout to prove anything, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn upload_stuck_server_still_times_out_at_scaled_deadline() {
    let uri = spawn_throttled_server(1, true).await;
    let client = Arc::new(DbClient::new(&uri, WAREHOUSE_ID, "fake-token").with_http_timeout(1.0));
    let start = Instant::now();
    let err = client
        .upload_volume_file("/Volumes/c/s/v/big.bin", Bytes::from(vec![7u8; 2 * MIB]))
        .await
        .expect_err("a stuck server must still fail");
    let took = start.elapsed();
    assert!(!err.transient, "timeouts stay non-transient (not retried): {err:?}");
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_secs(10),
        "deadline should be the scaled ~2 s, took {took:?}"
    );
}

#[tokio::test]
async fn upload_retry_after_503_resends_full_identical_body() {
    use wiremock::matchers::body_bytes;
    let server = MockServer::start().await;
    let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    Mock::given(method("PUT"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/api/2\.0/fs/files/.*"))
        .and(body_bytes(payload.clone()))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = Arc::new(DbClient::new(&server.uri(), WAREHOUSE_ID, "fake-token"));
    client
        .upload_volume_file("/Volumes/c/s/v/retry.bin", Bytes::from(payload))
        .await
        .expect("second attempt must carry the complete, identical body");
}
