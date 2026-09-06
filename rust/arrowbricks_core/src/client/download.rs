//! Downloading and decompressing one cloud-fetch external link: a retryable
//! GET (`fetch_link_bytes`), an adaptive-concurrency Range-split variant for
//! when the shared download-slot budget has room to spare
//! (`fetch_link_bytes_budgeted`/`fetch_link_bytes_split`), and unwrapping the
//! outer LZ4 Frame compression the server applies when `result_compression`
//! was requested (`decompress_lz4_frame`) -- shared by both the SEA and
//! Thrift backends' own chunk-fetch workers.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::DbClient;
use super::MAX_SPLIT_PARTS;
use super::error::{ApiError, join_error};
use super::model::QueryStatsAccumulator;

/// Unwraps one downloaded chunk file's outer LZ4 Frame compression --
/// server-side, this is a whole-file wrapper applied because we asked for
/// `result_compression: "LZ4_FRAME"`, not Arrow-IPC's own (unrelated,
/// per-buffer) compression option. `reqwest::Response::bytes()` already
/// collected the full body reliably before this runs, so a decode failure
/// here means a genuine format problem, not a network blip -- treated as
/// permanent (not retried), same reasoning as `ApiError::from_reqwest`.
///
/// A real Databricks chunk is not one LZ4 frame -- it's several concatenated
/// back to back (confirmed against a real workspace: a 50k-row/1MB chunk came
/// back as 18 separate frames). `FrameDecoder::read_to_end` stops the moment
/// it hits the first frame's own end marker; calling it once silently
/// produced only that first frame's ~200 bytes (just the Arrow schema
/// message, no data) instead of the full stream, which `pipeline/reorder.rs`'s
/// `decode_chunk` then decoded as a zero-row/zero-column result with no error at all -- this bug
/// shipped from a design that was only ever verified against synthetic
/// single-frame test data. One `FrameDecoder` resets its own frame state
/// after each `EndMark` and picks up the next concatenated frame on a
/// subsequent `read_to_end` call against the *same* instance (verified: the
/// decoder's position in the underlying byte slice carries over across
/// calls) -- so looping `read_to_end` on one decoder until its underlying
/// reader is exhausted reads every frame without reconstructing a decoder.
pub(crate) fn decompress_lz4_frame(compressed: &Bytes) -> Result<Bytes, ApiError> {
    use std::io::Read;
    // LZ4 on Arrow-IPC data often compresses several-fold. Starting at the
    // compressed size can therefore pay for repeated buffer growth. Neither
    // size is a bound on the other: incompressible input can expand slightly.
    // `* 4` remains a heuristic; Vec grows normally when it underestimates.
    // See benchmark_lz4_capacity_hypotheses for the allocation tradeoffs.
    let mut out = Vec::with_capacity(compressed.len() * 4);
    let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
    // Terminate on the *reader* being exhausted, not on "output stopped
    // growing" -- found in code review that a frame which happens to decode
    // to zero bytes (a real, valid LZ4 Frame shape: header + immediate
    // EndMark) makes one `read_to_end` call return `Ok(0)` for that frame
    // without erroring and without necessarily advancing into the next
    // frame yet, which the old `out.len() == before` check read as "no more
    // frames" -- silently dropping every subsequent concatenated frame with
    // no error at all. Same silent-truncation shape as the original
    // multi-frame bug this loop exists to fix. Looping while the reader
    // still has bytes left (regardless of whether the last call grew `out`)
    // is the correct fix -- verified against a zero-content frame sandwiched
    // between two real ones, see `decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle`.
    while !decoder.get_ref().is_empty() {
        decoder
            .read_to_end(&mut out)
            .map_err(|e| ApiError::permanent(format!("LZ4 frame decompress failed: {e}")))?;
    }
    Ok(Bytes::from(out))
}

impl DbClient {
    /// Unauthenticated -- external links are presigned blob-storage URLs,
    /// same as `_fetch_link_bytes` in the Python client. `compressed` decodes
    /// the server's LZ4 Frame wrapper (see `client/sea.rs`'s `execute_statement`'s
    /// `result_compression` request) before handing bytes onward -- the
    /// downloaded file is smaller over the wire, but its content is opaque
    /// (Arrow-IPC or JSON, depending on `format`) until unwrapped here.
    /// Decompression runs on `spawn_blocking`, same reasoning as
    /// `pipeline/reorder.rs`'s Arrow-IPC decode: it's real CPU work (multiple LZ4
    /// frames per chunk, see `decompress_lz4_frame`), and running it inline
    /// would block this task's tokio worker thread from polling anything
    /// else scheduled on it -- other concurrent chunk fetches, heartbeat
    /// timers -- for however long that takes.
    pub(crate) async fn fetch_link_bytes(
        &self,
        url: &str,
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Bytes, ApiError> {
        self.retry_call_tracked(Some(stats), || async {
            let resp = self
                .http
                .get(url)
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text, true));
            }
            let bytes = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))?;
            // Counted here, before decompression -- "downloaded" means bytes
            // actually received off the wire, which is exactly the smaller,
            // (usually) LZ4-compressed size `compress_results` exists to
            // shrink -- see `QueryStats.bytes_downloaded`'s own doc comment
            // in `lib.rs`.
            stats.bytes_downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if !compressed {
                return Ok(bytes);
            }
            tokio::task::spawn_blocking(move || decompress_lz4_frame(&bytes))
                .await
                .map_err(join_error)?
        })
        .await
    }

    /// Downloads one cloud-fetch link, splitting it across parallel HTTP
    /// Range requests when the shared `download_slots` budget has room to
    /// spare. `split_limit` reserves a fair share for the other links in
    /// the same discovered batch; available permits alone do not tell us
    /// how many workers have yet to start requesting their own slots.
    pub(crate) async fn fetch_link_bytes_budgeted(
        self: &Arc<Self>,
        url: &str,
        compressed: bool,
        split_limit: usize,
        stats: &Arc<QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        // One permit per file is mandatory. This can wait because other
        // files' extra Range requests consume the same shared budget.
        let _base = self
            .download_slots
            .acquire()
            .await
            .map_err(|e| ApiError::permanent(format!("download_slots semaphore closed unexpectedly: {e}")))?;
        let want = (MAX_SPLIT_PARTS.min(split_limit.max(1)) - 1).min(self.download_slots.available_permits());
        let extra = if want > 0 {
            self.download_slots.try_acquire_many(want as u32).ok()
        } else {
            None
        };
        let parts = 1 + extra.as_ref().map(|p| p.num_permits()).unwrap_or(0);
        if parts <= 1 {
            return self.fetch_link_bytes(url, compressed, stats).await;
        }
        self.fetch_link_bytes_split(url, compressed, 1 << 20, parts as u64, stats)
            .await
    }

    /// Downloads a `part_size` probe and spreads the remainder across
    /// bounded parallel Range requests, concatenated in order. The real object size
    /// is learned from the first range response's `Content-Range` header --
    /// the Thrift link's own `bytesNum` is the *uncompressed* row-set size,
    /// not the file's size on blob storage, and using it produces HTTP 416.
    ///
    /// A server that answers the probe with `200 OK` instead of `206
    /// Partial Content` (Range unsupported) falls back to treating the
    /// whole response as the complete file -- correct, since it ignored
    /// Range and sent everything. But a server that *does* answer `206`
    /// with a `Content-Range` header this code can't parse is a different,
    /// unsafe case: silently treating the first `part_size` bytes as the
    /// whole file would truncate the real result with no error. That's
    /// treated as a hard failure instead, not a silent truncation --
    /// confirmed unreachable against real Azure Blob Storage (always
    /// returns a well-formed `bytes start-end/total`), but this is a
    /// third-party response shape, not something this crate controls.
    pub(crate) async fn fetch_link_bytes_split(
        self: &Arc<Self>,
        url: &str,
        compressed: bool,
        part_size: u64,
        max_parts: u64,
        stats: &Arc<QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        // Retry the probe as one request (headers and body). Start tail
        // downloads as soon as its headers reveal the file size, so reading
        // the first MiB overlaps the rest. JoinSet aborts those requests if
        // the probe fails and retries, or the surrounding fetch is cancelled.
        let (total, head, mut downloads) = self
            .retry_call_tracked(Some(stats), || async {
                let resp = self
                    .http
                    .get(url)
                    .header("Range", format!("bytes=0-{}", part_size - 1))
                    .timeout(self.http_timeout)
                    .send()
                    .await
                    .map_err(|e| ApiError::from_reqwest(e, true))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(ApiError::from_status(status, &text, true));
                }
                let ranged = status == reqwest::StatusCode::PARTIAL_CONTENT;
                let total: Option<u64> = resp
                    .headers()
                    .get(reqwest::header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.rsplit('/').next().and_then(|t| t.parse().ok()));
                if ranged && total.is_none() {
                    return Err(ApiError::permanent(
                        "cloud-fetch link answered a Range request with 206 Partial Content but an \
                         unparseable Content-Range header -- refusing to silently return a truncated file",
                    ));
                }

                let mut downloads = tokio::task::JoinSet::new();
                if let Some(total) = ranged.then_some(total).flatten() {
                    let remaining = total.saturating_sub(part_size);
                    let n_rest = remaining.div_ceil(part_size).min(max_parts.saturating_sub(1));
                    let rest_size = if n_rest == 0 { 0 } else { remaining.div_ceil(n_rest) };
                    let mut start = part_size;
                    for index in 0..n_rest {
                        if start >= total {
                            break;
                        }
                        let end = (start + rest_size - 1).min(total - 1);
                        let this = self.clone();
                        let url = url.to_string();
                        let part_stats = stats.clone();
                        downloads.spawn(async move {
                            let bytes = this
                                .retry_call_tracked(Some(&part_stats), || async {
                                    let resp = this
                                        .http
                                        .get(&url)
                                        .header("Range", format!("bytes={start}-{end}"))
                                        .timeout(this.http_timeout)
                                        .send()
                                        .await
                                        .map_err(|e| ApiError::from_reqwest(e, true))?;
                                    let status = resp.status();
                                    if !status.is_success() {
                                        let text = resp.text().await.unwrap_or_default();
                                        return Err(ApiError::from_status(status, &text, true));
                                    }
                                    resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))
                                })
                                .await;
                            if let Ok(b) = &bytes {
                                part_stats.bytes_downloaded.fetch_add(b.len() as u64, Ordering::Relaxed);
                            }
                            (index, bytes)
                        });
                        start = end + 1;
                    }
                }
                let head = match resp.bytes().await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        // Finish cancelling the old parts before retrying
                        // the probe under the same download-slot budget.
                        downloads.shutdown().await;
                        return Err(ApiError::from_reqwest(error, true));
                    }
                };
                Ok((total, head, downloads))
            })
            .await?;
        stats.bytes_downloaded.fetch_add(head.len() as u64, Ordering::Relaxed);

        let bytes = if downloads.is_empty() {
            head
        } else {
            let mut parts = Vec::with_capacity(downloads.len());
            while let Some(result) = downloads.join_next().await {
                let (index, bytes) = result.map_err(join_error)?;
                parts.push((index, bytes?));
            }
            parts.sort_unstable_by_key(|(index, _)| *index);
            let mut out = bytes::BytesMut::with_capacity(total.unwrap_or(head.len() as u64) as usize);
            out.extend_from_slice(&head);
            for (_, part) in parts {
                out.extend_from_slice(&part);
            }
            out.freeze()
        };
        if let Some(t) = total
            && bytes.len() as u64 != t
        {
            return Err(ApiError::permanent(format!(
                "split download assembled {} bytes, expected {t}",
                bytes.len()
            )));
        }
        if !compressed {
            return Ok(bytes);
        }
        tokio::task::spawn_blocking(move || decompress_lz4_frame(&bytes))
            .await
            .map_err(join_error)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "manual release-mode decompression allocation experiment"]
    fn benchmark_lz4_capacity_hypotheses() {
        use std::hint::black_box;
        use std::io::{Read, Write};
        use std::time::Instant;

        // Deterministic random bytes mixed with repeated bytes cover different
        // compression ratios without depending on warehouse data.
        for random_fraction in [0, 25, 50, 100] {
            let mut state = 0x12345678_u64;
            let input: Vec<u8> = (0..8 * 1024 * 1024)
                .map(|i| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    if i % 100 < random_fraction { state as u8 } else { 0 }
                })
                .collect();
            let mut compressed = Vec::new();
            for part in input.chunks(512 * 1024) {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder.write_all(part).unwrap();
                compressed.extend(encoder.finish().unwrap());
            }
            let variants = [
                ("compressed_x1", compressed.len()),
                ("compressed_x2", compressed.len() * 2),
                ("compressed_x4", compressed.len() * 4),
                ("exact", input.len()),
                ("short_hint", input.len() - 64),
            ];
            for round in 0..10 {
                for index in 0..variants.len() {
                    let (variant, capacity) = variants[(index + round) % variants.len()];
                    let start = Instant::now();
                    let mut output = Vec::with_capacity(capacity);
                    let mut decoder = lz4_flex::frame::FrameDecoder::new(black_box(compressed.as_slice()));
                    while !decoder.get_ref().is_empty() {
                        decoder.read_to_end(&mut output).unwrap();
                    }
                    let seconds = start.elapsed().as_secs_f64();
                    assert_eq!(output, input);
                    if round > 1 {
                        println!(
                            "{}",
                            serde_json::json!({
                                "random_percent": random_fraction, "variant": variant,
                                "round": round, "seconds": seconds,
                                "compressed_bytes": compressed.len(), "decoded_bytes": output.len(),
                                "retained_capacity": output.capacity()
                            })
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn split_download_starts_tail_requests_before_the_probe_body_finishes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        for fail_first_probe in [false, true] {
            let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let server_probes = probes.clone();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let tail_started = Arc::new(tokio::sync::Notify::new());
            tokio::spawn(async move {
                for _ in 0..if fail_first_probe { 6 } else { 3 } {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let tail_started = tail_started.clone();
                    let probes = server_probes.clone();
                    tokio::spawn(async move {
                        let mut request = Vec::new();
                        while !request.ends_with(b"\r\n\r\n") {
                            request.push(socket.read_u8().await.unwrap());
                        }
                        let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
                        let range = request.lines().find(|s| s.starts_with("range:")).unwrap();
                        let (start, end) = range.split_once("bytes=").unwrap().1.split_once('-').unwrap();
                        let start: usize = start.parse().unwrap();
                        let end: usize = end.parse().unwrap();
                        let headers = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/12\r\nConnection: close\r\n\r\n",
                            end - start + 1,
                        );
                        socket.write_all(headers.as_bytes()).await.unwrap();
                        if start == 0 {
                            let first = probes.fetch_add(1, Ordering::Relaxed) == 0;
                            tail_started.notified().await;
                            if fail_first_probe && first {
                                socket.write_all(b"a").await.unwrap();
                                return; // truncated body: the whole probe must retry
                            }
                        } else {
                            tail_started.notify_one();
                        }
                        let _ = socket.write_all(&b"abcdefghijkl"[start..=end]).await;
                    });
                }
            });
            let client = Arc::new(
                DbClient::new(&format!("http://{address}"), "wh", "token")
                    .with_retry_attempts(2)
                    .with_retry_max_wait_s(0.0),
            );
            let stats = Arc::new(QueryStatsAccumulator::default());
            let bytes = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.fetch_link_bytes_split(&format!("http://{address}/blob"), false, 4, 3, &stats),
            )
            .await
            .expect("tail requests must start from the probe headers, without waiting for its body")
            .unwrap();
            assert_eq!(&bytes[..], b"abcdefghijkl");
            assert_eq!(probes.load(Ordering::Relaxed), if fail_first_probe { 2 } else { 1 });
            assert_eq!(stats.retry_count.load(Ordering::Relaxed), u32::from(fail_first_probe));
            assert!(stats.bytes_downloaded.load(Ordering::Relaxed) >= 12);
        }
    }

    /// Regression test for a real bug found by testing against an actual
    /// Databricks workspace (not just synthetic single-frame test data): a
    /// real chunk's LZ4 compression is several frames concatenated back to
    /// back (a 50k-row chunk came back as 18), and `decompress_lz4_frame`
    /// originally only decoded the first one via one `read_to_end` call --
    /// silently truncating to just that frame's ~200 bytes (the Arrow schema
    /// message, no row data) with no error at all, which `pipeline/reorder.rs`'s
    /// `decode_chunk` then happily decoded as an empty result.
    #[test]
    fn decompress_lz4_frame_reads_every_concatenated_frame() {
        use std::io::Write;

        fn compress_one_frame(data: &[u8]) -> Vec<u8> {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        let part_a = b"the quick brown fox jumps over the lazy dog ".repeat(50);
        let part_b = b"pack my box with five dozen liquor jugs ".repeat(50);
        let part_c = b"how vexingly quick daft zebras jump ".repeat(50);
        let mut concatenated_frames = Vec::new();
        concatenated_frames.extend(compress_one_frame(&part_a));
        concatenated_frames.extend(compress_one_frame(&part_b));
        concatenated_frames.extend(compress_one_frame(&part_c));

        let decompressed = decompress_lz4_frame(&Bytes::from(concatenated_frames)).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&part_a);
        expected.extend_from_slice(&part_b);
        expected.extend_from_slice(&part_c);
        assert_eq!(decompressed, Bytes::from(expected));
    }

    /// Regression test for a bug found in code review: a real, valid LZ4
    /// Frame that happens to decode to zero bytes (a header immediately
    /// followed by an EndMark -- a legal frame shape, not malformed input)
    /// makes `read_to_end` return `Ok(0)` for that frame without erroring.
    /// The old loop read "output didn't grow" as "no more frames" and
    /// stopped there, silently dropping every frame concatenated after the
    /// empty one. `decompress_lz4_frame` must keep going as long as the
    /// underlying reader still has bytes left, not just as long as output
    /// keeps growing.
    #[test]
    fn decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle() {
        use std::io::Write;

        fn compress_one_frame(data: &[u8]) -> Vec<u8> {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        let part_a = b"the quick brown fox jumps over the lazy dog ".repeat(50);
        let part_b = b"pack my box with five dozen liquor jugs ".repeat(50);
        let mut concatenated_frames = Vec::new();
        concatenated_frames.extend(compress_one_frame(&part_a));
        concatenated_frames.extend(compress_one_frame(b"")); // real frame, zero content
        concatenated_frames.extend(compress_one_frame(&part_b));

        let decompressed = decompress_lz4_frame(&Bytes::from(concatenated_frames)).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&part_a);
        expected.extend_from_slice(&part_b);
        assert_eq!(
            decompressed,
            Bytes::from(expected),
            "the frame after the zero-content one must not be silently dropped"
        );
    }

    /// Regression test for a real bug found against a real workspace: a
    /// 400-chunk/5.6M-row fetch failed permanently on one of many large
    /// concurrent blob downloads with reqwest's "error decoding response
    /// body" -- a connection that closes early mid-body (`Kind::Decode`,
    /// see `ApiError::from_reqwest`'s comment), not a genuinely dead
    /// endpoint. Before the fix, `transient` was unconditionally `false` for
    /// every reqwest error, so `retry_call` never got a second attempt and
    /// the whole query failed outright. A raw truncated-response server
    /// (rather than wiremock, which doesn't expose a way to violate its own
    /// Content-Length) reproduces the same client-side error reqwest raised
    /// against the real blob storage endpoint.
    #[tokio::test]
    async fn fetch_link_bytes_retries_after_a_connection_closed_mid_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                attempt += 1;
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                if attempt == 1 {
                    // Claims 100 bytes, sends 10, then closes -- reqwest's
                    // `.bytes()` surfaces exactly this as `Kind::Decode`.
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
                        .await;
                    let _ = socket.shutdown().await;
                } else {
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                        .await;
                    let _ = socket.shutdown().await;
                    return;
                }
            }
        });

        let client = DbClient::new(&format!("http://{addr}"), "wh-test", "fake-token");
        let stats = QueryStatsAccumulator::default();
        let bytes = client
            .fetch_link_bytes(&format!("http://{addr}/data"), false, &stats)
            .await
            .expect("must retry past the truncated first attempt and succeed on the second");
        assert_eq!(&bytes[..], b"hello");
        assert_eq!(
            stats.retry_count.load(Ordering::Relaxed),
            1,
            "the one retry after the truncated first attempt must be counted"
        );
        assert_eq!(bytes.len() as u64, stats.bytes_downloaded.load(Ordering::Relaxed));
    }
}

/// Property-based fuzzing for `decompress_lz4_frame` -- the other real,
/// previously-shipped bug class this crate's hand-rolled/third-party-library-
/// adjacent parsing code has already produced (the multi-frame truncation
/// bug documented on this function's own call site in `execute_statement`'s
/// doc comment, and `decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle`
/// above -- both found by real-workspace testing, not fuzzing). Separate
/// `#[cfg(test)]` module from `mod tests` above for the same reason
/// `thrift.rs`/`json_convert.rs`'s own `mod proptests` are split out.
#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Arbitrary/malformed/truncated/empty bytes reinterpreted as an LZ4
        /// Frame: must never panic (most inputs are simply not a valid LZ4
        /// Frame at all and should return `Err`; a few short/empty inputs
        /// are legal-but-trivial frames and should return `Ok` with little
        /// or no content -- either outcome is fine, only a panic is a bug).
        #[test]
        fn decompress_lz4_frame_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = decompress_lz4_frame(&Bytes::from(bytes));
        }

        /// A *truncated* real frame -- compress real data, then chop the
        /// tail off at an arbitrary point -- targeting the multi-frame loop
        /// specifically (a cut mid-frame, mid-block, or exactly on a frame
        /// boundary), which arbitrary random bytes essentially never
        /// produce (LZ4 Frame's magic number alone is 4 specific bytes).
        #[test]
        fn decompress_lz4_frame_never_panics_on_a_truncated_real_frame(
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
            cut_at_fraction in 0.0f64..=1.0,
        ) {
            use std::io::Write;
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(&payload).unwrap();
            let full = encoder.finish().unwrap();
            let cut = ((full.len() as f64) * cut_at_fraction) as usize;
            let _ = decompress_lz4_frame(&Bytes::from(full[..cut].to_vec()));
        }
    }
}
