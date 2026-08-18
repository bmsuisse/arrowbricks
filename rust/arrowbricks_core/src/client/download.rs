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
/// calls) -- so looping `read_to_end` on one decoder until it stops growing
/// `out` reads every frame without reconstructing a decoder per frame.
pub(crate) fn decompress_lz4_frame(compressed: &Bytes) -> Result<Bytes, ApiError> {
    use std::io::Read;
    // `compressed.len()` is a real lower bound, but LZ4 on Arrow-IPC data
    // (long dictionary/offset-buffer runs, mostly-repeated bytes) typically
    // compresses several-fold -- estimating just the lower bound means the
    // real decompressed size almost always blows past initial capacity,
    // paying for repeated doubling-and-copy growth on every chunk. `* 4` is
    // a heuristic, not a guarantee (`Vec` still grows normally if it's wrong
    // either way) -- just a better starting point than the guaranteed-too-
    // small lower bound.
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
    /// spare -- see that field's own doc comment for the measured win and
    /// why the large-result case is safe.
    pub(crate) async fn fetch_link_bytes_budgeted(
        self: &Arc<Self>,
        url: &str,
        compressed: bool,
        stats: &Arc<QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        // One permit per download is mandatory; the worker pool already
        // bounds concurrent links to `chunk_fetch_concurrency`, so this
        // never blocks in practice -- it just makes the budget accounting
        // exact. `acquire()`'s `Err` case (the semaphore closed) can't
        // happen -- nothing in this crate ever calls `.close()` on
        // `download_slots` -- but propagating it as a real error instead of
        // an `.expect()` costs nothing and avoids a panic if that ever
        // changes.
        let _base = self
            .download_slots
            .acquire()
            .await
            .map_err(|e| ApiError::permanent(format!("download_slots semaphore closed unexpectedly: {e}")))?;
        let want = (MAX_SPLIT_PARTS - 1).min(self.download_slots.available_permits());
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

    /// Downloads one cloud-fetch link as concurrent HTTP Range requests of
    /// `part_size` bytes each, concatenated in order. The real object size
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
        // First part doubles as the size probe -- same retry_call wrapping
        // every other download in this crate gets, so a transient failure
        // on the probe itself doesn't skip straight to a hard error.
        let (ranged, total, head) = self
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
                let head = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))?;
                Ok((ranged, total, head))
            })
            .await?;
        stats.bytes_downloaded.fetch_add(head.len() as u64, Ordering::Relaxed);

        if ranged && total.is_none() {
            return Err(ApiError::permanent(
                "cloud-fetch link answered a Range request with 206 Partial Content but an \
                 unparseable Content-Range header -- refusing to silently return a truncated \
                 file"
                    .to_string(),
            ));
        }

        let mut handles = Vec::new();
        if let Some(total) = ranged.then_some(total).flatten() {
            // Spread everything after the probe part evenly over at most
            // max_parts-1 further requests, so no single tail request
            // dominates the wall clock.
            let remaining = total.saturating_sub(part_size);
            let n_rest = remaining.div_ceil(part_size).min(max_parts.saturating_sub(1));
            let rest_size = if n_rest == 0 { 0 } else { remaining.div_ceil(n_rest) };
            let mut start = part_size;
            let mut n = 1u64;
            while start < total && n <= n_rest {
                let end = (start + rest_size - 1).min(total - 1);
                let this = self.clone();
                let url = url.to_string();
                let part_stats = stats.clone();
                handles.push(tokio::spawn(async move {
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
                    bytes
                }));
                start = end + 1;
                n += 1;
            }
        }

        let mut out = bytes::BytesMut::with_capacity(total.unwrap_or(head.len() as u64) as usize);
        out.extend_from_slice(&head);
        // Parts must concatenate in order, so this can't use `JoinSet`
        // (which yields in completion order) without tracking indices --
        // simpler to keep the `Vec` and explicitly `.abort()` every
        // not-yet-awaited sibling the moment one part fails, rather than
        // silently leaving them running (a bare `?` here would return
        // early and just drop the rest, which does NOT cancel them --
        // `JoinHandle::drop` detaches, it doesn't abort -- leaving up to
        // `MAX_SPLIT_PARTS - 1` sibling Range downloads, each with their
        // own `retry_call` backoff, still in flight for a link the caller
        // has already given up on).
        let mut iter = handles.into_iter();
        while let Some(h) = iter.next() {
            match h.await {
                Ok(Ok(part)) => out.extend_from_slice(&part),
                Ok(Err(e)) => {
                    for remaining in iter {
                        remaining.abort();
                    }
                    return Err(e);
                }
                Err(join_err) => {
                    for remaining in iter {
                        remaining.abort();
                    }
                    return Err(join_error(join_err));
                }
            }
        }
        let bytes = out.freeze();
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
