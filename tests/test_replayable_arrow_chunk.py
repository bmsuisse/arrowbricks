from __future__ import annotations

from arrowbricks import ReplayableArrowChunk, write_ipc_stream


def test_to_table_parses_raw_ipc_bytes(chunk_bytes_builder):
    """Regression test: ReplayableArrowChunk was silently dropped from the
    top-level `arrowbricks` package in 1.0.0's dependency-elimination pass
    (arro3-io, which its read-side relied on, was fully eliminated with no
    replacement) -- a real break for any caller storing/replaying raw
    Arrow-IPC bytes outside of a live query, not just an internal detail."""
    data = chunk_bytes_builder(0, 3)
    chunk = ReplayableArrowChunk(data, chunk_index=0)

    table = chunk.to_table()

    assert table.num_rows == 3
    assert chunk.nbytes() == len(data)
    assert chunk.chunk_index == 0


def test_arrow_c_stream_protocol_is_callable_more_than_once(chunk_bytes_builder):
    """The whole point of this class: a caller (e.g. DuckDB's registration
    path) may call __arrow_c_stream__ more than once per relation -- a
    schema peek, then the actual scan."""
    data = chunk_bytes_builder(0, 5)
    chunk = ReplayableArrowChunk(data, chunk_index=0)

    first = chunk.__arrow_c_stream__()
    second = chunk.__arrow_c_stream__()
    assert first is not None
    assert second is not None


def test_write_ipc_stream_then_replayable_chunk_round_trips(chunk_bytes_builder):
    """write_ipc_stream's own output must be exactly what ReplayableArrowChunk
    can read back -- the write/read pair this package exposes must agree
    with each other, not just with a third-party Arrow library."""
    import io

    data = chunk_bytes_builder(10, 13)
    chunk = ReplayableArrowChunk(data, chunk_index=0)
    table = chunk.to_table()

    buf = io.BytesIO()
    write_ipc_stream(table, buf)

    round_tripped = ReplayableArrowChunk(buf.getvalue(), chunk_index=0).to_table()
    assert round_tripped.num_rows == table.num_rows
