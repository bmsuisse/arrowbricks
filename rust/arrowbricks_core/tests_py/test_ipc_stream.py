"""Python-level tests for the free functions write_ipc_stream/read_ipc_stream
-- write_ipc_stream accepts any object implementing __arrow_c_stream__ (not
just this crate's own Table return type) and writes uncompressed Arrow-IPC
stream bytes; read_ipc_stream is its exact inverse, parsing raw bytes back
into a Table with no dependency needed regardless of where the bytes came
from. Together these are what let ReplayableArrowChunk work without arro3-io
(dropped in the 1.0.0 dependency-elimination pass -- this pairing was missing
a read-side replacement at first, breaking ReplayableArrowChunk in 1.0.0;
these tests exist so that regression can't recur silently)."""

import io

import arro3.core as core
import arro3.io as aio

from arrowbricks import _core as arrowbricks_core


def test_write_ipc_stream_round_trips_an_arro3_table():
    table = core.Table.from_pydict({"id": core.Array([1, 2, 3], type=core.DataType.int64())})
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)

    buf.seek(0)
    reader = aio.read_ipc_stream(buf)
    round_tripped = core.Table.from_arrow(reader)
    assert round_tripped["id"].to_pylist() == [1, 2, 3]


def test_write_ipc_stream_is_uncompressed():
    """arro3's own default (compression="LZ4") is opaque to some Arrow IPC
    readers (e.g. duckdb-wasm) -- this crate's writer must never compress."""
    table = core.Table.from_pydict({"id": core.Array(list(range(1000)), type=core.DataType.int64())})
    uncompressed = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, uncompressed)

    compressed = io.BytesIO()
    aio.write_ipc_stream(table, compressed, compression="LZ4")

    assert len(uncompressed.getvalue()) > len(compressed.getvalue()), (
        "expected our writer's uncompressed output to be larger than an LZ4-compressed reference"
    )


def test_read_ipc_stream_parses_bytes_written_by_write_ipc_stream():
    table = core.Table.from_pydict({
        "id": core.Array([1, 2, 3], type=core.DataType.int64()),
        "label": core.Array(["a", "b", "c"], type=core.DataType.string()),
    })
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)

    parsed = arrowbricks_core.read_ipc_stream(buf.getvalue())
    assert parsed.num_rows == 3
    assert core.Table.from_arrow(parsed)["id"].to_pylist() == [1, 2, 3]


def test_read_ipc_stream_result_is_callable_more_than_once():
    """The whole point of ReplayableArrowChunk: re-parsing the same bytes
    must be safe to do repeatedly (a schema peek, then the actual scan --
    DuckDB's registration path does this)."""
    table = core.Table.from_pydict({"id": core.Array([1, 2, 3], type=core.DataType.int64())})
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    data = buf.getvalue()

    first = arrowbricks_core.read_ipc_stream(data)
    second = arrowbricks_core.read_ipc_stream(data)
    assert first.num_rows == 3
    assert second.num_rows == 3
