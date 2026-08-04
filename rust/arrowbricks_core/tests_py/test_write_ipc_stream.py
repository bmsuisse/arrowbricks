"""Python-level test for the free function write_ipc_stream -- accepts any
object implementing __arrow_c_stream__ (not just this crate's own Table
return type) and writes uncompressed Arrow-IPC stream bytes."""

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
