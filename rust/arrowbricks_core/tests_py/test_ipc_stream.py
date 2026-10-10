"""Python-level tests for the free functions write_ipc_stream/read_ipc_stream
-- write_ipc_stream accepts any object implementing __arrow_c_stream__ (not
just this crate's own Table return type) and writes uncompressed Arrow-IPC
stream bytes; read_ipc_stream is its exact inverse, parsing raw bytes back
into a Table with no dependency needed regardless of where the bytes came
from. Together these are what let ReplayableArrowChunk work without arro3-io
(dropped in the 1.0.0 dependency-elimination pass -- this pairing was missing
a read-side replacement at first, breaking ReplayableArrowChunk in 1.0.0;
these tests exist so that regression can't recur silently)."""

import gc
import io
import sys

import arro3.core as core
import arro3.io as aio
import pytest

from arrowbricks import _core as arrowbricks_core


def test_write_ipc_stream_bounds_writes_and_handles_short_writes():
    table = core.Table.from_pydict({"text": core.Array(["x" * 256] * 10000, type=core.DataType.string())})

    class ShortWriter:
        def __init__(self):
            self.output = io.BytesIO()

        def write(self, data):
            assert len(data) <= 1024 * 1024, "output must not duplicate the entire IPC stream in Python"
            accepted = min(len(data), 65536)
            return self.output.write(data[:accepted])

    sink = ShortWriter()
    arrowbricks_core.write_ipc_stream(table, sink)
    actual = core.Table.from_arrow(arrowbricks_core.read_ipc_stream(sink.output.getvalue()))
    assert actual["text"].to_pylist() == ["x" * 256] * 10000


def test_write_ipc_stream_propagates_sink_failure():
    table = core.Table.from_pydict({"id": core.Array([1], type=core.DataType.int64())})

    class FailedWriter:
        def write(self, data):
            raise OSError("disk full")

    with pytest.raises(OSError, match="disk full"):
        arrowbricks_core.write_ipc_stream(table, FailedWriter())


@pytest.mark.parametrize("count", [0, 10**9])
def test_write_ipc_stream_rejects_invalid_write_counts(count):
    table = core.Table.from_pydict({"id": core.Array([1], type=core.DataType.int64())})

    class InvalidWriter:
        def write(self, data):
            return count

    with pytest.raises(OSError, match="byte count"):
        arrowbricks_core.write_ipc_stream(table, InvalidWriter())


def test_replayed_tables_share_the_immutable_input_and_outlive_it():
    table = core.Table.from_pydict({"label": core.Array(["alpha", "beta", None], type=core.DataType.string())})
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    data = buf.getvalue()
    refs = sys.getrefcount(data)
    first = arrowbricks_core.read_ipc_stream(data)
    second = arrowbricks_core.read_ipc_stream(data)
    assert sys.getrefcount(data) >= refs + 2, "each replay must retain the input instead of copying its columns"
    del data, buf
    gc.collect()
    assert core.Table.from_arrow(first)["label"].to_pylist() == ["alpha", "beta", None]
    assert core.Table.from_arrow(second)["label"].to_pylist() == ["alpha", "beta", None]


def test_read_ipc_stream_preserves_a_schema_without_batches():
    schema = core.Schema([core.Field("id", core.DataType.int64())])
    table = core.Table.from_batches([], schema=schema)
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    parsed = core.Table.from_arrow(arrowbricks_core.read_ipc_stream(buf.getvalue()))
    assert parsed.num_rows == 0
    assert parsed.schema == schema


def test_read_ipc_stream_reads_all_batches_after_the_input_is_released():
    schema = core.Schema([core.Field("id", core.DataType.int64())])
    batches = [
        core.RecordBatch.from_pydict({"id": core.Array(values, type=core.DataType.int64())})
        for values in ([1, 2], [3, 4])
    ]
    table = core.Table.from_batches(batches, schema=schema)
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    parsed = arrowbricks_core.read_ipc_stream(buf.getvalue())
    del buf, table, batches
    gc.collect()
    assert core.Table.from_arrow(parsed)["id"].to_pylist() == [1, 2, 3, 4]


@pytest.mark.parametrize("data", [b"", b"not IPC"])
def test_read_ipc_stream_rejects_invalid_input(data):
    with pytest.raises(RuntimeError):
        arrowbricks_core.read_ipc_stream(data)


def test_read_ipc_stream_rejects_trailing_bytes():
    table = core.Table.from_pydict({"id": core.Array([1], type=core.DataType.int64())})
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    with pytest.raises(RuntimeError):
        arrowbricks_core.read_ipc_stream(buf.getvalue() + b"trailing garbage")


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
    table = core.Table.from_pydict(
        {
            "id": core.Array([1, 2, 3], type=core.DataType.int64()),
            "label": core.Array(["a", "b", "c"], type=core.DataType.string()),
        }
    )
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


def test_read_ipc_stream_allows_another_python_thread_to_run():
    import threading

    table = core.Table.from_pydict(
        {"label": core.Array(["synthetic text " * 16] * 100_000, type=core.DataType.string())}
    )
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(table, buf)
    data = buf.getvalue()
    ready = threading.Event()
    go = threading.Event()
    progressed = threading.Event()

    def other_thread():
        ready.set()
        go.wait()
        progressed.set()

    thread = threading.Thread(target=other_thread)
    thread.start()
    ready.wait()
    previous = sys.getswitchinterval()
    try:
        # Prevent a periodic Python bytecode switch from satisfying the test.
        # The decoder itself must release the interpreter for the worker.
        sys.setswitchinterval(10)
        go.set()
        result = arrowbricks_core.read_ipc_stream(data)
        ran_during_decode = progressed.is_set()
    finally:
        sys.setswitchinterval(previous)
        go.set()
        thread.join(timeout=5)
    assert result.num_rows == 100_000
    assert ran_during_decode, "IPC decoding must release the interpreter lock"


def test_table_exports_are_independent_and_outlive_the_table():
    source = core.Table.from_pydict({"id": core.Array([1, 2], type=core.DataType.int64())})
    buf = io.BytesIO()
    arrowbricks_core.write_ipc_stream(source, buf)
    table = arrowbricks_core.read_ipc_stream(buf.getvalue())
    assert len(table) == 2
    assert table.column_names == ["id"]
    # A capsule discarded without a consumer must release only its own reader.
    unused = table.__arrow_c_stream__()
    del unused
    first = table.__arrow_c_stream__(requested_schema=None)
    second = table.__arrow_c_stream__()
    del table, source, buf
    gc.collect()

    class Export:
        def __init__(self, capsule):
            self.capsule = capsule

        def __arrow_c_stream__(self, requested_schema=None):
            return self.capsule

    for capsule in (first, second):
        assert core.Table.from_arrow(Export(capsule))["id"].to_pylist() == [1, 2]
    # Each capsule can be consumed only once; a second import must error.
    with pytest.raises(RuntimeError):
        arrowbricks_core.write_ipc_stream(Export(first), io.BytesIO())


def test_write_ipc_stream_rejects_a_schema_capsule():
    class WrongCapsule:
        def __arrow_c_stream__(self):
            return core.Schema([core.Field("id", core.DataType.int64())]).__arrow_c_schema__()

    with pytest.raises(ValueError):
        arrowbricks_core.write_ipc_stream(WrongCapsule(), io.BytesIO())
