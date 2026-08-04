"""Regression suite for arrowbricks' central claim: `arro3-core` is an
*optional* extra, needed only by `Cursor.fetchone`/`fetchmany`/`fetchall`
(via `_table_to_rows`'s `table.column(i).to_pylist()`). Every other path --
`fetchall_arrow`/`fetchall_arrow_streamed`, `cursor.description`,
`ReplayableArrowChunk`, `write_ipc_stream`/`read_ipc_stream` -- is supposed to
work with zero Arrow-library Python packages installed at all.

That claim was never actually checked anywhere: every CI leg ran `uv sync
--all-extras`, so arro3-core was always present -- exactly the blind spot
that let `ReplayableArrowChunk` silently vanish from the public API in 1.0.0
with nothing catching it until a real downstream integration did (see
tests/test_replayable_arrow_chunk.py's own regression-test docstring). The
CI job running this directory (see .github/workflows/ci.yml's
`test-arro3-free` job) installs the wheel with no extras at all -- if arro3
somehow ends up imported by any of these paths again, importing arro3 itself
would raise ModuleNotFoundError and fail these tests for real."""

from __future__ import annotations

import io

import pytest

from arrowbricks import ReplayableArrowChunk, connect, write_ipc_stream


def test_arro3_is_not_importable() -> None:
    """Sanity check for the dedicated CI job (.github/workflows/ci.yml's
    `test-arro3-free`), which installs arrowbricks with no extras at all --
    skips rather than fails in a normal dev venv (arro3-core is a real `dev`
    dependency here, see pyproject.toml, so a plain `uv run pytest -q` has it
    installed) so this file stays green in the everyday local loop; the CI
    job is what actually enforces this, by construction of its own install
    step, not by this test's assertion passing there too."""
    try:
        import arro3.core  # noqa: F401
    except ModuleNotFoundError:
        return
    pytest.skip("arro3-core is installed in this environment -- run the test-arro3-free CI job to enforce this")


def test_replayable_arrow_chunk_round_trips(sample_chunk_bytes: bytes) -> None:
    chunk = ReplayableArrowChunk(sample_chunk_bytes, chunk_index=0)

    table = chunk.to_table()
    assert table.num_rows == 5
    assert chunk.nbytes() == len(sample_chunk_bytes)

    # __arrow_c_stream__ must be callable more than once (DuckDB's
    # registration path does a schema peek, then the actual scan).
    assert chunk.__arrow_c_stream__() is not None
    assert chunk.__arrow_c_stream__() is not None

    buf = io.BytesIO()
    write_ipc_stream(table, buf)
    round_tripped = ReplayableArrowChunk(buf.getvalue(), chunk_index=0).to_table()
    assert round_tripped.num_rows == 5


@pytest.mark.asyncio
async def test_cursor_fetchall_arrow_streamed_and_description(one_chunk_server, warehouse_id: str) -> None:
    from arrowbricks import HEARTBEAT

    conn = connect(host=one_chunk_server.host, warehouse_id=warehouse_id, token="fake-token")
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM whatever")

    assert cursor.description is not None
    assert [c[0] for c in cursor.description] == []  # manifest carries no schema in this fixture -- see below

    table = None
    async for item in cursor.fetchall_arrow_streamed():
        if item is not HEARTBEAT:
            table = item
    assert table is not None
    assert table.num_rows == 5
    assert table.num_columns == 2

    # description reflects the *real* fetched schema once available, not just
    # the (absent, here) manifest estimate -- see cursor.py's _schema comment.
    assert [c[0] for c in cursor.description] == ["id", "label"]
