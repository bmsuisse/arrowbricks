"""Regression tests for _ResultSet's chunk reorder buffer -- specifically the
two failure modes a naive dict[int, chunk] keyed-by-index buffer gets wrong:
a chunk_index that shows up more than once (DatabricksClient._fetch_chunk_index
gathers over possibly-multiple external_links per chunk), and a chunk_index
that never shows up at all (fetch_arrow_chunks_for_statement skips falsy
blobs). Both must never lose rows -- see cursor.py's _pull_one_chunk_table
docstring."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from arrowbricks._streaming import ReplayableArrowChunk
from arrowbricks.cursor import _ResultSet


async def _aiter(chunks: list[ReplayableArrowChunk]) -> AsyncIterator[ReplayableArrowChunk]:
    for c in chunks:
        yield c


def _ids(chunk_bytes_builder, lo: int, hi: int, chunk_index: int) -> ReplayableArrowChunk:
    return ReplayableArrowChunk(chunk_bytes_builder(lo, hi), chunk_index=chunk_index)


@pytest.mark.asyncio
async def test_duplicate_chunk_index_keeps_both_blobs(chunk_bytes_builder):
    """Two separate blobs arriving under the SAME chunk_index (multiple
    external_links for one chunk) must both survive, not overwrite each
    other."""
    chunks = [
        _ids(chunk_bytes_builder, 0, 2, chunk_index=0),
        _ids(chunk_bytes_builder, 5, 7, chunk_index=0),  # same index, second blob
        _ids(chunk_bytes_builder, 2, 4, chunk_index=1),
    ]
    result = _ResultSet(schema=None, chunk_aiter=_aiter(chunks))

    table = await result.fetchall_arrow()

    assert sorted(table.column(0).combine_chunks().to_pylist()) == [0, 1, 2, 3, 5, 6]


@pytest.mark.asyncio
async def test_gap_in_chunk_index_does_not_strand_later_chunks(chunk_bytes_builder):
    """chunk_index 1 never arrives at all (e.g. its bytes came back empty and
    got filtered upstream) -- chunk 2's rows must still come out, not get
    stranded behind a hole that never fills in."""
    chunks = [
        _ids(chunk_bytes_builder, 0, 2, chunk_index=0),
        _ids(chunk_bytes_builder, 2, 4, chunk_index=2),  # index 1 is missing
    ]
    result = _ResultSet(schema=None, chunk_aiter=_aiter(chunks))

    table = await result.fetchall_arrow()

    assert sorted(table.column(0).combine_chunks().to_pylist()) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_out_of_order_arrival_still_preserves_row_order(chunk_bytes_builder):
    """The common case (no gaps, no duplicates, just network completion
    order != chunk_index order) must still come out in chunk_index order."""
    chunks = [
        _ids(chunk_bytes_builder, 4, 6, chunk_index=2),
        _ids(chunk_bytes_builder, 0, 2, chunk_index=0),
        _ids(chunk_bytes_builder, 2, 4, chunk_index=1),
    ]
    result = _ResultSet(schema=None, chunk_aiter=_aiter(chunks))

    table = await result.fetchall_arrow()

    assert table.column(0).combine_chunks().to_pylist() == [0, 1, 2, 3, 4, 5]
