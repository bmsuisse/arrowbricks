"""Covers `retry_attempts`/`retry_max_wait_s`, the configurable counterparts
to `client.rs`'s old compile-time `RETRY_ATTEMPTS`/`RETRY_MAX_WAIT_S`
constants -- threaded through the exact same "Rust constant -> PyO3 default
-> `client.py` kwarg default -> `_core.pyi` stub" pattern
`chunk_fetch_concurrency` already uses (see AGENTS.md's own entry on that
one's footgun: a default that's silently unused because a different layer's
default always wins). Every test here uses 429 on the statement-submit POST,
not a 5xx -- see `test_errors.py`'s own comment on why 429 is the reliable
choice for exercising this path's retry loop (unconditionally transient,
unlike a 5xx on a non-idempotent POST)."""

from __future__ import annotations

import pytest
from conftest import WAREHOUSE_ID, Response

from arrowbricks import DatabricksClient, TransientError
from arrowbricks.cursor import Cursor


def _persistently_failing_statements_server(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    statements_route = server.post("/api/2.0/sql/statements").mock(Response(status=429, content=b"slow down"))
    return server, statements_route


@pytest.mark.asyncio
async def test_default_retry_attempts_is_six(mock_server):
    server, statements_route = _persistently_failing_statements_server(mock_server)
    # retry_max_wait_s overridden to keep this fast -- retry_attempts is left
    # at its default specifically to prove *that* default is 6, independent
    # of the wait-cap kwarg under test in the next case.
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_max_wait_s=0.01)
    cursor = Cursor(client)

    with pytest.raises(TransientError):
        await cursor.execute("SELECT 1")

    assert statements_route.call_count == 6


@pytest.mark.asyncio
async def test_retry_attempts_override_gives_up_after_exactly_n(mock_server):
    server, statements_route = _persistently_failing_statements_server(mock_server)
    client = DatabricksClient(
        server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=2, retry_max_wait_s=0.01
    )
    cursor = Cursor(client)

    with pytest.raises(TransientError):
        await cursor.execute("SELECT 1")

    assert statements_route.call_count == 2, "retry_attempts=2 must give up after exactly 2 attempts, not the default 6"


@pytest.mark.asyncio
async def test_retry_attempts_of_one_never_retries(mock_server):
    server, statements_route = _persistently_failing_statements_server(mock_server)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=1)
    cursor = Cursor(client)

    with pytest.raises(TransientError):
        await cursor.execute("SELECT 1")

    assert statements_route.call_count == 1


def test_retry_attempts_zero_rejected():
    with pytest.raises(ValueError, match="retry_attempts"):
        DatabricksClient("http://fake", WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=0)


def test_retry_attempts_negative_rejected_with_value_error_not_overflow_error():
    """Regression test: `retry_attempts` used to be typed `u32` at the PyO3
    boundary, so a negative Python int failed PyO3's own argument conversion
    with `OverflowError` *before* the hand-written `ValueError` check ever
    ran -- contradicting the `ValueError`-for-bad-retry-config contract
    documented in client.py/README.md/CHANGELOG.md (`OverflowError` is not a
    `ValueError` subclass, so `except ValueError` written against that
    contract wouldn't have caught it). Now typed `i64` so a negative value
    reaches the real validation and raises `ValueError` as documented."""
    with pytest.raises(ValueError, match="retry_attempts"):
        DatabricksClient("http://fake", WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=-1)


def test_retry_max_wait_s_negative_rejected():
    with pytest.raises(ValueError, match="retry_max_wait_s"):
        DatabricksClient("http://fake", WAREHOUSE_ID, token="test-token", protocol="sea", retry_max_wait_s=-1.0)
