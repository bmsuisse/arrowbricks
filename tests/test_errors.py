"""Covers the typed exception hierarchy (`ArrowbricksError` and its
`TransientError`/`AuthError`/`StatementError` subclasses, `_core`-defined via
PyO3's `create_exception!` -- see `rust/arrowbricks_core/src/lib.rs`'s own
"Typed error taxonomy" section) end to end through the real public API, not
just at the Rust unit-test level. `retry_attempts`/`retry_max_wait_s` are
used throughout purely to keep these tests fast (an exhausted-retries
scenario would otherwise take real wall-clock seconds/minutes at the default
policy) -- `tests/test_retry.py` is where those two kwargs are actually
under test."""

from __future__ import annotations

import pytest
from conftest import WAREHOUSE_ID, Response

from arrowbricks import ArrowbricksError, AuthError, DatabricksClient, StatementError, TransientError
from arrowbricks.cursor import Cursor


@pytest.mark.asyncio
async def test_401_raises_auth_error(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    statements_route = server.post("/api/2.0/sql/statements").mock(Response(status=401, content=b"unauthorized"))
    # retry_attempts=1: 401 is internally retryable (a retry re-fetches the
    # token -- see client.rs's `from_status`), which would otherwise mean
    # this test pays the real exponential-backoff wall-clock cost before
    # AuthError is ever raised.
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=1)
    cursor = Cursor(client)

    with pytest.raises(AuthError) as exc_info:
        await cursor.execute("SELECT 1")

    assert isinstance(exc_info.value, ArrowbricksError)
    assert isinstance(exc_info.value, RuntimeError), "must stay catchable by a pre-existing `except RuntimeError`"
    assert statements_route.call_count == 1


@pytest.mark.asyncio
async def test_token_provider_raising_surfaces_as_auth_error(mock_server):
    """Regression test found in code review: a `token_provider` that itself
    raises (e.g. its own OAuth token refresh comes back unauthorized) used
    to always surface as the generic `ArrowbricksError` base, never
    `AuthError` -- breaking README.md's documented `except AuthError:
    refresh_credentials()` pattern for exactly the case a caller would most
    want it to work. `py_err_to_api_error`'s only caller is
    `PyTokenProvider::get_token`, so classifying every `PyErr` reaching it as
    `ApiErrorKind::Auth` is justified by that calling context alone."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))

    def bad_provider() -> str:
        raise RuntimeError("token refresh failed: 401 unauthorized")

    client = DatabricksClient(server.host, WAREHOUSE_ID, token_provider=bad_provider, protocol="sea")
    cursor = Cursor(client)

    with pytest.raises(AuthError) as exc_info:
        await cursor.execute("SELECT 1")

    assert isinstance(exc_info.value, ArrowbricksError)
    assert isinstance(exc_info.value, RuntimeError)
    assert "token refresh failed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_403_raises_auth_error(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(Response(status=403, content=b"forbidden"))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=1)
    cursor = Cursor(client)

    with pytest.raises(AuthError):
        await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_failed_statement_raises_statement_error(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-failed",
                "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
            }
        )
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    cursor = Cursor(client)

    with pytest.raises(StatementError, match="SYNTAX_ERROR") as exc_info:
        await cursor.execute("not valid sql")

    assert isinstance(exc_info.value, ArrowbricksError)
    assert isinstance(exc_info.value, RuntimeError)


@pytest.mark.asyncio
async def test_canceled_statement_raises_statement_error(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(json_body={"statement_id": "stmt-canceled", "status": {"state": "CANCELED"}})
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    cursor = Cursor(client)

    with pytest.raises(StatementError, match="canceled"):
        await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_persistent_429_raises_transient_error(mock_server):
    # 429 (unlike a 5xx on this same POST) is unconditionally transient
    # regardless of the statement-submit POST's own `idempotent=false` --
    # see `ApiError::from_status`'s doc comment -- so it's the fast, reliable
    # choice here for a status that actually retries.
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    statements_route = server.post("/api/2.0/sql/statements").mock(Response(status=429, content=b"slow down"))
    client = DatabricksClient(
        server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=2, retry_max_wait_s=0.01
    )
    cursor = Cursor(client)

    with pytest.raises(TransientError) as exc_info:
        await cursor.execute("SELECT 1")

    assert isinstance(exc_info.value, ArrowbricksError)
    assert isinstance(exc_info.value, RuntimeError)
    assert statements_route.call_count == 2, "must have actually retried once before giving up"


@pytest.mark.asyncio
async def test_generic_400_raises_plain_arrowbricks_error(mock_volume_files):
    """A non-auth, non-transient, non-statement failure (upload/delete's own
    `ApiError`s never carry `ApiErrorKind::Statement` -- there's no SQL
    statement involved) still raises the plain base class, not a more
    specific subclass it doesn't actually match."""
    server, _put_route, delete_route = mock_volume_files()
    delete_route.mock(Response(status=400))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    with pytest.raises(ArrowbricksError, match="400") as exc_info:
        await client.delete_volume_file("/Volumes/cat/schema/vol/forbidden.parquet")

    assert not isinstance(exc_info.value, (AuthError, StatementError, TransientError))
    assert isinstance(exc_info.value, RuntimeError)
