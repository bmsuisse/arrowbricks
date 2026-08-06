"""This directory's existing tests each roll their own inline `http.server`
mock (see `test_parameters.py`/`test_streaming.py`) rather than sharing a
conftest.py fixture -- there wasn't one before this file. The one exception
added here is `mock_thrift_server`: `protocol="thrift"` mock support
(`thrift_mock.ThriftMockServer`, this directory's own copy of
`tests/thrift_mock.py` -- see that module's doc comment for why it's built
on `databricks-sql-connector`'s real installed Thrift codegen rather than a
hand-rolled one) is real, non-trivial infrastructure worth sharing across
`test_thrift.py`'s own tests rather than duplicating per test function, the
same way `tests/conftest.py`'s `mock_thrift_server` fixture is shared across
`tests/test_thrift_pipeline.py`."""

from __future__ import annotations

import pytest


@pytest.fixture
def mock_thrift_server():
    from thrift_mock import ThriftMockServer

    servers = []

    def _make(warehouse_id: str):
        server = ThriftMockServer(warehouse_id)
        servers.append(server)
        return server

    yield _make
    for server in servers:
        server.shutdown()
