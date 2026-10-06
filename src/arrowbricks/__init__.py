# PEP 810 (Python 3.15+): defer loading these submodules until a name from
# them is first used. Older Pythons ignore this list and import eagerly.
__lazy_modules__ = ["arrowbricks._streaming", "arrowbricks.client", "arrowbricks.cursor"]

from ._core import ArrowbricksError, AuthError, QueryStats, StatementError, TransientError
from ._streaming import (
    HEARTBEAT,
    QueryTimeout,
    ReplayableArrowChunk,
    await_with_heartbeat,
    stream_query_json,
    write_ipc_stream,
)
from .client import DatabricksClient
from .cursor import Connection, Cursor, connect

__all__ = [
    "HEARTBEAT",
    "ArrowbricksError",
    "AuthError",
    "Connection",
    "Cursor",
    "DatabricksClient",
    "QueryStats",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "StatementError",
    "TransientError",
    "await_with_heartbeat",
    "connect",
    "stream_query_json",
    "write_ipc_stream",
]
