"""Option values for `DatabricksClient.query_agent` and the CLI."""

from __future__ import annotations

from typing import Literal, TypeAlias, get_args

AgentFormat: TypeAlias = Literal["tsv", "markdown"]
"""`"tsv"`: tab-separated, `name:Type` header. `"markdown"`: unpadded pipe table."""

AGENT_FORMATS: tuple[AgentFormat, ...] = get_args(AgentFormat)
DEFAULT_AGENT_FORMAT: AgentFormat = "tsv"
