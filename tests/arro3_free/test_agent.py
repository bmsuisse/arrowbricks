"""The agent path works with no Python Arrow libraries installed."""

from arrowbricks import DatabricksClient


async def test_query_agent_needs_no_optional_arrow_packages(one_chunk_server, warehouse_id):
    client = DatabricksClient(one_chunk_server.host, warehouse_id, token="fake-token", protocol="sea")
    text = await client.query_agent("SELECT * FROM whatever")
    assert "row_0" in text and "row_4" in text
    assert "rows_shown=5" in text and "complete=true" in text
