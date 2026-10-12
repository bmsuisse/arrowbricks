"""SQL input handling (`_sql.py`): plain `str` passthrough, PEP 750 t-strings
bound as named parameters, sqlglot expressions. t-strings need Python 3.14+,
so Templates are built through their constructor here (a `t"..."` literal is a
SyntaxError on older Pythons); one test below evals a real literal."""

from __future__ import annotations

import datetime
import json
import sys
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from conftest import WAREHOUSE_ID, Request, Response

from arrowbricks import DatabricksClient, unsafe_sql
from arrowbricks._sql import prepare_sql
from arrowbricks.cursor import Cursor

if TYPE_CHECKING:
    from string.templatelib import Interpolation, Template
else:
    try:
        from string.templatelib import Interpolation, Template
    except ImportError:  # Python < 3.14
        Interpolation = Template = None

needs_t_strings = pytest.mark.skipif(Template is None, reason="t-strings need Python 3.14+")


def tpl(*parts: str | tuple[Any, ...]) -> Template:
    """tpl("select ", (value, "value"), " from t") -- strings and (value, expr[, spec]) pairs."""
    return Template(
        *[Interpolation(p[0], p[1], None, p[2] if len(p) > 2 else "") if isinstance(p, tuple) else p for p in parts]
    )


def test_str_and_explicit_parameters_pass_through_unchanged():
    params = [{"name": "x", "value": "1", "type": "INT"}]
    assert prepare_sql("select :x", params) == ("select :x", params)
    assert prepare_sql("select 1", None) == ("select 1", None)


def test_unsafe_sql_is_identity():
    assert unsafe_sql("select 1") == "select 1"


@needs_t_strings
def test_interpolations_become_named_parameters():
    sql, params = prepare_sql(tpl("select * from t where a = ", (5, "a"), " and b = ", ("x'; drop table t; --", "b")))
    assert sql == "select * from t where a = :ab_p0 and b = :ab_p1"
    assert params == [
        {"name": "ab_p0", "value": "5", "type": "BIGINT"},
        {"name": "ab_p1", "value": "x'; drop table t; --", "type": "STRING"},
    ]


@needs_t_strings
def test_template_without_interpolations_keeps_explicit_parameters_untouched():
    assert prepare_sql(tpl("select 1"), None) == ("select 1", None)


@needs_t_strings
@pytest.mark.parametrize(
    ("value", "expected_value", "expected_type"),
    [
        (None, None, "STRING"),
        (True, "true", "BOOLEAN"),
        (False, "false", "BOOLEAN"),
        (-7, "-7", "BIGINT"),
        (2**63, str(2**63), "DECIMAL(38,0)"),
        (1.5, "1.5", "DOUBLE"),
        (float("nan"), "NaN", "DOUBLE"),
        (float("inf"), "Infinity", "DOUBLE"),
        (float("-inf"), "-Infinity", "DOUBLE"),
        (Decimal("12.340"), "12.340", "DECIMAL(5,3)"),
        (Decimal("0.05"), "0.05", "DECIMAL(2,2)"),
        (Decimal("1E+3"), "1000", "DECIMAL(4,0)"),
        ("héllo", "héllo", "STRING"),
        (datetime.date(2026, 10, 12), "2026-10-12", "DATE"),
        (datetime.datetime(2026, 10, 12, 8, 30), "2026-10-12 08:30:00", "TIMESTAMP_NTZ"),
        (
            datetime.datetime(2026, 10, 12, 8, 30, tzinfo=datetime.UTC),
            "2026-10-12 08:30:00+00:00",
            "TIMESTAMP",
        ),
        (uuid.UUID(int=1), "00000000-0000-0000-0000-000000000001", "STRING"),
    ],
)
def test_value_types(value, expected_value, expected_type):
    _, params = prepare_sql(tpl("select ", (value, "v")))
    assert params == [{"name": "ab_p0", "value": expected_value, "type": expected_type}]


@needs_t_strings
@pytest.mark.parametrize("value", [b"bytes", [1, 2], {"a": 1}, datetime.time(1), datetime.timedelta(1), object()])
def test_unsupported_value_types_raise_naming_the_expression(value):
    with pytest.raises(TypeError, match=r"can't bind \{v\}"):
        prepare_sql(tpl("select ", (value, "v")))


@needs_t_strings
@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
def test_non_finite_decimal_raises(value):
    with pytest.raises(ValueError, match="non-finite"):
        prepare_sql(tpl("select ", (value, "v")))


@needs_t_strings
def test_too_wide_numbers_raise():
    with pytest.raises(OverflowError):
        prepare_sql(tpl("select ", (10**40, "v")))
    with pytest.raises(OverflowError):
        prepare_sql(tpl("select ", (Decimal("1." + "1" * 40), "v")))


@needs_t_strings
def test_identifier_spec_quotes_with_backticks_and_escapes_them():
    sql, params = prepare_sql(tpl("select * from ", ("my`table", "t", "i"), ".", ("x", "u", "i")))
    assert sql == "select * from `my``table`.`x`"
    assert params is None


@needs_t_strings
@pytest.mark.parametrize("value", [5, "", None])
def test_identifier_must_be_non_empty_str(value):
    with pytest.raises(TypeError, match="identifier"):
        prepare_sql(tpl("select * from ", (value, "t", "i")))


@needs_t_strings
@pytest.mark.parametrize("spec", ["d", "q", "x"])
def test_unknown_format_spec_raises(spec):
    with pytest.raises(ValueError, match="format spec"):
        prepare_sql(tpl("select ", (1, "v", spec)))


@needs_t_strings
def test_conversion_raises():
    template = Template("select ", Interpolation(1, "v", "r", ""))
    with pytest.raises(ValueError, match="conversion"):
        prepare_sql(template)


@needs_t_strings
def test_nested_template_is_spliced_and_numbering_continues():
    inner = tpl("b = ", (2, "b"), " and c = ", (3, "c"))
    sql, params = prepare_sql(tpl("select 1 where a = ", (1, "a"), " and ", (inner, "inner"), " and d = ", (4, "d")))
    assert sql == "select 1 where a = :ab_p0 and b = :ab_p1 and c = :ab_p2 and d = :ab_p3"
    assert params is not None
    assert [p["value"] for p in params] == ["1", "2", "3", "4"]


@needs_t_strings
def test_explicit_parameters_are_merged_and_reserved_names_rejected():
    explicit = [{"name": "limit", "value": "5", "type": "INT"}]
    sql, params = prepare_sql(tpl("select ", (1, "a"), " limit :limit"), explicit)
    assert sql == "select :ab_p0 limit :limit"
    assert params == [explicit[0], {"name": "ab_p0", "value": "1", "type": "BIGINT"}]
    assert explicit == [{"name": "limit", "value": "5", "type": "INT"}]  # caller's list is not mutated
    with pytest.raises(ValueError, match="reserved"):
        prepare_sql(tpl("select ", (1, "a")), [{"name": "ab_p0", "value": "1"}])


def test_non_sql_input_raises_type_error():
    with pytest.raises(TypeError, match="sql must be"):
        prepare_sql(123)
    with pytest.raises(TypeError, match="sql must be"):
        prepare_sql(b"select 1")


def test_sqlglot_expression_is_rendered_in_the_databricks_dialect():
    sqlglot = pytest.importorskip("sqlglot")
    expr = sqlglot.parse_one("SELECT x FROM t WHERE y = :p", read="databricks")
    sql, params = prepare_sql(expr, [{"name": "p", "value": "1", "type": "INT"}])
    assert sql == "SELECT x FROM t WHERE y = :p"
    assert params == [{"name": "p", "value": "1", "type": "INT"}]


@pytest.mark.skipif(sys.version_info < (3, 14), reason="t-string literal syntax needs Python 3.14+")
def test_real_t_string_literal():
    user_id, table = 42, "orders"
    template = eval('t"select * from {table:i} where id = {user_id}"', {"user_id": user_id, "table": table})  # noqa: S307
    assert prepare_sql(template) == (
        "select * from `orders` where id = :ab_p0",
        [{"name": "ab_p0", "value": "42", "type": "BIGINT"}],
    )


@needs_t_strings
@pytest.mark.asyncio
async def test_cursor_execute_sends_t_string_parameters_on_the_wire(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    bodies: list[dict] = []

    def submit(request: Request) -> Response:
        bodies.append(json.loads(request.body))
        return Response(
            json_body={
                "statement_id": "stmt-t",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [], "schema": {"columns": [{"name": "id", "type_name": "LONG"}]}},
            }
        )

    server.post("/api/2.0/sql/statements").mock(side_effect=submit)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    await Cursor(client).execute(tpl("select * from ", ("tbl", "t", "i"), " where id = ", (7, "id")), row_limit=3)

    assert len(bodies) == 1
    assert bodies[0]["statement"].startswith("SELECT * FROM (select * from `tbl` where id = :ab_p0")
    assert bodies[0]["parameters"] == [{"name": "ab_p0", "value": "7", "type": "BIGINT"}]
