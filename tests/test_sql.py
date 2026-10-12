"""SQL input handling (`_sql.py`): plain `str` passthrough, PEP 750 t-strings
bound as named parameters, sqlglot expressions. t-strings need Python 3.14+,
so Templates are built through their constructor here (a `t"..."` literal is a
SyntaxError on older Pythons); one test below evals a real literal."""

from __future__ import annotations

import datetime
import json
import shutil
import subprocess
import sys
import textwrap
import uuid
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from conftest import WAREHOUSE_ID, Request, Response

from arrowbricks import DatabricksClient, Sql, unsafe_sql
from arrowbricks._sql import Interpolation, Template, prepare_sql
from arrowbricks.cursor import Cursor

needs_t_strings = pytest.mark.skipif(sys.version_info < (3, 14), reason="t-strings need Python 3.14+")


def tpl(*parts: str | tuple[Any, ...]) -> Template:
    """tpl("select ", (value, "value"[, spec]), " from t") -- strings and (value, expr[, spec]) pairs."""
    return Template(
        *[Interpolation(p[0], p[1], None, p[2] if len(p) > 2 else "") if isinstance(p, tuple) else p for p in parts]
    )


def test_str_and_explicit_parameters_pass_through_unchanged():
    params = [{"name": "x", "value": "1", "type": "INT"}]
    assert prepare_sql("select :x", params) == ("select :x", params)
    assert prepare_sql("select 1", None) == ("select 1", None)


def test_unsafe_sql_is_identity():
    assert unsafe_sql("select 1") == "select 1"


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


@needs_t_strings
def test_interpolations_become_named_parameters():
    sql, params = prepare_sql(tpl("select * from t where a = ", (5, "a"), " and b = ", ("x'; drop table t; --", "b")))
    assert sql == "select * from t where a = :ab_p0 and b = :ab_p1"
    assert params == [
        {"name": "ab_p0", "value": "5", "type": "BIGINT"},
        {"name": "ab_p1", "value": "x'; drop table t; --", "type": "STRING"},
    ]


@needs_t_strings
def test_explicit_s_spec_binds_like_no_spec():
    assert prepare_sql(tpl("select ", (1, "a", "s")))[0] == "select :ab_p0"


@needs_t_strings
def test_template_without_interpolations_is_plain_sql():
    assert prepare_sql(tpl("select 1"), None) == ("select 1", None)


@needs_t_strings
def test_template_with_explicit_parameters_raises():
    with pytest.raises(TypeError, match="own parameters"):
        prepare_sql(tpl("select ", (1, "a")), [{"name": "x", "value": "1"}])


class _Color(str, Enum):  # noqa: UP042 -- the plain mixin is what str() mishandles
    RED = "red"


class _FakeNumpyFloat(float):
    def __repr__(self) -> str:
        return f"np.float64({float(self)})"


@needs_t_strings
@pytest.mark.parametrize(
    ("value", "expected_value", "expected_type"),
    [
        (None, None, "STRING"),
        (True, "true", "BOOLEAN"),
        (False, "false", "BOOLEAN"),
        (-7, "-7", "BIGINT"),
        (2**63 - 1, str(2**63 - 1), "BIGINT"),
        (2**63, str(2**63), "DECIMAL(38,0)"),
        (-(2**63) - 1, str(-(2**63) - 1), "DECIMAL(38,0)"),
        (1.5, "1.5", "DOUBLE"),
        (_FakeNumpyFloat(1.5), "1.5", "DOUBLE"),
        (float("nan"), "NaN", "DOUBLE"),
        (float("inf"), "Infinity", "DOUBLE"),
        (float("-inf"), "-Infinity", "DOUBLE"),
        (Decimal("12.340"), "12.340", "DECIMAL(5,3)"),
        (Decimal("0.05"), "0.05", "DECIMAL(2,2)"),
        (Decimal("1E+3"), "1000", "DECIMAL(4,0)"),
        ("héllo", "héllo", "STRING"),
        (_Color.RED, "red", "STRING"),
        (datetime.date(2026, 10, 12), "2026-10-12", "DATE"),
        (datetime.datetime(2026, 10, 12, 8, 30), "2026-10-12 08:30:00", "TIMESTAMP_NTZ"),
        (datetime.datetime(2026, 10, 12, 8, 30, tzinfo=datetime.UTC), "2026-10-12 08:30:00+00:00", "TIMESTAMP"),
        (uuid.UUID(int=1), "00000000-0000-0000-0000-000000000001", "STRING"),
    ],
)
def test_value_types(value, expected_value, expected_type):
    _, params = prepare_sql(tpl("select ", (value, "v")))
    assert params == [{"name": "ab_p0", "value": expected_value, "type": expected_type}]


@needs_t_strings
def test_datetime_whose_tzinfo_has_no_offset_is_naive():
    class NoOffset(datetime.tzinfo):
        def utcoffset(self, dt):
            return None

        def dst(self, dt):
            return None

        def tzname(self, dt):
            return None

    _, params = prepare_sql(tpl("select ", (datetime.datetime(2026, 1, 1, tzinfo=NoOffset()), "v")))
    assert params is not None
    assert params[0]["type"] == "TIMESTAMP_NTZ"


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
        prepare_sql(tpl("select ", (10**5000, "v")))  # beyond int->str digit limit, still OverflowError
    with pytest.raises(OverflowError):
        prepare_sql(tpl("select ", (Decimal("1." + "1" * 40), "v")))


@needs_t_strings
def test_identifier_spec_quotes_with_backticks_and_escapes_them():
    sql, params = prepare_sql(tpl("select * from ", ("my`table", "t", "i"), ".", ("x", "u", "i")))
    assert sql == "select * from `my``table`.`x`"
    assert params is None


@needs_t_strings
def test_identifier_tuple_is_a_qualified_name_but_a_dotted_str_is_one_identifier():
    assert prepare_sql(tpl("from ", (("cat", "sch", "tbl"), "n", "i")))[0] == "from `cat`.`sch`.`tbl`"
    assert prepare_sql(tpl("from ", ("cat.sch", "n", "i")))[0] == "from `cat.sch`"


@needs_t_strings
@pytest.mark.parametrize("value", [5, "", None, (), ("a", ""), ("a", 1)])
def test_identifier_must_be_non_empty_str_or_tuple_of_them(value):
    with pytest.raises(TypeError, match="identifier"):
        prepare_sql(tpl("select * from ", (value, "t", "i")))


@needs_t_strings
@pytest.mark.parametrize("spec", ["d", "l", "b", "x"])
def test_unknown_format_spec_raises(spec):
    with pytest.raises(ValueError, match="format spec"):
        prepare_sql(tpl("select ", (1, "v", spec)))


@needs_t_strings
def test_conversion_raises_type_error_showing_it():
    template = Template("select ", Interpolation(1, "v", "r", ""))
    with pytest.raises(TypeError, match=r"conversions not supported.*\{v!r\}"):
        prepare_sql(template)


@needs_t_strings
def test_nested_template_needs_q_spec_and_numbering_continues():
    inner = tpl("b = ", (2, "b"), " and c = ", (3, "c"))
    sql, params = prepare_sql(
        tpl("select 1 where a = ", (1, "a"), " and ", (inner, "inner", "q"), " and d = ", (4, "d"))
    )
    assert sql == "select 1 where a = :ab_p0 and b = :ab_p1 and c = :ab_p2 and d = :ab_p3"
    assert params is not None
    assert [p["value"] for p in params] == ["1", "2", "3", "4"]


@needs_t_strings
@pytest.mark.parametrize("spec", ["", "s", "i"])
def test_nested_template_without_q_raises(spec):
    with pytest.raises(ValueError, match="':q'"):
        prepare_sql(tpl("select ", (tpl("1"), "inner", spec)))


@needs_t_strings
def test_q_spec_on_a_plain_str_raises_instead_of_splicing_it():
    with pytest.raises(ValueError, match="':q'"):
        prepare_sql(tpl("select ", ("1; drop table t", "raw", "q")))


@needs_t_strings
@pytest.mark.parametrize(
    "prefix",
    ["select * from t where n like '", 'select "', "select `", "select 1 -- ", "select /* ", "select 'it\\'s "],
)
def test_interpolation_inside_quotes_or_comments_raises(prefix):
    with pytest.raises(ValueError, match="quoted string, identifier or comment"):
        prepare_sql(tpl(prefix, ("a", "name"), "%'"))


@needs_t_strings
@pytest.mark.parametrize(
    "prefix", ["select 'a' || ", "select 'it''s' = ", "select 1 /* c */ + ", "select 1 -- c\n + ", "select `a b`, "]
)
def test_closed_quotes_and_comments_before_an_interpolation_are_fine(prefix):
    assert prepare_sql(tpl(prefix, (1, "a")))[0].endswith(":ab_p0")


@needs_t_strings
@pytest.mark.parametrize(("before", "after"), [("", "abc"), ("", "1"), ("", "_x"), ("", ":ab_p1"), ("x:", "")])
def test_marker_touching_other_text_raises(before, after):
    with pytest.raises(ValueError, match="separated"):
        prepare_sql(tpl("select " + before, (1, "a"), after))


@needs_t_strings
def test_adjacent_interpolations_raise_but_punctuation_is_fine():
    with pytest.raises(ValueError, match="separated"):
        prepare_sql(tpl("select ", (1, "a"), (2, "b")))
    assert prepare_sql(tpl("select f(", (1, "a"), ",", (2, "b"), ")"))[0] == "select f(:ab_p0,:ab_p1)"


@needs_t_strings
def test_real_t_string_literal():
    user_id, table = 42, "orders"
    template = eval('t"select * from {table:i} where id = {user_id}"', {"user_id": user_id, "table": table})  # noqa: S307
    assert prepare_sql(template) == (
        "select * from `orders` where id = :ab_p0",
        [{"name": "ab_p0", "value": "42", "type": "BIGINT"}],
    )


def _sea_server(mock_server) -> tuple[Any, list[dict[str, Any]]]:
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    bodies: list[dict[str, Any]] = []

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
    return server, bodies


@needs_t_strings
@pytest.mark.asyncio
async def test_every_entry_point_sends_t_string_parameters_on_the_wire(mock_server):
    server, bodies = _sea_server(mock_server)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    def query() -> Template:
        return tpl("select * from ", ("tbl", "t", "i"), " where id = ", (7, "id"))

    await Cursor(client).execute(query(), row_limit=3)
    async for _ in Cursor(client).execute_streamed(query()):
        pass
    await client.query_agent(query())
    async for _ in client.stream_query_json(query()):
        pass
    from arrowbricks import stream_query_json

    async for _ in stream_query_json(client, query()):
        pass

    assert len(bodies) == 5
    for body in bodies:
        assert "select * from `tbl` where id = :ab_p0" in body["statement"]
        assert body["parameters"] == [{"name": "ab_p0", "value": "7", "type": "BIGINT"}]
    assert bodies[0]["statement"].startswith("SELECT * FROM (select * from `tbl`")  # row_limit wraps the rendered SQL


@pytest.mark.asyncio
async def test_str_entry_points_still_send_explicit_parameters(mock_server):
    server, bodies = _sea_server(mock_server)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    params = [{"name": "p", "value": "1", "type": "INT"}]

    await Cursor(client).execute("select :p", params)
    await client.query_agent("select :p", params=params)

    assert [b["statement"] for b in bodies] == ["select :p", "select :p"]
    assert all(b["parameters"] == params for b in bodies)


def test_sql_alias_is_exported():
    assert Sql is not None


_TYPING_PROBE = textwrap.dedent(
    """
    from arrowbricks import Cursor, DatabricksClient, Sql, unsafe_sql


    def passes_through(sql: Sql) -> Sql:
        return sql


    async def fine(cur: Cursor, client: DatabricksClient, vouched: str) -> None:
        await cur.execute("select 1")
        await cur.execute(unsafe_sql(vouched))
        await client.query_agent("select 1")


    async def flagged(cur: Cursor, client: DatabricksClient, dynamic: str) -> None:
        await cur.execute(dynamic)  # FLAG
        await cur.execute(f"select {dynamic}")  # FLAG
        await client.query_agent("select " + dynamic)  # FLAG
        client.stream_query_json(dynamic)  # FLAG
    """
)


@pytest.mark.parametrize("python_version", ["3.11", "3.12", "3.14"])
def test_type_checker_flags_runtime_strings_on_every_python_version(tmp_path: Path, python_version: str):
    """The point of typing `sql` as `LiteralString | Template | ...`: if any union
    member became unresolvable (a `Template` import that doesn't exist before
    3.14, sqlglot not installed) the union would collapse to `Unknown`, accept
    every `str`, and this guard would silently turn off."""
    ty = Path(sys.executable).parent / "ty"
    if not ty.exists():
        ty_path = shutil.which("ty")
        if ty_path is None:
            pytest.skip("ty is not installed")
        ty = Path(ty_path)
    probe = tmp_path / "probe.py"
    probe.write_text(_TYPING_PROBE)
    result = subprocess.run(  # noqa: S603
        [
            str(ty),
            "check",
            "--python",
            sys.prefix,
            "--python-version",
            python_version,
            "--output-format",
            "concise",
            str(probe),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    flagged = {
        int(line.split(":")[1])
        for line in result.stdout.splitlines()
        if "invalid-argument-type" in line and ":" in line
    }
    expected = {i for i, line in enumerate(_TYPING_PROBE.splitlines(), start=1) if line.endswith("# FLAG")}
    assert flagged == expected, result.stdout + result.stderr
