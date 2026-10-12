"""SQL input handling: `LiteralString`, PEP 750 t-strings and sqlglot expressions.

Every public entry point that takes SQL accepts the `Sql` type below. A plain
`str` still works at runtime (it is sent unchanged), but is typed as
`LiteralString` so a type checker flags SQL built from runtime values -- the
injection bug class. Dynamic *values* go in a t-string (Python 3.14+), whose
interpolations are bound as Databricks named parameters, the same way
psycopg 3.3's t-string support works, never spliced into the SQL text:

    await cur.execute(t"SELECT * FROM {table:i} WHERE id = {user_id}")

`{x}`/`{x:s}` binds a parameter, `{x:i}` quotes an identifier, `{t:q}` splices a
nested t-string; conversions (`!r`) and other specs raise, as in psycopg.
A t-string takes no separate `parameters` -- its interpolations are the
parameters. Never build a `Template(...)` from runtime strings: unlike a t-string
literal, the type system can't tell it apart.

`prepare_sql` turns any accepted input into the `(sql, parameters)` pair the
Rust core takes -- `parameters` being Databricks' own
`[{"name": ..., "value": ..., "type": ...}]` format. Nothing below the Python
facade knows about t-strings.
"""

from __future__ import annotations

import datetime
import re
import sys
import uuid
from decimal import Decimal
from typing import Any, LiteralString, Protocol, TypeAlias, cast

if sys.version_info >= (3, 14):
    from string.templatelib import Interpolation, Template
else:
    # Stand-ins so type checkers (and `Sql` below) see a real `Template` type
    # on older Pythons, instead of an unresolved name that would silently turn
    # the whole `Sql` union into `Any` and switch the LiteralString guard off.
    # Nothing at runtime ever builds or matches these.
    from collections.abc import Iterator
    from dataclasses import dataclass
    from typing import Generic, Literal, TypeVar

    _T = TypeVar("_T")

    class Template:
        strings: tuple[str, ...]
        interpolations: tuple[Interpolation[Any], ...]

        def __new__(cls, *args: str | Interpolation[Any]) -> Template:
            raise NotImplementedError("t-strings need Python 3.14+")

        def __iter__(self) -> Iterator[str | Interpolation[Any]]:
            raise NotImplementedError

    @dataclass
    class Interpolation(Generic[_T]):
        value: _T
        expression: str
        conversion: Literal["a", "r", "s"] | None
        format_spec: str


class _SqlExpression(Protocol):
    """Structural stand-in for a sqlglot expression (`pip install arrowbricks[sqlglot]`),
    so the `Sql` union doesn't depend on sqlglot being installed. A `str` has no
    `.sql()`, so it still doesn't match."""

    def sql(self, dialect: str | None = None, **opts: Any) -> str: ...


Sql: TypeAlias = LiteralString | Template | _SqlExpression

__all__ = ["Sql", "prepare_sql", "unsafe_sql"]

_PARAM_PREFIX = "ab_p"

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_DECIMAL_MAX = 10**38

# What may not directly follow a `:ab_pN` marker: it would extend the name (or,
# for `:`, turn into Databricks' JSON-path operator).
_MARKER_CONTINUES = re.compile(r"[A-Za-z0-9_:]")
_ENDS_WITH_MARKER = re.compile(rf":{_PARAM_PREFIX}\d+$")


def unsafe_sql(sql: str) -> LiteralString:
    """Marks a runtime `str` as trusted SQL for type checkers -- the explicit,
    greppable escape hatch for SQL that is genuinely not a literal (a CLI
    argument, a `.sql` file read from disk). Does nothing at runtime. Never use
    it on text that contains request data; bind values with a t-string."""
    return cast("LiteralString", sql)


def prepare_sql(sql: Any, parameters: list[dict[str, Any]] | None = None) -> tuple[str, list[dict[str, Any]] | None]:
    """Returns the SQL text and the named-parameter list to submit. `str` is
    returned as-is with `parameters`; a t-string is rendered with one
    `:ab_p<n>` marker per interpolation (and may not be combined with
    `parameters`); a sqlglot expression is rendered in the Databricks dialect."""
    if isinstance(sql, str):
        return sql, parameters
    if sys.version_info >= (3, 14) and isinstance(sql, Template):
        if parameters:
            raise TypeError("a t-string carries its own parameters; don't also pass `parameters`")
        parts: list[str] = []
        bound: list[dict[str, Any]] = []
        _render(sql, parts, bound)
        return "".join(parts), bound or parameters
    if _is_sqlglot_expression(sql):
        return sql.sql(dialect="databricks"), parameters
    raise TypeError(
        f"sql must be a str, a t-string or a sqlglot expression, not {type(sql).__name__}"
        + (" (t-strings need Python 3.14+)" if sys.version_info < (3, 14) else "")
    )


def _is_sqlglot_expression(value: object) -> bool:
    # Duck-typed so sqlglot stays an optional dependency, never imported here.
    return type(value).__module__.partition(".")[0] == "sqlglot" and callable(getattr(value, "sql", None))


def _render(template: Any, parts: list[str], bound: list[dict[str, Any]]) -> None:
    interpolations = template.interpolations
    for index, literal in enumerate(template.strings):
        parts.append(literal)
        if index == len(interpolations):
            break
        _render_interpolation(interpolations[index], template.strings[index + 1], parts, bound)


def _render_interpolation(interpolation: Any, following: str, parts: list[str], bound: list[dict[str, Any]]) -> None:
    spec = interpolation.format_spec
    conversion = f"!{interpolation.conversion}" if interpolation.conversion else ""
    shown = f"{{{interpolation.expression or '<value>'}{conversion}{':' + spec if spec else ''}}}"
    if interpolation.conversion is not None:
        raise TypeError(f"conversions not supported in SQL t-string; got '{shown}'")
    if spec not in ("", "s", "i", "q"):
        raise ValueError(f"format spec not supported in SQL t-string; got '{shown}' (use none, 's', 'i' or 'q')")
    text = "".join(parts)
    if _in_quoted_context(text):
        raise ValueError(
            f"{shown} sits inside a quoted string, identifier or comment, where it would not be bound; "
            "build the value instead, e.g. {prefix + '%'} outside the quotes"
        )
    value = interpolation.value
    if spec == "q" or isinstance(value, Template):
        if not (spec == "q" and isinstance(value, Template)):
            raise ValueError(f"{shown}: a nested t-string needs the ':q' spec, and ':q' accepts only a t-string")
        _render(value, parts, bound)
        return
    if spec == "i":
        parts.append(_quote_identifier(shown, value))
        return
    if text.endswith(":") or _ENDS_WITH_MARKER.search(text) or _MARKER_CONTINUES.match(following):
        raise ValueError(f"{shown} must be separated from adjacent text by a space or punctuation (parameter marker)")
    name = f"{_PARAM_PREFIX}{len(bound)}"
    bound.append({"name": name, **_to_parameter(shown, value)})
    parts.append(f":{name}")


def _in_quoted_context(text: str) -> bool:
    """True if the end of `text` is inside a '...'/"..."/`...` literal, a
    `-- ...` line comment or a `/* ... */` block comment. Backslash escapes the
    next character inside quotes, as in Databricks' default string parsing."""
    quote: str | None = None
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if quote == "--":
            if c == "\n":
                quote = None
        elif quote == "/*":
            if text.startswith("*/", i):
                quote = None
                i += 1
        elif quote is not None:
            if c == "\\" and quote != "`":
                i += 1
            elif c == quote:
                quote = None
        elif c in "'\"`":
            quote = c
        elif text.startswith("--", i):
            quote = "--"
            i += 1
        elif text.startswith("/*", i):
            quote = "/*"
            i += 1
        i += 1
    return quote is not None


def _quote_identifier(shown: str, value: object) -> str:
    # A tuple/list of parts renders `a`.`b`.`c`; a lone str is ONE identifier
    # (a dotted string is not split), as with psycopg.
    names = value if isinstance(value, (tuple, list)) else [value]
    if not names or not all(isinstance(n, str) and n for n in names):
        raise TypeError(f"identifier {shown} must be a non-empty str or a tuple of them, not {type(value).__name__}")
    return ".".join("`" + str.__str__(n).replace("`", "``") + "`" for n in cast("list[str]", names))


def _to_parameter(shown: str, value: object) -> dict[str, Any]:
    """Databricks named-parameter `value`/`type` for a Python value -- the same
    scalar types psycopg adapts, minus the ones Databricks can't bind (BINARY is
    not a parameter type there). Text is taken through the base type's own
    methods so a subclass (numpy.float64, a `str` Enum) binds its value, not its
    repr or member name."""
    if value is None:
        return {"value": None, "type": "STRING"}
    if isinstance(value, bool):
        return {"value": "true" if value else "false", "type": "BOOLEAN"}
    if isinstance(value, int):
        if _INT64_MIN <= value <= _INT64_MAX:
            return {"value": int.__repr__(value), "type": "BIGINT"}
        if abs(value) >= _DECIMAL_MAX:
            raise OverflowError(f"integer {shown} does not fit Databricks' DECIMAL(38,0)")
        return {"value": int.__repr__(value), "type": "DECIMAL(38,0)"}
    if isinstance(value, float):
        return {"value": _float_text(value), "type": "DOUBLE"}
    if isinstance(value, Decimal):
        return _decimal_parameter(shown, value)
    if isinstance(value, str):
        return {"value": str.__str__(value), "type": "STRING"}
    if isinstance(value, datetime.datetime):  # before date: datetime is a date subclass
        kind = "TIMESTAMP_NTZ" if value.utcoffset() is None else "TIMESTAMP"
        return {"value": value.isoformat(sep=" "), "type": kind}
    if isinstance(value, datetime.date):
        return {"value": value.isoformat(), "type": "DATE"}
    if isinstance(value, uuid.UUID):
        return {"value": str(value), "type": "STRING"}
    raise TypeError(
        f"can't bind {shown} of type {type(value).__name__} as a SQL parameter; "
        "supported: None, bool, int, float, Decimal, str, date, datetime, UUID "
        "(convert other values explicitly)"
    )


def _float_text(value: float) -> str:
    if value != value:  # NaN
        return "NaN"
    if value in (float("inf"), float("-inf")):
        return "Infinity" if value > 0 else "-Infinity"
    return float.__repr__(value)


def _decimal_parameter(shown: str, value: Decimal) -> dict[str, Any]:
    if not value.is_finite():
        raise ValueError(f"can't bind non-finite Decimal {shown}; Databricks DECIMAL has no NaN/Infinity")
    _, digits, exponent = value.as_tuple()
    exponent = cast("int", exponent)  # an int, now that NaN/Infinity are ruled out
    if exponent >= 0:
        precision, scale = len(digits) + exponent, 0
    else:
        precision, scale = max(len(digits), -exponent), -exponent
    precision = max(precision, 1)
    if precision > 38:
        raise OverflowError(f"Decimal {shown} needs precision {precision}, Databricks allows at most 38")
    return {"value": Decimal.__format__(value, "f"), "type": f"DECIMAL({precision},{scale})"}
