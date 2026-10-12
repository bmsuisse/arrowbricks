"""SQL input handling: `LiteralString`, PEP 750 t-strings and sqlglot expressions.

Every public entry point that takes SQL accepts the `Sql` type below. A plain
`str` still works at runtime (it is sent unchanged), but is typed as
`LiteralString` so a type checker flags SQL built from runtime values -- the
injection bug class. Dynamic *values* go in a t-string (Python 3.14+), whose
interpolations are bound as Databricks named parameters, the same way
psycopg's t-string support works, never spliced into the SQL text:

    await cur.execute(t"SELECT * FROM {table:i} WHERE id = {user_id}")

`prepare_sql` turns any accepted input into the `(sql, parameters)` pair the
Rust core takes -- `parameters` being Databricks' own
`[{"name": ..., "value": ..., "type": ...}]` format. Nothing below the Python
facade knows about t-strings.
"""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any, LiteralString, cast

if TYPE_CHECKING:
    from string.templatelib import Template

    # Optional (`pip install arrowbricks[sqlglot]`); only used as a type.
    from sqlglot import exp

    Sql = LiteralString | Template | exp.Expression
else:
    Sql = Any

try:
    from string.templatelib import Template as _Template  # Python 3.14+
except ImportError:  # pragma: no cover -- exercised on Python < 3.14
    _Template = None

__all__ = ["Sql", "prepare_sql", "unsafe_sql"]

# Named-parameter prefix for t-string interpolations. Explicit `parameters`
# may not use it, so the two sources can never collide.
_PARAM_PREFIX = "ab_p"

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_DECIMAL_MAX_PRECISION = 38


def unsafe_sql(sql: str) -> LiteralString:
    """Marks a runtime `str` as trusted SQL for type checkers -- the explicit,
    greppable escape hatch for SQL that is genuinely not a literal (a CLI
    argument, a `.sql` file read from disk). Does nothing at runtime. Never use
    it on text that contains request data; bind values with a t-string."""
    return cast("LiteralString", sql)


def prepare_sql(sql: Any, parameters: list[dict[str, Any]] | None = None) -> tuple[str, list[dict[str, Any]] | None]:
    """Returns the SQL text and the named-parameter list to submit. `str` is
    returned as-is, a t-string is rendered with one `:ab_p<n>` marker per
    interpolation, a sqlglot expression is rendered in the Databricks dialect.
    Explicit `parameters` are kept and extended by the t-string's own."""
    if isinstance(sql, str):
        return sql, parameters
    if _Template is not None and isinstance(sql, _Template):
        explicit = parameters or []
        taken = {p.get("name") for p in explicit if isinstance(p, dict)}
        if any(isinstance(name, str) and name.startswith(_PARAM_PREFIX) for name in taken):
            raise ValueError(f"parameter names starting with {_PARAM_PREFIX!r} are reserved for t-string values")
        bound: list[dict[str, Any]] = []
        text = _render(sql, bound)
        if not bound:
            return text, parameters
        return text, [*explicit, *bound]
    if _is_sqlglot_expression(sql):
        return sql.sql(dialect="databricks"), parameters
    raise TypeError(
        f"sql must be a str, a t-string or a sqlglot expression, not {type(sql).__name__}"
        + (" (t-strings need Python 3.14+)" if _Template is None else "")
    )


def _is_sqlglot_expression(value: object) -> bool:
    # Duck-typed so sqlglot stays an optional dependency, never imported here.
    return type(value).__module__.partition(".")[0] == "sqlglot" and callable(getattr(value, "sql", None))


def _render(template: Any, bound: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    interpolations = template.interpolations
    for index, literal in enumerate(template.strings):
        parts.append(literal)
        if index == len(interpolations):
            break
        parts.append(_render_interpolation(interpolations[index], bound))
    return "".join(parts)


def _render_interpolation(interpolation: Any, bound: list[dict[str, Any]]) -> str:
    expression = interpolation.expression or "<value>"
    if interpolation.conversion is not None:
        raise ValueError(f"conversion !{interpolation.conversion} is not supported in SQL t-string ({{{expression}}})")
    value = interpolation.value
    spec = interpolation.format_spec
    if spec == "i":
        return _quote_identifier(expression, value)
    if spec not in ("", "s"):
        raise ValueError(f"unsupported format spec {spec!r} in SQL t-string ({{{expression}:{spec}}}); use 'i' or none")
    if _Template is not None and isinstance(value, _Template):
        return _render(value, bound)
    name = f"{_PARAM_PREFIX}{len(bound)}"
    bound.append({"name": name, **_to_parameter(expression, value)})
    return f":{name}"


def _quote_identifier(expression: str, value: object) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"identifier {{{expression}:i}} must be a non-empty str, not {type(value).__name__}")
    return "`" + value.replace("`", "``") + "`"


def _to_parameter(expression: str, value: object) -> dict[str, Any]:
    """Databricks named-parameter `value`/`type` for a Python value -- the same
    scalar types psycopg adapts, minus the ones Databricks can't bind."""
    if value is None:
        return {"value": None, "type": "STRING"}
    if isinstance(value, bool):
        return {"value": "true" if value else "false", "type": "BOOLEAN"}
    if isinstance(value, int):
        if _INT64_MIN <= value <= _INT64_MAX:
            return {"value": str(value), "type": "BIGINT"}
        if len(str(abs(value))) > _DECIMAL_MAX_PRECISION:
            raise OverflowError(f"integer {{{expression}}} does not fit Databricks' DECIMAL(38,0)")
        return {"value": str(value), "type": f"DECIMAL({_DECIMAL_MAX_PRECISION},0)"}
    if isinstance(value, float):
        return {"value": _float_text(value), "type": "DOUBLE"}
    if isinstance(value, Decimal):
        return _decimal_parameter(expression, value)
    if isinstance(value, str):
        return {"value": str(value), "type": "STRING"}
    if isinstance(value, datetime.datetime):  # before date: datetime is a date subclass
        if value.tzinfo is None:
            return {"value": value.isoformat(sep=" "), "type": "TIMESTAMP_NTZ"}
        return {"value": value.isoformat(sep=" "), "type": "TIMESTAMP"}
    if isinstance(value, datetime.date):
        return {"value": value.isoformat(), "type": "DATE"}
    if isinstance(value, uuid.UUID):
        return {"value": str(value), "type": "STRING"}
    raise TypeError(
        f"can't bind {{{expression}}} of type {type(value).__name__} as a SQL parameter; "
        "supported: None, bool, int, float, Decimal, str, date, datetime, UUID "
        "(convert other values explicitly, or splice SQL structure with a nested t-string)"
    )


def _float_text(value: float) -> str:
    if value != value:  # NaN
        return "NaN"
    if value in (float("inf"), float("-inf")):
        return "Infinity" if value > 0 else "-Infinity"
    return repr(value)


def _decimal_parameter(expression: str, value: Decimal) -> dict[str, Any]:
    if not value.is_finite():
        raise ValueError(f"can't bind non-finite Decimal {{{expression}}}; Databricks DECIMAL has no NaN/Infinity")
    _, digits, exponent = value.as_tuple()
    exponent = cast("int", exponent)  # an int, now that NaN/Infinity are ruled out
    if exponent >= 0:
        precision, scale = len(digits) + exponent, 0
    else:
        precision, scale = max(len(digits), -exponent), -exponent
    precision = max(precision, 1)
    if precision > _DECIMAL_MAX_PRECISION:
        raise OverflowError(f"Decimal {{{expression}}} needs precision {precision}, Databricks allows at most 38")
    return {"value": format(value, "f"), "type": f"DECIMAL({precision},{scale})"}
