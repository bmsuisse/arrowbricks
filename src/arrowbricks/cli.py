"""Command-line SQL previews rendered by the Rust agent pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import NoReturn


def _configuration_error(message: str) -> None:
    print(
        json.dumps(
            {
                "category": "configuration",
                "message": message,
                "next_step": "Check --help, credentials, SQL input and positive budgets (max_bytes >= 256).",
            }
        ),
        file=sys.stderr,
    )


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        _configuration_error(message)
        raise SystemExit(2)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(description="Databricks SQL to bounded typed Markdown; failures are JSON on stderr.")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("-c", "--sql", "--command", dest="sql", help="SQL statement, sent unchanged")
    source.add_argument("-f", "--file", help="UTF-8 SQL file, or - for stdin; defaults to piped stdin")
    parser.add_argument("--agent", "-agent", action="store_true", help="agent Markdown output (the default)")
    parser.add_argument("--host", default=os.environ.get("DATABRICKS_HOST"), help="workspace host (DATABRICKS_HOST)")
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse ID (DATABRICKS_WAREHOUSE_ID)",
    )
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN"), help="access token (DATABRICKS_TOKEN)")
    parser.add_argument("--protocol", choices=("sea", "thrift"), default="thrift")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--max-bytes", type=int, default=10000)
    parser.add_argument("--max-cell-chars", type=int, default=500)
    parser.add_argument("--total-timeout-s", "--timeout", dest="total_timeout_s", type=float, default=60.0)
    return parser


async def _run(args: argparse.Namespace, sql: str) -> int:
    from .client import DatabricksClient

    try:
        async with DatabricksClient(
            args.host,
            args.warehouse_id,
            token=args.token,
            protocol=args.protocol,
        ) as client:
            result = await client.query_agent(
                sql,
                max_rows=args.max_rows,
                max_bytes=args.max_bytes,
                max_cell_chars=args.max_cell_chars,
                total_timeout_s=args.total_timeout_s,
                catalog=args.catalog,
                schema=args.schema,
            )
    except (ValueError, OverflowError, TypeError) as exc:
        _configuration_error(str(exc))
        return 2
    # Locale encoding and newline translation must not change the byte budget.
    if result.lstrip().startswith("{"):
        sys.stderr.buffer.write(result.encode("utf-8"))
        return 1
    sys.stdout.buffer.write(result.encode("utf-8"))
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run one SQL statement; return 0 for Markdown, 1 for query errors, 2 for configuration."""
    parser = _parser()
    args = parser.parse_args(argv)
    if not all((args.host, args.warehouse_id, args.token)):
        parser.error("set DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID and DATABRICKS_TOKEN or supply their flags")
    try:
        if args.sql is not None:
            sql = args.sql
        elif args.file and args.file != "-":
            sql = Path(args.file).read_bytes().decode("utf-8")
        else:
            if sys.stdin.isatty():
                parser.error("supply --sql, --file or pipe a SQL statement on stdin")
            sql = sys.stdin.buffer.read().decode("utf-8")
    except (OSError, UnicodeError) as exc:
        parser.error(f"cannot read SQL input: {exc}")
    if not sql.strip():
        parser.error("SQL input is empty; supply --sql, --file or pipe SQL on stdin")
    import asyncio

    try:
        return asyncio.run(_run(args, sql))
    except KeyboardInterrupt:
        print(
            json.dumps(
                {
                    "category": "interrupted",
                    "message": "Query interrupted.",
                    "next_step": "Retry only if you still need the result.",
                }
            ),
            file=sys.stderr,
        )
        return 130
