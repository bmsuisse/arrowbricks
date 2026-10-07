"""Importing the package must stay cheap: asyncio (~27 ms) loads only when first needed."""

import subprocess
import sys

import pytest


def _modules_after(statement: str) -> set[str]:
    code = f"import sys; {statement}; print(' '.join(sorted(sys.modules)))"
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True).stdout  # noqa: S603
    return set(out.split())


def test_import_does_not_load_asyncio():
    assert "asyncio" not in _modules_after("import arrowbricks")


@pytest.mark.skipif(sys.version_info < (3, 15), reason="__lazy_modules__ (PEP 810) needs Python 3.15")
def test_import_defers_pure_python_submodules_on_3_15():
    loaded = _modules_after("import arrowbricks")
    assert not {"arrowbricks.client", "arrowbricks.cursor", "arrowbricks._streaming"} & loaded


def test_lazily_exposed_names_still_resolve():
    code = "import arrowbricks as a; print(a.QueryTimeout.__name__, a.Cursor.__name__, a.HEARTBEAT is not None)"
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True).stdout  # noqa: S603
    assert out.split() == ["QueryTimeout", "Cursor", "True"]
