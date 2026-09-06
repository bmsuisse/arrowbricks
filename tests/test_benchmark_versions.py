"""Synthetic coverage for benchmark integrity and private worker output."""

import importlib.util
import io
import json
import subprocess
from pathlib import Path

import arro3.core as core
import pytest


@pytest.fixture
def benchmark(monkeypatch):
    examples = Path(__file__).resolve().parents[1] / "examples"
    monkeypatch.syspath_prepend(str(examples))
    spec = importlib.util.spec_from_file_location("benchmark_versions_under_test", examples / "benchmark_versions.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "load_dotenv", lambda: None)
    return module


def test_ipc_checksum_distinguishes_schema_only_results(benchmark):
    def empty(name, dtype):
        return core.Table.from_batches([], schema=core.Schema([core.Field(name, dtype)]))

    original = benchmark.ipc_checksum(empty("id", core.DataType.int64()))
    assert original == benchmark.ipc_checksum(empty("id", core.DataType.int64()))
    assert original != benchmark.ipc_checksum(empty("other", core.DataType.int64()))
    assert original != benchmark.ipc_checksum(empty("id", core.DataType.string()))


def test_ipc_checksum_detects_values_nulls_and_order(benchmark):
    def checksum(values):
        return benchmark.ipc_checksum(core.Table.from_pydict({"id": core.Array(values, type=core.DataType.int64())}))

    original = checksum([1, None, 2])
    assert original == checksum([1, None, 2])
    assert original != checksum([1, None, 3])
    assert original != checksum([1, 0, 2])
    assert original != checksum([2, None, 1])


class FakeChild:
    def __init__(self, samples, stubborn=False):
        self.stdin = io.StringIO()
        self.stdout = io.StringIO("".join(json.dumps(sample) + "\n" for sample in samples))
        self.stubborn = stubborn
        self.actions = []

    def wait(self, timeout=None):
        self.actions.append("wait")
        if self.stubborn and "kill" not in self.actions:
            assert timeout is not None
            raise subprocess.TimeoutExpired("synthetic-worker", timeout)
        return 0

    def terminate(self):
        self.actions.append("terminate")

    def kill(self):
        self.actions.append("kill")


def sample(checksum="synthetic-private-digest"):
    return {"rows": 3, "total_s": 1.0, "peak_rss_mib": 2.0, "checksum": checksum}


def install_children(benchmark, monkeypatch, children):
    pending = iter(children)

    def popen(command, **kwargs):
        assert kwargs["stderr"] == subprocess.DEVNULL
        assert "--verify-ipc" in command
        return next(pending)

    monkeypatch.setattr(benchmark.subprocess, "Popen", popen)
    monkeypatch.setattr(
        benchmark.sys,
        "argv",
        ["benchmark", "--baseline", "old", "--candidate", "new", "--runs", "1", "--verify-ipc"],
    )


def test_equal_worker_checksums_publish_only_metrics(benchmark, monkeypatch, capsys):
    children = [FakeChild([sample(), sample()]), FakeChild([sample(), sample()])]
    install_children(benchmark, monkeypatch, children)
    benchmark.main()
    output = capsys.readouterr()
    records = [json.loads(line) for line in output.out.splitlines()]
    assert len(records) == 4
    assert sum("round" in record for record in records) == 2
    assert "checksum" not in output.out
    assert "synthetic-private-digest" not in output.out + output.err
    assert all(child.stdin.closed and child.actions == ["wait"] for child in children)


@pytest.mark.parametrize("failure", ["missing", "different", "exit"])
def test_failed_pair_emits_no_metrics_or_digest_and_reaps_children(benchmark, monkeypatch, capsys, failure):
    bad = sample("different-private-digest")
    if failure == "missing":
        bad.pop("checksum")
    children = [
        FakeChild([sample(), sample()], stubborn=True),
        FakeChild([sample()] if failure == "exit" else [sample(), bad]),
    ]
    install_children(benchmark, monkeypatch, children)
    with pytest.raises(RuntimeError, match="exited without a result|checksums differ"):
        benchmark.main()
    output = capsys.readouterr()
    assert output.out == output.err == ""
    assert all(child.stdin.closed and child.actions[-1] == "wait" for child in children)
    assert children[0].actions == ["wait", "terminate", "wait", "kill", "wait"]


def test_worker_exception_suppresses_sensitive_details(benchmark, monkeypatch, capsys):
    async def failing_worker(args):
        raise RuntimeError("synthetic-secret SELECT private_value https://example.invalid/?signature=private")

    monkeypatch.setattr(benchmark, "worker", failing_worker)
    monkeypatch.setattr(benchmark.sys, "argv", ["benchmark", "--worker", "synthetic"])
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "benchmark worker failed (RuntimeError)\n"
