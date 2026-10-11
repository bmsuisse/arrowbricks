from examples.benchmark_vs_connector import load_dotenv


def test_live_example_env_file_is_explicit_and_keeps_existing_values(tmp_path, monkeypatch):
    path = tmp_path / "warehouse.env"
    path.write_text('ARROWBRICKS_TEST_HOST="fake-host"\nARROWBRICKS_TEST_TOKEN=fake-token\n')
    monkeypatch.delenv("ARROWBRICKS_TEST_HOST", raising=False)
    monkeypatch.setenv("ARROWBRICKS_TEST_TOKEN", "existing-fake-token")
    load_dotenv(path)
    import os

    assert os.environ["ARROWBRICKS_TEST_HOST"] == "fake-host"
    assert os.environ["ARROWBRICKS_TEST_TOKEN"] == "existing-fake-token"  # noqa: S105 -- synthetic test value
