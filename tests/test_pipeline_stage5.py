"""Checks do contrato de entrega. Use: pytest tests/ -v"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "scripts" / "run_pipeline.py"
spec = importlib.util.spec_from_file_location("run_pipeline_stage5", MODULE)
assert spec and spec.loader
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


def test_expected_pipeline_scripts_exist():
    for name in ("bronze_ingest.py", "silver_cdc.py", "data_quality.py"):
        assert (ROOT / "scripts" / name).is_file()


def test_snapshot_excludes_volatile_metadata_and_detects_business_change(tmp_path):
    db = tmp_path / "sample.duckdb"
    con = duckdb.connect(str(db))
    for schema in ("bronze", "silver", "dq"):
        con.execute(f"CREATE SCHEMA {schema}")
    for table in pipeline.BRONZE_TABLES:
        con.execute(f"CREATE TABLE bronze.{table} (id INTEGER, _ingested_at TIMESTAMP, _ingestion_row_number INTEGER)")
        con.execute(f"INSERT INTO bronze.{table} VALUES (1, NOW(), 1)")
    for table in pipeline.SILVER_TABLES:
        con.execute(f"CREATE TABLE silver.{table} (id INTEGER)")
        con.execute(f"INSERT INTO silver.{table} VALUES (1)")
    con.execute("CREATE TABLE dq.issues (id INTEGER, evaluated_at TIMESTAMP)")
    con.execute("INSERT INTO dq.issues VALUES (1, NOW())")
    con.execute("CREATE VIEW silver.paysettler_events AS SELECT * FROM bronze.paysettler")
    con.close()
    before = pipeline.snapshot(db)
    con = duckdb.connect(str(db))
    con.execute("UPDATE bronze.transactions SET _ingested_at = NOW() + INTERVAL '1 hour', _ingestion_row_number = 99")
    con.execute("UPDATE dq.issues SET evaluated_at = NOW() + INTERVAL '1 hour'")
    con.close()
    assert pipeline.snapshot(db) == before
    con = duckdb.connect(str(db))
    con.execute("UPDATE silver.transactions_current SET id = 2")
    con.close()
    assert pipeline.snapshot(db) != before


def test_gold_is_explicitly_optional(tmp_path):
    with pytest.raises(FileNotFoundError, match="Script ausente"):
        pipeline.load_module("missing_gold", tmp_path / "no_gold.py")
