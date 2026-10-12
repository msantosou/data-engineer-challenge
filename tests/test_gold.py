"""Tests against a small DuckDB fixture; no original challenge datasets required."""
from pathlib import Path
import importlib.util

import duckdb
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gold.py"
spec = importlib.util.spec_from_file_location("marvin_gold", SCRIPT)
gold = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold)


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA bronze; CREATE SCHEMA silver;")
    con.execute("""CREATE TABLE silver.transactions_current (
      transaction_id VARCHAR, created_at TIMESTAMPTZ, currency VARCHAR,
      status VARCHAR, amount DECIMAL(20,2))""")
    con.execute("""INSERT INTO silver.transactions_current VALUES
      ('A', '2025-03-04 10:00:00+00', 'BRL', 'COMPLETED', 100),
      ('B', '2025-03-04 11:00:00+00', 'BRL', 'COMPLETED', 200),
      ('C', '2025-03-04 12:00:00+00', 'USD', 'COMPLETED', 20),
      ('D', '2025-03-04 13:00:00+00', 'BRL', 'FAILED', 300)""")
    con.execute("""CREATE TABLE silver.reconciliation_runs_current (
      id BIGINT, reference_date DATE, status VARCHAR, completed_at TIMESTAMPTZ,
      started_at TIMESTAMPTZ, file_name VARCHAR)""")
    con.execute("""INSERT INTO silver.reconciliation_runs_current VALUES
      (1, '2025-03-04','COMPLETED','2025-03-04 12:00:00+00','2025-03-04 10:00:00+00','first.csv'),
      (2, '2025-03-04','COMPLETED','2025-03-04 13:00:00+00','2025-03-04 11:00:00+00','second.csv')""")
    con.execute("""CREATE TABLE silver.reconciliation_results_current (
      id BIGINT, run_id BIGINT, category VARCHAR, difference DECIMAL(20,2))""")
    con.execute("""INSERT INTO silver.reconciliation_results_current VALUES
      (1,1,'MISMATCHED',10), (2,2,'MATCHED',0), (3,2,'MISMATCHED',2)""")
    con.execute("""CREATE TABLE bronze.reconciliation_results (
      id BIGINT, run_id BIGINT, transaction_id VARCHAR, merchant_id VARCHAR,
      category VARCHAR, internal_amount DECIMAL(20,2), processor_amount DECIMAL(20,2),
      difference DECIMAL(20,2), created_at TIMESTAMPTZ,
      Op VARCHAR, _timestamp TIMESTAMPTZ, _source_file VARCHAR,
      _ingestion_row_number BIGINT)""")
    con.execute("""INSERT INTO bronze.reconciliation_results VALUES
       (2,2,'A','MERCH_001','MATCHED',100,100,0,'2025-03-04 11:00:00+00',
       'I','2025-03-04 11:00:00+00','sample.parquet',1),
       (2,2,'A','MERCH_001','MATCHED',100,100,0,'2025-03-04 11:00:00+00',
       'U','2025-03-04 12:00:00+00','sample.parquet',2)""")
    con.close()
    return path


def test_grains_and_metrics(db):
    counts = gold.materialize(db)
    assert counts == {"daily_financial_volume": 2, "daily_reconciliation_metrics": 1, "reconciliation_audit_trail": 2}
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT transaction_count, gross_amount FROM gold.daily_financial_volume WHERE currency='BRL'").fetchone() == (2, 300)
    assert con.execute("SELECT selected_run_id, total_results, mismatch_rate_pct FROM gold.daily_reconciliation_metrics").fetchone() == (2, 2, 50)
    assert con.execute("SELECT COUNT(*) FROM gold.reconciliation_audit_trail WHERE reconciliation_result_id=2").fetchone()[0] == 2
    con.close()


def test_idempotent_and_upstream_unchanged(db):
    con = duckdb.connect(str(db), read_only=True)
    initial = con.execute("SELECT COUNT(*) FROM bronze.reconciliation_results").fetchone()[0]
    con.close()
    assert gold.materialize(db) == gold.materialize(db)
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT COUNT(*) FROM bronze.reconciliation_results").fetchone()[0] == initial
    assert con.execute("SELECT COUNT(*) FROM gold.reconciliation_audit_trail").fetchone()[0] == 2
    con.close()
