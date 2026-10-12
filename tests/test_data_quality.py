"""Tests executed with: pytest tests/test_data_quality.py -v"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

MODULE = Path(__file__).resolve().parents[1] / 'scripts' / 'data_quality.py'
spec = importlib.util.spec_from_file_location('data_quality', MODULE)
dq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dq)


@pytest.fixture()
def database(tmp_path):
    path = tmp_path / 'fixture.duckdb'
    con = duckdb.connect(str(path))
    con.execute('CREATE SCHEMA bronze')
    con.execute('CREATE SCHEMA silver')
    con.execute('''CREATE TABLE silver.transactions_current (
        transaction_id VARCHAR, merchant_id VARCHAR, amount DECIMAL(20,2),
        currency VARCHAR, status VARCHAR, created_at TIMESTAMPTZ)''')
    con.execute('''CREATE TABLE silver.reconciliation_runs_current (
        id BIGINT, reference_date DATE)''')
    con.execute('''CREATE TABLE silver.reconciliation_results_current (
        id BIGINT, run_id BIGINT, transaction_id VARCHAR, merchant_id VARCHAR,
        category VARCHAR, internal_amount DECIMAL(20,2),
        processor_amount DECIMAL(20,2), difference DECIMAL(20,2))''')
    con.execute('''CREATE TABLE silver.enterprise_company_current (
        id BIGINT, merchant_id VARCHAR)''')
    con.execute('''CREATE TABLE silver.cdc_conflicts (
        source_table VARCHAR, business_key VARCHAR, event_timestamp TIMESTAMPTZ)''')
    con.execute("INSERT INTO silver.enterprise_company_current VALUES (1,'M1'),(2,'M2')")
    con.execute("""INSERT INTO silver.transactions_current VALUES
        ('T1','M1',100.00,'BRL','COMPLETED','2025-03-10 10:00:00+00'),
        ('T2','M1',50.00,'BRL','COMPLETED','2025-03-10 10:00:00+00')""")
    con.execute("INSERT INTO silver.reconciliation_runs_current VALUES (1, DATE '2025-03-10')")
    con.execute("""INSERT INTO silver.reconciliation_results_current VALUES
        (1,1,'T1','M2','MATCHED',120.00,120.00,0),
        (2,1,'T2','M1','MATCHED',50.00,50.00,0),
        (3,1,'MISSING','M1','UNRECONCILED_PROCESSOR',NULL,20.00,NULL),
        (4,1,'T1','M1','MISMATCHED',100.00,90.00,10.00)""")
    con.execute("INSERT INTO silver.cdc_conflicts VALUES ('transactions','T3','2025-03-10 00:00:00+00')")
    con.close()
    return path


def get_counts(path):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return dict(con.execute('SELECT rule_id, COUNT(*) FROM dq.issues GROUP BY rule_id').fetchall())
    finally:
        con.close()


def test_detects_real_issues_and_no_false_processor_missing(database):
    counts = dq.run_quality(database)
    assert counts['MERCHANT_MISMATCH'] == 1
    assert counts['INTERNAL_AMOUNT_MISMATCH'] == 1
    assert counts['CDC_AMBIGUOUS_LATEST'] == 1
    assert counts.get('TRANSACTION_MISSING', 0) == 0
    assert counts.get('TRANSACTION_OUTSIDE_WINDOW', 0) == 0


def test_idempotent_and_silver_is_not_modified(database):
    con = duckdb.connect(str(database), read_only=True)
    before = con.execute('SELECT COUNT(*), SUM(amount) FROM silver.transactions_current').fetchone()
    con.close()
    dq.run_quality(database)
    first = get_counts(database)
    dq.run_quality(database)
    assert get_counts(database) == first
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute('SELECT COUNT(*), SUM(amount) FROM silver.transactions_current').fetchone() == before
    assert con.execute('SELECT COUNT(*) = COUNT(DISTINCT issue_id) FROM dq.issues').fetchone()[0]
    con.close()


def test_missing_run(database):
    con = duckdb.connect(str(database))
    con.execute("INSERT INTO silver.reconciliation_results_current VALUES (5,999,'T2','M1','MATCHED',50,50,0)")
    con.close()
    counts = dq.run_quality(database)
    assert counts['RECON_RUN_MISSING'] == 1
