"""Smoke tests for Bronze, using isolated synthetic source files."""
from pathlib import Path
import importlib.util

import duckdb

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "bronze_ingest.py"
spec = importlib.util.spec_from_file_location("bronze_ingest", MODULE)
bronze = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(bronze)


def make_sources(folder: Path):
    con = duckdb.connect()
    parquet_tables = {
        "transactions_batch_1": "SELECT 1 AS id, 'TX1' AS transaction_id, 'M1' AS merchant_id, 12.30 AS amount, 'I' AS Op, '2025-03-01T10:00:00+00:00' AS _timestamp",
        "transactions_batch_2": "SELECT 1 AS id, 'TX1' AS transaction_id, 'M1' AS merchant_id, 14.30 AS amount, 'U' AS Op, '2025-03-02T10:00:00+00:00' AS _timestamp, 'pix' AS payment_method",
        "reconciliation_runs": "SELECT 1 AS id, '2025-03-02' AS reference_date, 'I' AS Op, '2025-03-02T10:00:00+00:00' AS _timestamp",
        "reconciliation_results": "SELECT 1 AS id, 1 AS run_id, 'TX1' AS transaction_id, 'M1' AS merchant_id, 'MATCHED' AS category, 14.30 AS internal_amount, 14.30 AS processor_amount, 0.0 AS difference, 'I' AS Op, '2025-03-02T10:00:00+00:00' AS _timestamp",
        "enterprise_company": "SELECT 1 AS id, 'M1' AS merchant_id, 'Merchant' AS legal_name, 'I' AS Op, '2025-03-02T10:00:00+00:00' AS _timestamp",
    }
    for name, sql in parquet_tables.items():
        destination = str(folder / f"{name}.parquet").replace("'", "''")
        con.execute(f"COPY ({sql}) TO '{destination}' (FORMAT PARQUET)")
    (folder / "settlement_paysettler.csv").write_text(
        "transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status\n"
        "TX1,M1,14.30,BRL,2025-03-02T10:00:00Z,REF1,SETTLED\n", encoding="utf-8"
    )
    con.close()


def test_bronze_ingests_all_sources_and_is_repeatable(tmp_path):
    inputs = tmp_path / "source"
    inputs.mkdir()
    make_sources(inputs)
    database = tmp_path / "out" / "marvin.duckdb"
    first = bronze.ingest(inputs, database)
    second = bronze.ingest(inputs, database)
    assert first == second == {
        "transactions": 2, "reconciliation_runs": 1,
        "reconciliation_results": 1, "enterprise_company": 1, "paysettler": 1,
    }
    con = duckdb.connect(str(database))
    assert con.execute("SELECT COUNT(*) FROM bronze.transactions").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM bronze.transactions WHERE \"Op\" = 'U'").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM bronze.transactions WHERE payment_method IS NULL").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM bronze.transactions WHERE _source_file IS NULL OR _raw_payload IS NULL OR _ingested_at IS NULL").fetchone()[0] == 0
    assert str(con.execute("SELECT amount FROM bronze.paysettler").fetchone()[0]) == "14.30"
    con.close()


def test_missing_source_fails_without_creating_database(tmp_path):
    try:
        bronze.ingest(tmp_path, tmp_path / "out" / "db.duckdb")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected missing source failure")
    assert not (tmp_path / "out" / "db.duckdb").exists()
