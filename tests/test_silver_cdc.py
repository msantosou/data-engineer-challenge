"""Testes isolados do CDC: updates, deletes, PK, idempotência e conflitos."""
import importlib.util
from pathlib import Path

import duckdb
import pytest

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "silver_cdc.py"
spec = importlib.util.spec_from_file_location("silver_cdc", MODULE)
silver = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(silver)


def sample_db(path):
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA bronze")
    for name in silver.CDC_KEYS:
        key = "transaction_id VARCHAR" if name == "transactions" else "id BIGINT"
        con.execute(f'''CREATE TABLE bronze.{name} (
          {key}, amount DECIMAL(20,2), Op VARCHAR, _timestamp TIMESTAMPTZ,
          _raw_payload VARCHAR, _source_file VARCHAR, _ingestion_row_number BIGINT
        )''')
    con.execute("CREATE TABLE bronze.paysettler (transaction_id VARCHAR, _source_file VARCHAR, _raw_payload VARCHAR)")
    for name, key in silver.CDC_KEYS.items():
        value = "'TX-A'" if key == "transaction_id" else "1"
        for index, (op, amount, timestamp) in enumerate([
            ("I", 10, "2025-03-01 00:00:00+00"),
            ("U", 12, "2025-03-02 00:00:00+00"),
        ], 1):
            con.execute(f"INSERT INTO bronze.{name} VALUES ({value}, ?, ?, ?::TIMESTAMPTZ, ?, 'x.parquet', ?)",
                        [amount, op, timestamp, f"{op}-{amount}", index])
        # Chave 2 inserida e depois excluída: não pode reaparecer no current.
        second = "'TX-B'" if key == "transaction_id" else "2"
        con.execute(f"INSERT INTO bronze.{name} VALUES ({second}, 99, 'I', '2025-03-01'::TIMESTAMPTZ, 'I-99', 'x.parquet', 3)")
        con.execute(f"INSERT INTO bronze.{name} VALUES ({second}, 99, 'D', '2025-03-03'::TIMESTAMPTZ, 'D-99', 'x.parquet', 4)")
    con.execute("INSERT INTO bronze.paysettler VALUES ('TX-A','x.csv','{}')")
    con.close()


def test_latest_update_and_delete_are_applied(tmp_path):
    db = tmp_path / 'test.duckdb'
    sample_db(db)
    a = silver.materialize(db)
    b = silver.materialize(db)
    assert a == b
    con = duckdb.connect(str(db))
    for name in silver.CDC_KEYS:
        rows = con.execute(f"SELECT amount, Op FROM silver.{name}_current").fetchall()
        assert rows == [(12, 'U')]
        assert a[name]["current"] == 1
        assert a[name]["deleted_keys"] == 1
    assert con.execute("SELECT COUNT(*) FROM silver.paysettler_events").fetchone()[0] == 1
    con.close()


def test_ambiguous_latest_timestamp_is_quarantined(tmp_path):
    db = tmp_path / 'test.duckdb'
    sample_db(db)
    con = duckdb.connect(str(db))
    con.execute("""INSERT INTO bronze.transactions VALUES
       ('TX-A', 33, 'U', '2025-03-02'::TIMESTAMPTZ, 'conflict', 'x.parquet', 5)""")
    con.close()
    report = silver.materialize(db)
    assert report['transactions']['ambiguous_keys'] == 1
    con = duckdb.connect(str(db))
    assert con.execute("SELECT COUNT(*) FROM silver.cdc_conflicts WHERE source_table = 'transactions'").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM silver.transactions_current WHERE transaction_id = 'TX-A'").fetchone()[0] == 0
    con.close()


def test_missing_key_rejected(tmp_path):
    db = tmp_path / 'test.duckdb'
    sample_db(db)
    con = duckdb.connect(str(db))
    con.execute("INSERT INTO bronze.transactions VALUES (NULL, 5, 'I', '2025-03-04'::TIMESTAMPTZ, '{}', 'x.parquet', 5)")
    con.close()
    with pytest.raises(ValueError, match='inválidos'):
        silver.materialize(db)
