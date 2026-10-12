"""Etapa 4: Gold analytics for Operations, CFO and Compliance.

Run: python scripts/gold.py --db /app/data/marvin.duckdb
Or: python scripts/run_pipeline.py --with-gold --gold-script scripts/gold.py ...
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import duckdb

LOG = logging.getLogger("gold")

REQUIRED = (
    ("silver", "transactions_current"),
    ("silver", "reconciliation_runs_current"),
    ("silver", "reconciliation_results_current"),
    ("bronze", "reconciliation_results"),
)


def materialize(db_path: Path) -> dict[str, int]:
    """Replace three Gold tables transactionally, without changing upstream data."""
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Database not found: {path}")

    con = duckdb.connect(str(path))
    try:
        absent = [(schema, name) for schema, name in REQUIRED if not con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            [schema, name],
        ).fetchone()]
        if absent:
            raise ValueError(f"Missing tables; run Bronze/Silver first: {absent}")

        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")

        # CFO: one row per UTC creation date + currency; never mix currencies.
        # Snapshot of current, active, completed transactions. Not historical as-of accounting.
        con.execute("""
            CREATE OR REPLACE TABLE gold.daily_financial_volume AS
            SELECT
                CAST(created_at AT TIME ZONE 'UTC' AS DATE) AS transaction_date,
                currency,
                COUNT(*)::BIGINT AS transaction_count,
                CAST(SUM(amount) AS DECIMAL(24,2)) AS gross_amount,
                CAST(AVG(amount) AS DECIMAL(24,2)) AS avg_ticket
            FROM silver.transactions_current
            WHERE status = 'COMPLETED'
              AND created_at IS NOT NULL
              AND currency IS NOT NULL
              AND amount IS NOT NULL
            GROUP BY 1, 2
        """)

        # Operations: results from latest COMPLETED run per reference_date.
        # Run id tie-break provides deterministic choice, not a business sequencing guarantee.
        con.execute("""
            CREATE OR REPLACE TABLE gold.daily_reconciliation_metrics AS
            WITH selected_runs AS (
                SELECT id, reference_date, completed_at, started_at,
                       ROW_NUMBER() OVER (
                         PARTITION BY reference_date
                         ORDER BY completed_at DESC NULLS LAST,
                                  started_at DESC NULLS LAST, id DESC
                       ) AS rn
                FROM silver.reconciliation_runs_current
                WHERE UPPER(status) = 'COMPLETED'
                  AND reference_date IS NOT NULL
            ), counts AS (
                SELECT
                    r.reference_date,
                    r.id AS selected_run_id,
                    COUNT(rr.id)::BIGINT AS total_results,
                    COUNT(rr.id) FILTER (WHERE rr.category='MATCHED')::BIGINT AS matched_count,
                    COUNT(rr.id) FILTER (WHERE rr.category='MISMATCHED')::BIGINT AS mismatched_count,
                    COUNT(rr.id) FILTER (WHERE rr.category='UNRECONCILED_INTERNAL')::BIGINT AS unreconciled_internal_count,
                    COUNT(rr.id) FILTER (WHERE rr.category='UNRECONCILED_PROCESSOR')::BIGINT AS unreconciled_processor_count,
                    COUNT(rr.id) FILTER (WHERE rr.category IS NULL OR rr.category NOT IN (
                       'MATCHED','MISMATCHED','UNRECONCILED_INTERNAL','UNRECONCILED_PROCESSOR'
                    ))::BIGINT AS unknown_category_count,
                    CAST(COALESCE(SUM(ABS(rr.difference)) FILTER (
                      WHERE rr.category='MISMATCHED'), 0) AS DECIMAL(24,2)) AS mismatched_difference_amount
                FROM selected_runs r
                LEFT JOIN silver.reconciliation_results_current rr ON rr.run_id = r.id
                WHERE r.rn=1
                GROUP BY r.reference_date, r.id
            )
            SELECT *,
                ROUND(100.0 * mismatched_count / NULLIF(total_results,0), 2) AS mismatch_rate_pct,
                ROUND(100.0 * (unreconciled_internal_count+unreconciled_processor_count)
                   / NULLIF(total_results,0), 2) AS unreconciled_rate_pct,
                ROUND(100.0 * matched_count / NULLIF(total_results,0), 2) AS matched_rate_pct
            FROM counts
        """)

        # Compliance: preserve every Bronze result CDC event, rather than only current.
        # Joining to latest run metadata is context, not an as-of snapshot of the run.
        con.execute("""
            CREATE OR REPLACE TABLE gold.reconciliation_audit_trail AS
            SELECT
                rr.id AS reconciliation_result_id,
                rr.run_id,
                rr.transaction_id,
                rr.merchant_id,
                rr.category,
                rr.internal_amount,
                rr.processor_amount,
                rr.difference,
                rr.created_at AS reconciliation_created_at,
                rr.Op AS result_cdc_op,
                rr._timestamp AS result_event_timestamp,
                rr._source_file AS result_source_file,
                rr._ingestion_row_number AS result_ingestion_row_number,
                r.reference_date,
                r.file_name AS run_file_name,
                r.status AS run_current_status,
                r.started_at AS run_started_at,
                r.completed_at AS run_completed_at,
                (r.id IS NULL) AS missing_run_reference
            FROM bronze.reconciliation_results rr
            LEFT JOIN silver.reconciliation_runs_current r
              ON rr.run_id = r.id
        """)

        counts = {
            name: con.execute(f"SELECT COUNT(*) FROM gold.{name}").fetchone()[0]
            for name in ("daily_financial_volume", "daily_reconciliation_metrics", "reconciliation_audit_trail")
        }
        # Assert declared grains before publishing.
        checks = (
            ("daily_financial_volume", "transaction_date, currency"),
            ("daily_reconciliation_metrics", "reference_date"),
        )
        for table, keys in checks:
            duplicates = con.execute(f"""
                SELECT COUNT(*) FROM (
                   SELECT {keys} FROM gold.{table}
                   GROUP BY {keys} HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            if duplicates:
                raise AssertionError(f"gold.{table}: {duplicates} duplicate grains")
        con.execute("COMMIT")
        for table, rows in counts.items():
            LOG.info("gold.%s: %s rows", table, rows)
        return counts
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize Marvin Gold data products")
    parser.add_argument("--db", type=Path, default=Path("data/marvin.duckdb"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(materialize(args.db), indent=2))


if __name__ == "__main__":
    main()
