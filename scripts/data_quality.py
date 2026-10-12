"""Etapa 3: regras de qualidade sem mutação das camadas Bronze/Silver.

Uso: python scripts/data_quality.py --db /app/data/marvin.duckdb
As taxas são diagnósticas; não reclassificam resultados de reconciliação.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

LOG = logging.getLogger(__name__)

# issue_id estável por execução lógica; um evento gera uma ocorrência por regra.
# reference key = chave do resultado + run quando disponível.
RULES: list[tuple[str, str, str, str]] = [
    (
        "RECON_RUN_MISSING", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, CAST(r.run_id AS VARCHAR) AS related_key,
                 'run_id=' || CAST(r.run_id AS VARCHAR) AS details
           FROM silver.reconciliation_results_current r
           LEFT JOIN silver.reconciliation_runs_current run ON r.run_id = run.id
           WHERE run.id IS NULL""",
    ),
    (
        "TRANSACTION_MISSING", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'category=' || COALESCE(r.category, '<NULL>') AS details
           FROM silver.reconciliation_results_current r
           LEFT JOIN silver.transactions_current t ON r.transaction_id = t.transaction_id
           WHERE UPPER(COALESCE(r.category, '')) IN ('MATCHED', 'MISMATCHED', 'UNRECONCILED_INTERNAL')
             AND t.transaction_id IS NULL""",
    ),
    (
        "MERCHANT_MISMATCH", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'reconciliation=' || COALESCE(r.merchant_id, '<NULL>') ||
                 ';transaction=' || COALESCE(t.merchant_id, '<NULL>') AS details
           FROM silver.reconciliation_results_current r
           JOIN silver.transactions_current t ON r.transaction_id = t.transaction_id
           WHERE r.merchant_id IS DISTINCT FROM t.merchant_id""",
    ),
    (
        "INTERNAL_AMOUNT_MISMATCH", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'reconciliation=' || COALESCE(CAST(r.internal_amount AS VARCHAR), '<NULL>') ||
                 ';transaction=' || COALESCE(CAST(t.amount AS VARCHAR), '<NULL>') AS details
           FROM silver.reconciliation_results_current r
           JOIN silver.transactions_current t ON r.transaction_id = t.transaction_id
           WHERE r.internal_amount IS NOT NULL AND t.amount IS NOT NULL
             AND ABS(r.internal_amount - t.amount) > 0.01""",
    ),
    (
        "TRANSACTION_OUTSIDE_WINDOW", "MEDIUM", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'reference_date=' || CAST(run.reference_date AS VARCHAR) ||
                 ';transaction_created_at=' || CAST(t.created_at AS VARCHAR) AS details
           FROM silver.reconciliation_results_current r
           JOIN silver.reconciliation_runs_current run ON r.run_id = run.id
           JOIN silver.transactions_current t ON r.transaction_id = t.transaction_id
           WHERE t.created_at IS NOT NULL AND run.reference_date IS NOT NULL
             AND CAST(t.created_at AT TIME ZONE 'UTC' AS DATE)
                 NOT BETWEEN (run.reference_date - INTERVAL '7 days')::DATE AND run.reference_date
             AND UPPER(COALESCE(r.category, '')) IN ('MATCHED', 'MISMATCHED', 'UNRECONCILED_INTERNAL')""",
    ),
    (
        "RECON_DIFFERENCE_INCORRECT", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'stored=' || CAST(r.difference AS VARCHAR) ||
                 ';computed=' || CAST(ABS(r.internal_amount - r.processor_amount) AS VARCHAR) AS details
           FROM silver.reconciliation_results_current r
           WHERE r.internal_amount IS NOT NULL AND r.processor_amount IS NOT NULL
             AND r.difference IS NOT NULL
             AND ABS(r.difference - ABS(r.internal_amount - r.processor_amount)) > 0.01""",
    ),
    (
        "MATCHED_ABOVE_TOLERANCE", "HIGH", "reconciliation_results",
        """SELECT CAST(r.id AS VARCHAR) AS record_key, r.transaction_id AS related_key,
                 'difference=' || COALESCE(CAST(r.difference AS VARCHAR), '<NULL>') AS details
           FROM silver.reconciliation_results_current r
           WHERE UPPER(COALESCE(r.category, '')) = 'MATCHED'
             AND (r.difference > 0.01 OR r.difference < 0 OR r.difference IS NULL)""",
    ),
    (
        "MERCHANT_NOT_IN_COMPANY", "MEDIUM", "transactions",
        """SELECT t.transaction_id AS record_key, t.merchant_id AS related_key,
                 'merchant_id=' || COALESCE(t.merchant_id, '<NULL>') AS details
           FROM silver.transactions_current t
           LEFT JOIN silver.enterprise_company_current c ON t.merchant_id = c.merchant_id
           WHERE t.merchant_id IS NOT NULL AND c.merchant_id IS NULL""",
    ),
    (
        "TRANSACTION_CRITICAL_NULL", "HIGH", "transactions",
        """SELECT t.transaction_id AS record_key, t.merchant_id AS related_key,
                 'amount/currency/created_at/status missing' AS details
           FROM silver.transactions_current t
           WHERE t.amount IS NULL OR t.currency IS NULL
              OR t.created_at IS NULL OR t.status IS NULL""",
    ),
]


def run_quality(db_path: Path) -> dict[str, int]:
    db_path = Path(db_path).expanduser().resolve()
    if not db_path.is_file():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")
    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN TRANSACTION")
        # Falha cedo se a Silver não foi materializada.
        required = ["transactions_current", "reconciliation_runs_current",
                    "reconciliation_results_current", "enterprise_company_current", "cdc_conflicts"]
        for name in required:
            con.execute(f'SELECT COUNT(*) FROM silver."{name}"').fetchone()
        con.execute("CREATE SCHEMA IF NOT EXISTS dq")
        con.execute("""
            CREATE OR REPLACE TABLE dq.issues (
                issue_id VARCHAR NOT NULL,
                rule_id VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                source_table VARCHAR NOT NULL,
                record_key VARCHAR,
                related_key VARCHAR,
                details VARCHAR,
                evaluated_at TIMESTAMPTZ NOT NULL
            )
        """)
        for rule_id, severity, source, sql in RULES:
            # id estável; evita duplicar entre reprocessamentos. Normaliza NULLs.
            con.execute(f"""
                INSERT INTO dq.issues
                SELECT md5(? || '|' || COALESCE(CAST(q.record_key AS VARCHAR), '<NULL>')
                           || '|' || COALESCE(CAST(q.related_key AS VARCHAR), '<NULL>')),
                       ?, ?, ?, CAST(q.record_key AS VARCHAR),
                       CAST(q.related_key AS VARCHAR), CAST(q.details AS VARCHAR), current_timestamp
                FROM ({sql}) q
            """, [rule_id, rule_id, severity, source])
        con.execute("""
            INSERT INTO dq.issues
            SELECT md5('CDC_AMBIGUOUS_LATEST|' || source_table || '|' || business_key ||
                       '|' || CAST(event_timestamp AS VARCHAR)),
                   'CDC_AMBIGUOUS_LATEST', 'HIGH', source_table,
                   business_key, CAST(event_timestamp AS VARCHAR),
                   'Multiple latest CDC payloads at same timestamp', current_timestamp
            FROM silver.cdc_conflicts
            GROUP BY source_table, business_key, event_timestamp
        """)
        # Evita afirmar falsamente que a ausência da transação interna nos eventos
        # UNRECONCILED_PROCESSOR é necessariamente um defeito.
        con.execute("""
            CREATE OR REPLACE VIEW dq.summary AS
            SELECT rule_id, severity, source_table, COUNT(*) AS issue_count
            FROM dq.issues GROUP BY 1, 2, 3
        """)
        duplicates = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT issue_id) FROM dq.issues
        """).fetchone()[0]
        if duplicates:
            raise ValueError(f"Quality rules generated {duplicates} duplicate issue IDs")
        report = dict(con.execute("SELECT rule_id, COUNT(*) FROM dq.issues GROUP BY 1 ORDER BY 1").fetchall())
        con.execute("COMMIT")
        return report
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 3 - Data quality assessment")
    parser.add_argument("--db", type=Path, default=Path("data/marvin.duckdb"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = run_quality(args.db)
    for rule_id, count in report.items():
        LOG.info("%s: %s issues", rule_id, count)
    LOG.info("Total: %s issues", sum(report.values()))


if __name__ == "__main__":
    main()
