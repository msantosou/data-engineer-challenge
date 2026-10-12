"""Etapa 2 — materializa estado atual de fontes CDC a partir da Bronze.

Execução: python scripts/silver_cdc.py --db /app/data/marvin.duckdb
Requisitos: executar scripts/bronze_ingest.py antes.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

LOG = logging.getLogger("silver_cdc")

# Nome da fonte, chave natural do CDC. Id de resultado é diferente de transaction_id:
# uma mesma transação pode ter resultados em diversas execuções.
CDC_KEYS = {
    "transactions": "transaction_id",
    "reconciliation_runs": "id",
    "reconciliation_results": "id",
    "enterprise_company": "id",
}


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _required_columns(con: duckdb.DuckDBPyConnection, table: str, required: set[str]) -> None:
    columns = {row[1] for row in con.execute(f"PRAGMA table_info('bronze.{table}')").fetchall()}
    missing = required - columns
    if missing:
        raise ValueError(f"bronze.{table}: colunas ausentes: {sorted(missing)}")


def materialize(db_path: Path) -> dict[str, dict[str, int]]:
    """Atualiza Silver atomicamente; chaves ambíguas vão para quarentena.

    Regra: maior _timestamp por chave; empates de payload diferente são
    quarentenados (sem desempate arbitrário). Delete mais recente remove chave.
    """
    db_path = Path(db_path).expanduser().resolve()
    if not db_path.is_file():
        raise FileNotFoundError(f"Banco Bronze não encontrado: {db_path}")

    con = duckdb.connect(str(db_path))
    report: dict[str, dict[str, int]] = {}
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("""
            CREATE OR REPLACE TABLE silver.cdc_conflicts (
                source_table VARCHAR,
                business_key VARCHAR,
                event_op VARCHAR,
                event_timestamp TIMESTAMPTZ,
                source_file VARCHAR,
                raw_payload VARCHAR
            )
        """)
        for table, key in CDC_KEYS.items():
            qualified = f"bronze.{identifier(table)}"
            key_sql = identifier(key)
            _required_columns(con, table, {key, "Op", "_timestamp", "_raw_payload", "_source_file"})

            # Abort rather than silently discard events with critical missing CDC fields.
            malformed = con.execute(f"""
                SELECT COUNT(*) FROM {qualified}
                WHERE {key_sql} IS NULL OR _timestamp IS NULL
                   OR Op IS NULL OR UPPER(TRIM(Op)) NOT IN ('I', 'U', 'D')
            """).fetchone()[0]
            if malformed:
                raise ValueError(f"bronze.{table}: {malformed} eventos com chave/CDC inválidos")

            # Múltiplos payloads no maior timestamp da mesma chave são
            # inconclusivos. Guardamos TODOS os eventos empatados para auditoria.
            con.execute(f"""
                INSERT INTO silver.cdc_conflicts
                WITH most_recent AS (
                    SELECT *, MAX(_timestamp) OVER (PARTITION BY {key_sql}) AS max_ts
                    FROM {qualified}
                ), ambiguous AS (
                    SELECT {key_sql}
                    FROM most_recent
                    WHERE _timestamp = max_ts
                    GROUP BY {key_sql}
                    HAVING COUNT(DISTINCT COALESCE(_raw_payload, '')) > 1
                )
                SELECT ?, CAST(m.{key_sql} AS VARCHAR), m.Op, m._timestamp,
                       m._source_file, m._raw_payload
                FROM most_recent m
                JOIN ambiguous a ON m.{key_sql} = a.{key_sql}
                WHERE m._timestamp = m.max_ts
            """, [table])
            conflicts = con.execute("""
                SELECT COUNT(DISTINCT business_key)
                FROM silver.cdc_conflicts WHERE source_table = ?
            """, [table]).fetchone()[0]

            # Nunca descartar D antes do ranking: isso ressuscitaria registros.
            # Uma chave ambígua não pode aparecer no current, mesmo que
            # uma das versões empatadas pareça ser um update válido.
            con.execute(f"""
                CREATE OR REPLACE TABLE silver.{identifier(table + '_current')} AS
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {key_sql}
                        ORDER BY _timestamp DESC, _raw_payload DESC,
                                 _source_file DESC, _ingestion_row_number DESC
                    ) AS _cdc_rank
                    FROM {qualified}
                )
                SELECT * EXCLUDE (_cdc_rank)
                FROM ranked r
                WHERE _cdc_rank = 1
                  AND UPPER(TRIM(Op)) <> 'D'
                  AND NOT EXISTS (
                    SELECT 1 FROM silver.cdc_conflicts c
                    WHERE c.source_table = ?
                      AND c.business_key = CAST(r.{key_sql} AS VARCHAR)
                  )
            """, [table])
            cnt = con.execute(f"SELECT COUNT(*) FROM silver.{identifier(table + '_current')}").fetchone()[0]
            distinct = con.execute(f"SELECT COUNT(DISTINCT {key_sql}) FROM silver.{identifier(table + '_current')}").fetchone()[0]
            if cnt != distinct:
                raise AssertionError(f"silver.{table}_current não é único por {key}")
            total = con.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            deleted = con.execute(f"""
                WITH ranked AS (
                    SELECT {key_sql}, Op, ROW_NUMBER() OVER (
                        PARTITION BY {key_sql}
                        ORDER BY _timestamp DESC, _raw_payload DESC,
                                 _source_file DESC, _ingestion_row_number DESC
                    ) AS rn FROM {qualified}
                )
                SELECT COUNT(*) FROM ranked r WHERE rn = 1 AND UPPER(TRIM(Op)) = 'D'
                  AND NOT EXISTS (
                    SELECT 1 FROM silver.cdc_conflicts c
                    WHERE c.source_table = ?
                      AND c.business_key = CAST(r.{key_sql} AS VARCHAR)
                  )
            """, [table]).fetchone()[0]
            report[table] = {"events": total, "current": cnt, "deleted_keys": deleted, "ambiguous_keys": conflicts}
            LOG.info("silver.%s_current: %s ativos, %s excluídos, %s ambíguos (%s eventos)", table, cnt, deleted, conflicts, total)

        # PaySettler é CSV de eventos de liquidação, sem envelope de CDC.
        # Mantém todas as linhas e a proveniência; nenhuma deduplicação inferida.
        _required_columns(con, "paysettler", {"transaction_id", "_source_file", "_raw_payload"})
        con.execute("CREATE OR REPLACE VIEW silver.paysettler_events AS SELECT * FROM bronze.paysettler")
        report["paysettler"] = {"events": con.execute("SELECT COUNT(*) FROM bronze.paysettler").fetchone()[0]}
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Materializa Silver/current aplicando CDC")
    parser.add_argument("--db", type=Path, default=Path("data/marvin.duckdb"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    for table, stats in materialize(args.db).items():
        print(f"{table}: {stats}")


if __name__ == "__main__":
    main()
