"""Execute Bronze -> Silver -> Data Quality, optionally Gold, and verify repeatability.

Usage (from project root):
  python scripts/run_pipeline.py --input-dir docs/sample-data --db data/marvin.duckdb
  python scripts/run_pipeline.py --input-dir docs/sample-data --db data/marvin.duckdb --verify-idempotency
  python scripts/run_pipeline.py --with-gold --gold-script scripts/gold.py

Do not run while the DuckDB CLI/Web UI holds a write connection to the DB.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import logging
from pathlib import Path
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb

LOG = logging.getLogger("pipeline")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_TABLES = ["transactions", "reconciliation_runs", "reconciliation_results", "enterprise_company", "paysettler"]
SILVER_TABLES = ["transactions_current", "reconciliation_runs_current", "reconciliation_results_current", "enterprise_company_current", "cdc_conflicts"]


def load_module(name: str, script: Path) -> Any:
    if not script.is_file():
        raise FileNotFoundError(f"Script ausente: {script}")
    spec = importlib.util.spec_from_file_location(name, script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Não foi possível carregar {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def execute_stages(input_dir: Path, db: Path, *, with_gold: bool, gold_script: Path | None,
                   observer: Any = None, run_id: str | None = None) -> dict[str, Any]:
    scripts = PROJECT_ROOT / "scripts"
    bronze = load_module("bronze_ingest_stage5", scripts / "bronze_ingest.py")
    silver = load_module("silver_cdc_stage5", scripts / "silver_cdc.py")
    dq = load_module("data_quality_stage5", scripts / "data_quality.py")
    steps = [
        ("bronze", lambda: bronze.ingest(input_dir, db)),
        ("silver", lambda: silver.materialize(db)),
        ("data_quality", lambda: dq.run_quality(db)),
    ]
    if with_gold:
        if gold_script is None:
            raise ValueError("Para executar a Gold, informe --gold-script")
        gold = load_module("gold_stage5", gold_script)
        if not hasattr(gold, "materialize"):
            raise ValueError("O modulo Gold precisa expor materialize(db_path: Path)")
        steps.append(("gold", lambda: gold.materialize(db)))
    if not with_gold:
        LOG.warning("Gold nao executada; use --with-gold --gold-script scripts/gold.py")
    result = {}
    for i, (name, fn) in enumerate(steps, 1):
        LOG.info("Etapa %s/%s: %s", i, len(steps), name)
        started = datetime.now(timezone.utc)
        tick = time.perf_counter()
        try:
            report = fn()
        except Exception as exc:
            if observer is not None and run_id is not None:
                observer.record_step(run_id, name, "FAILED", started,
                                     time.perf_counter() - tick, None, str(exc))
            raise
        if observer is not None and run_id is not None:
            observer.record_step(run_id, name, "SUCCESS", started,
                                 time.perf_counter() - tick, report, None)
        result[name] = report
    return result


def snapshot(db: Path) -> dict[str, dict[str, Any]]:
    """Digest exato do conteúdo lógico para fixtures locais; ignora timestamps técnicos de carga.

    Ordena linhas por representação normalizada, sem depender da ordem física. Não usar para
    datasets enormes: o ORDER BY precisa de recursos proporcionais ao tamanho da tabela.
    """
    con = duckdb.connect(str(db), read_only=True)
    result: dict[str, dict[str, Any]] = {}
    try:
        tables = [("bronze", n) for n in BRONZE_TABLES] + [("silver", n) for n in SILVER_TABLES] + [("dq", "issues")]
        for schema, name in tables:
            exists = con.execute("""SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = ? AND table_name = ?""", [schema, name]).fetchone()[0]
            if not exists:
                raise RuntimeError(f"Tabela esperada ausente: {schema}.{name}")
            cols = [r[1] for r in con.execute(f"PRAGMA table_info('{schema}.{name}')").fetchall()]
            ignored = {"_ingested_at", "_ingestion_row_number", "evaluated_at"}
            chosen = [c for c in cols if c not in ignored]
            select_cols = ", ".join('"' + c.replace('"', '""') + '"' for c in chosen)
            query = f'SELECT {select_cols} FROM "{schema}"."{name}" ORDER BY ALL'
            cursor = con.execute(query)
            digest = hashlib.sha256()
            count = 0
            while True:
                rows = cursor.fetchmany(500)
                if not rows:
                    break
                for row in rows:
                    digest.update(json.dumps(row, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8"))
                    digest.update(b"\n")
                    count += 1
            result[f"{schema}.{name}"] = {"rows": count, "sha256": digest.hexdigest()}
        # View com dados de origem precisa continuar acessível.
        result["silver.paysettler_events"] = {"rows": con.execute("SELECT COUNT(*) FROM silver.paysettler_events").fetchone()[0]}
    finally:
        con.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Marvin pipeline: Bronze -> Silver -> DQ [-> Gold]")
    parser.add_argument("--input-dir", type=Path, default=PROJECT_ROOT / "docs/sample-data")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data/marvin.duckdb")
    parser.add_argument("--with-gold", action="store_true", help="Executa Etapa 4; requer --gold-script")
    parser.add_argument("--gold-script", type=Path, help="Script Python com função materialize(db_path)")
    parser.add_argument("--verify-idempotency", action="store_true", help="Executa duas vezes e compara snapshots lógicos")
    parser.add_argument("--no-observability", action="store_true", help="Desabilita escrita em ops")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.with_gold and not args.gold_script:
        parser.error("--with-gold requer --gold-script")
    db = args.db.expanduser().resolve()
    source = args.input_dir.expanduser().resolve()
    gold_path = args.gold_script.expanduser().resolve() if args.gold_script else None
    observer = None
    run_id = str(uuid.uuid4())
    run_started = datetime.now(timezone.utc)
    run_tick = time.perf_counter()
    status = "SUCCESS"
    error = None
    if not args.no_observability:
        from pipeline_observability import Observer
        observer = Observer(db)
        observer.start_run(run_id, source, args.with_gold, args.verify_idempotency, run_started)
    try:
        first = execute_stages(source, db, with_gold=args.with_gold,
                               gold_script=gold_path, observer=observer, run_id=run_id)
        if args.verify_idempotency:
            before = snapshot(db)
            execute_stages(source, db, with_gold=args.with_gold,
                           gold_script=gold_path, observer=observer, run_id=run_id)
            after = snapshot(db)
            if before != after:
                changed = [k for k in sorted(set(before) | set(after)) if before.get(k) != after.get(k)]
                raise RuntimeError("FALHA na idempotencia: " + ", ".join(changed))
            LOG.info("Idempotencia OK: conteudo logico identico apos reprocessamento")
        print(json.dumps(first, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        status, error = "FAILED", str(exc)
        LOG.exception("Pipeline falhou: %s", exc)
        raise
    finally:
        if observer is not None:
            try:
                observer.finish_run(run_id, status, time.perf_counter() - run_tick, error)
            except Exception:
                LOG.exception("Nao foi possivel persistir observabilidade; consultar logs do container")


if __name__ == "__main__":
    main()
