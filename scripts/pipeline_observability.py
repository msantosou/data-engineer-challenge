"""Persist pipeline telemetry in ops schema of the existing DuckDB database.

Separate connections are opened after each stage has closed its writer connection.
Observability is best-effort only for finish_run; start/step writes fail loudly.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

DDL = [
    """CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
        run_id VARCHAR PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        duration_seconds DOUBLE,
        status VARCHAR NOT NULL,
        source_dir VARCHAR NOT NULL,
        with_gold BOOLEAN NOT NULL,
        verify_idempotency BOOLEAN NOT NULL,
        error_message VARCHAR
    )""",
    """CREATE TABLE IF NOT EXISTS ops.pipeline_steps (
        run_id VARCHAR NOT NULL,
        step_name VARCHAR NOT NULL,
        attempt INTEGER NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        duration_seconds DOUBLE NOT NULL,
        status VARCHAR NOT NULL,
        report_json VARCHAR,
        error_message VARCHAR,
        PRIMARY KEY (run_id, step_name, attempt)
    )""",
    """CREATE TABLE IF NOT EXISTS ops.pipeline_metrics (
        run_id VARCHAR NOT NULL,
        step_name VARCHAR NOT NULL,
        attempt INTEGER NOT NULL,
        metric_name VARCHAR NOT NULL,
        metric_value DOUBLE NOT NULL,
        PRIMARY KEY (run_id, step_name, attempt, metric_name)
    )""",
]


def numeric_metrics(report: Any, prefix: str = "") -> dict[str, float]:
    """Collect numeric metrics only; booleans and non-finite values are excluded."""
    import math
    result: dict[str, float] = {}
    if isinstance(report, dict):
        for key, value in report.items():
            result.update(numeric_metrics(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(report, (int, float)) and not isinstance(report, bool) and math.isfinite(report):
        result[prefix] = float(report)
    return result


class Observer:
    def __init__(self, db: str | Path):
        self.db = str(db)
        with duckdb.connect(self.db) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS ops")
            for ddl in DDL:
                con.execute(ddl)

    def start_run(self, run_id: str, source_dir: Path, with_gold: bool,
                  verify_idempotency: bool, started_at: datetime) -> None:
        with duckdb.connect(self.db) as con:
            con.execute("""INSERT INTO ops.pipeline_runs
                (run_id, started_at, status, source_dir, with_gold, verify_idempotency)
                VALUES (?, ?, 'RUNNING', ?, ?, ?)""",
                [run_id, started_at, str(source_dir), with_gold, verify_idempotency])

    def record_step(self, run_id: str, step_name: str, status: str,
                    started_at: datetime, duration_seconds: float,
                    report: Any, error: str | None) -> None:
        with duckdb.connect(self.db) as con:
            con.begin()
            try:
                attempt = con.execute("""SELECT COALESCE(MAX(attempt), 0) + 1
                    FROM ops.pipeline_steps WHERE run_id = ? AND step_name = ?""",
                    [run_id, step_name]).fetchone()[0]
                con.execute("""INSERT INTO ops.pipeline_steps VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [run_id, step_name, attempt, started_at, datetime.now(timezone.utc),
                     duration_seconds, status, json.dumps(report, default=str) if report is not None else None,
                     error[:4000] if error else None])
                for name, value in numeric_metrics(report).items():
                    con.execute("INSERT INTO ops.pipeline_metrics VALUES (?, ?, ?, ?, ?)",
                                [run_id, step_name, attempt, name, value])
                con.commit()
            except Exception:
                con.rollback()
                raise

    def finish_run(self, run_id: str, status: str, duration_seconds: float,
                   error: str | None) -> None:
        with duckdb.connect(self.db) as con:
            con.execute("""UPDATE ops.pipeline_runs SET finished_at = ?,
                duration_seconds = ?, status = ?, error_message = ? WHERE run_id = ?""",
                [datetime.now(timezone.utc), duration_seconds, status,
                 error[:4000] if error else None, run_id])
