from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import uuid

import duckdb

MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'pipeline_observability.py'
spec = importlib.util.spec_from_file_location('pipeline_observability', MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Observer = module.Observer


def test_persists_success_metrics_and_attempts(tmp_path):
    db = tmp_path / 'test.duckdb'
    o = Observer(db)
    rid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    o.start_run(rid, tmp_path, True, True, now)
    report = {'transactions': {'current': 992, 'events': 1060}, 'ok': True}
    o.record_step(rid, 'silver', 'SUCCESS', now, .3, report, None)
    o.record_step(rid, 'silver', 'SUCCESS', now, .4, report, None)
    o.finish_run(rid, 'SUCCESS', 1.0, None)
    with duckdb.connect(str(db)) as c:
        assert c.execute('SELECT status FROM ops.pipeline_runs').fetchone()[0] == 'SUCCESS'
        assert c.execute('SELECT COUNT(*) FROM ops.pipeline_steps').fetchone()[0] == 2
        assert c.execute("SELECT SUM(metric_value) FROM ops.pipeline_metrics WHERE metric_name = 'transactions.current'").fetchone()[0] == 1984
        assert c.execute("SELECT COUNT(*) FROM ops.pipeline_metrics WHERE metric_name = 'ok'").fetchone()[0] == 0


def test_persists_failure(tmp_path):
    db = tmp_path / 'test.duckdb'
    o = Observer(db)
    rid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    o.start_run(rid, tmp_path, False, False, now)
    o.record_step(rid, 'bronze', 'FAILED', now, .1, None, 'invalid input')
    o.finish_run(rid, 'FAILED', .2, 'invalid input')
    with duckdb.connect(str(db)) as c:
        assert c.execute('SELECT status, error_message FROM ops.pipeline_runs').fetchone() == ('FAILED', 'invalid input')
        assert c.execute('SELECT status FROM ops.pipeline_steps').fetchone()[0] == 'FAILED'
