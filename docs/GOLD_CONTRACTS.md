# Stage 4 — Gold data contracts

## Preconditions
Run Bronze and Silver first. Close any DuckDB UI / CLI connection holding a writer lock before invoking Gold.

## Products

### `gold.daily_financial_volume` — CFO
- **Grain:** one row per UTC `transaction_date` + `currency`.
- **Source:** `silver.transactions_current` (CDC resolved; tombstones and ambiguous keys excluded).
- **Population:** `status = 'COMPLETED'`, non-null `created_at`, `amount`, and `currency`.
- **Metrics:** `transaction_count`, `gross_amount`, `avg_ticket`.
- **Interpretation:** current snapshot of completed internal transactions grouped by **creation date**; **not** processor-settled volume, net revenue, nor an as-of historical ledger. Revisions to old transactions can restate a historical date. Amounts from distinct currencies are never summed together.
- **Known exclusions:** failed/pending, null critical dimensions/amount, quarantined keys; inspect Silver/DQ for omissions.

### `gold.daily_reconciliation_metrics` — Operations
- **Grain:** one row per `reference_date`.
- **Source:** `silver.reconciliation_runs_current`, `silver.reconciliation_results_current`.
- **Selection:** latest `COMPLETED` run per `reference_date`, ordered by `completed_at`, then `started_at`, then `id` descending. This is a documented heuristic when no authoritative run-version field exists.
- **Metrics:** result counts for `MATCHED`, `MISMATCHED`, `UNRECONCILED_INTERNAL`, `UNRECONCILED_PROCESSOR`, unknown category; `mismatched_difference_amount` (sum absolute `difference` for MISMATCHED); percentage rates using **all results in the selected run** as denominator. Zero denominator yields NULL percentages.
- **Interpretation:** **reported** reconciliation health from provided classifications, **not** an independently verified match against the internal transaction amounts. The provided sample sources have high cross-source DQ discrepancies.
- **Caveat:** No seven-day rolling trend calculated here; consumers can compute it from this per-date fact. Run metadata count need not equal result rows; use DQ to investigate.

### `gold.reconciliation_audit_trail` — Compliance
- **Grain:** one row per **Bronze reconciliation-result CDC event** (not per transaction or result ID).
- **Source:** `bronze.reconciliation_results`, left joined with the current Silver run record.
- **Metadata:** result event operation, event timestamp, source file, ingestion row ordinal; run reference date, file name, current status, start and completion timestamps, `missing_run_reference`.
- **Interpretation:** preserves the event-level reconciliation result history, including duplicates/updates/deletes. **Run fields are the current run snapshot, not run values as of each event**. For full historical run-state reconstruction, use `bronze.reconciliation_runs` alongside this table.
- **Limit:** `_ingestion_row_number` is not a stable event identifier across full reloads; use source identifiers + event metadata for investigative context.

## Quality policy
No joins from `reconciliation_results` to `transactions` in Gold financial metrics: data profiling found substantial inconsistency between the sample sources. Gold does not silently fix or rewrite mismatched amounts/merchants. Issues remain visible in `dq.issues`, and CDC ambiguity in `silver.cdc_conflicts`.

## Execution
```bash
# From host project directory (Docker running, DB writer closed):
docker compose exec pipeline python scripts/gold.py --db /app/data/marvin.duckdb

docker compose exec pipeline pytest tests/test_gold.py -v

# Stage 5 CLI already supports this interface:
docker compose exec pipeline python scripts/run_pipeline.py \
  --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb \
  --with-gold --gold-script scripts/gold.py
```

## Sample queries
```sql
SELECT * FROM gold.daily_financial_volume ORDER BY transaction_date, currency;
SELECT * FROM gold.daily_reconciliation_metrics ORDER BY reference_date;
SELECT * FROM gold.reconciliation_audit_trail WHERE transaction_id = 'YOUR_TRANSACTION_ID';
```

## Reload and locking
Tables are replaced inside one DuckDB transaction; rerunning against unchanged Silver/Bronze data yields unchanged logical business outputs. Keep the DuckDB Web UI/CLI writer connection closed while running the pipeline.
