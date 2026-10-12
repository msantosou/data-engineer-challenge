# Etapa 2 — Silver + CDC

## Premissas

A Bronze preserva todos os eventos `I`, `U`, `D`. A Silver reconstrói o último evento por chave, ordenando pelo `_timestamp` descendente, **antes** de remover eventos `D`. O evento `D` mais recente retira a chave da tabela atual. Não há JOIN entre `transactions` e `reconciliation_results`: inconsistências entre fontes serão tratadas na Etapa 3.

| Tabela Silver | Chave CDC | Semântica |
|---|---|---|
| `silver.transactions_current` | `transaction_id` | uma linha por transação ativa |
| `silver.reconciliation_runs_current` | `id` | uma linha por execução ativa |
| `silver.reconciliation_results_current` | `id` | uma linha por resultado ativo; NÃO deduplicar por `transaction_id` |
| `silver.enterprise_company_current` | `id` | uma linha por registro da empresa |
| `silver.paysettler_events` | — | view sobre eventos CSV; sem CDC disponível |

O atributo `merchant_id` **não** é chave CDC de `enterprise_company` nesta versão; a regra implementada é por `id`. A unicidade por merchant deve ser avaliada na Etapa 3.

A implementação rejeita eventos sem chave, sem timestamp, com `Op` inválido e chaves cujo timestamp máximo possui payloads conflitantes. Isso evita eleger arbitrariamente qual `U`/`D` prevalece. Duplicatas idênticas não modificam o estado. A Bronze fica preservada.

## Instalação

Copie `scripts/silver_cdc.py` e `tests/test_silver_cdc.py` para as pastas homônimas do projeto. Mantenha o bind mount de `./scripts:/app/scripts` e `./tests:/app/tests` no Compose.

Feche a conexão da UI que mantém `/app/data/marvin.duckdb` aberto para escrita. Em Ubuntu:

```bash
docker compose exec pipeline python scripts/silver_cdc.py --db /app/data/marvin.duckdb
docker compose exec pipeline pytest tests/test_silver_cdc.py -v
```

Opcional, pipeline completo:

```bash
docker compose exec pipeline python scripts/bronze_ingest.py --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb
docker compose exec pipeline python scripts/silver_cdc.py --db /app/data/marvin.duckdb
```

## Validar no DuckDB UI (após encerrar o processo de escrita)

```sql
SELECT COUNT(*) AS total, COUNT(DISTINCT transaction_id) AS unique_keys
FROM silver.transactions_current;

SELECT COUNT(*) AS total, COUNT(DISTINCT id) AS unique_keys
FROM silver.reconciliation_results_current;

SELECT COUNT(*) AS deleted_still_current
FROM (
  SELECT transaction_id,
         ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _timestamp DESC) rn,
         Op
  FROM bronze.transactions
) b
JOIN silver.transactions_current t USING (transaction_id)
WHERE b.rn = 1 AND UPPER(TRIM(b.Op)) = 'D';
```

O `deleted_still_current` deve ser 0. O restante do Bronze nunca é apagado.

## Escopo explícito

Esta etapa faz reconstrução **full refresh**, transacional e idempotente das tabelas Silver; não oferece checkpoints ou streaming. `_timestamp` é usado como ordem CDC e não como data do evento financeiro. A Silver não valida consistência cruzada nem interpreta transações `REVERSED` como `Op='D'`. Diferenças de schema já foram normalizadas pela Bronze. `TRY_CAST` da Bronze pode produzir nulos, a serem examinados em Data Quality.
