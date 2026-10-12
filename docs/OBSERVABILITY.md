# Observabilidade persistente do pipeline

## Escopo

O CLI grava telemetria no mesmo arquivo DuckDB do pipeline, em tabelas **append-only para cada execução**:

- `ops.pipeline_runs`: uma linha por invocação do CLI (`RUNNING`, `SUCCESS` ou `FAILED`).
- `ops.pipeline_steps`: uma linha por etapa e tentativa, inclusive as duas passagens quando `--verify-idempotency` está ativo.
- `ops.pipeline_metrics`: contagens numéricas dos relatórios das etapas, por tentativa.

Cada execução tem UUID próprio. Uma reconstrução das tabelas de negócio não elimina o histórico em `ops`.

## Executar

Feche a conexão de escrita do DuckDB UI antes de executar:

```bash
docker compose exec pipeline python scripts/run_pipeline.py \
  --input-dir /app/docs/sample-data \
  --db /app/data/marvin.duckdb \
  --with-gold --gold-script scripts/gold.py
```

Para executar sem persistência de telemetria, use `--no-observability`.

## Consultar

```sql
-- Últimas execuções
SELECT run_id, started_at, finished_at, duration_seconds, status, error_message
FROM ops.pipeline_runs
ORDER BY started_at DESC LIMIT 20;

-- Duração e estado por etapa
SELECT run_id, step_name, attempt, duration_seconds, status, error_message
FROM ops.pipeline_steps
ORDER BY started_at DESC;

-- Volume de registros processados por etapa
SELECT run_id, step_name, attempt, metric_name, metric_value
FROM ops.pipeline_metrics
ORDER BY run_id, step_name, metric_name;

-- Último sucesso e número de falhas
SELECT MAX(finished_at) FILTER (WHERE status='SUCCESS') AS last_success,
       COUNT(*) FILTER (WHERE status='FAILED') AS failures
FROM ops.pipeline_runs;
```

## Considerações operacionais

- DuckDB admite um escritor independente de cada vez. As gravações `ops` ocorrem **entre as etapas**, após cada função retornar/fechar a conexão. A UI não deve manter um escritor concorrente.
- Se o processo for morto abruptamente (`SIGKILL`, queda da VM), uma execução pode ficar como `RUNNING`; detectar registros antigos e reconciliar seu estado exige rotina adicional.
- A inicialização da observabilidade é obrigatória por padrão: erro de abertura do banco aborta o CLI. O registro final é best-effort: se falhar, o erro fica no log do container.
- Métricas são relatórios de execução, não contratos financeiros validados. Não substituem `dq.issues` nem monitoramento de freshness por SLA.
- Observabilidade fica no mesmo arquivo que o ETL: indisponibilidade do banco impede também a gravação da falha. Em produção, utilizar serviço externo de logs/telemetria.
- `--verify-idempotency` compara conteúdo lógico das camadas de negócio; tabelas `ops` são intencionalmente ignoradas por serem históricas.
