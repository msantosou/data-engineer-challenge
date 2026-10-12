# Etapa 1 — Bronze + ingestão

## Contrato

Cinco fontes independentes, sem joins e sem deduplicação:

- `bronze.transactions`: todos os eventos de `transactions_batch_*.parquet`, com schema drift (`payment_method` nulo no lote antigo).
- `bronze.reconciliation_runs`: todos os eventos CDC de execução.
- `bronze.reconciliation_results`: todos os eventos CDC de resultado.
- `bronze.enterprise_company`: todos os eventos CDC cadastrais.
- `bronze.paysettler`: linhas dos arquivos `settlement*.csv`.

Tipos normalizados: valores financeiros em `DECIMAL(20,2)`, datas em `DATE`, instantes em `TIMESTAMPTZ`, chaves de negócio como `VARCHAR` e identificadores numéricos em `BIGINT`. Conversões inválidas tornam-se `NULL` em colunas tipadas, **mas ficam preservadas em `_raw_payload`** para investigação na etapa de Data Quality.

Metadados de cada linha: `_source_file`, `_ingestion_row_number` (ordinal da execução, não chave estável), `_ingested_at`, `_raw_payload`.

## Execução

No host Ubuntu, na raiz do projeto, verifique se o Compose tem os mounts `./scripts:/app/scripts`, `./tests:/app/tests`, `./docs/sample-data:/app/docs/sample-data:ro`, `./data:/app/data`.

```bash
mkdir -p data
docker compose up -d --build
docker compose exec pipeline python scripts/bronze_ingest.py --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb
docker compose exec pipeline pytest tests/test_bronze_ingest.py -v
```

Para o dataset de teste gerado anteriormente, use `--input-dir /app/data/generated_test` se a pasta existir no volume.

**Importante:** feche DuckDB CLI/UI que mantenha conexão de escrita com `/app/data/marvin.duckdb` antes de rodar a ingestão. Abra novamente após a conclusão.

## Validação

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze' ORDER BY table_name;
SELECT COUNT(*) FROM bronze.transactions;
SELECT "Op", COUNT(*) FROM bronze.transactions GROUP BY 1 ORDER BY 1;
SELECT _source_file, COUNT(*) FROM bronze.transactions GROUP BY 1 ORDER BY 1;
DESCRIBE bronze.transactions;
```

## Trade-offs e limites

- Bronze é um **snapshot substituído transacionalmente**, não uma camada incremental por arquivo. Reexecução com os mesmos arquivos não duplica linhas; a substituição de todas as cinco tabelas ocorre em uma transação.
- Mantém dados inconsistentes e operações `D`; a etapa Silver vai interpretar CDC.
- Não corrige `merchant_id`, divergências financeiras ou a janela de reconciliação.
- Conversões com `TRY_CAST` são deliberadamente tolerantes; a etapa DQ precisa contar registros inválidos.
- A ingestão assume nomes de arquivos do desafio. O schema CSV é inferido do cabeçalho, não dos tipos.
- `_ingestion_row_number` não representa posição física estável do arquivo e não deve ser usado como identidade CDC.
- Ao trocar datasets, execute com `--input-dir` apontando apenas para o conjunto desejado.
