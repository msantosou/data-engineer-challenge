# Etapa 5 — Testes, CLI e documentação da entrega

## Contrato de execução

Pré-requisitos: Docker Compose, arquivos de origem em `docs/sample-data/`, `data/` com bind mount para `/app/data`, dependências do `pyproject.toml` instaladas na imagem. **Feche a conexão de escrita DuckDB UI/CLI antes dos comandos** para evitar lock.

**Etapas 1–3 (já implementadas):**

```bash
docker compose up -d
docker compose exec pipeline python scripts/run_pipeline.py \
  --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb
```

**Reprocessamento idempotente (duas execuções completas):**

```bash
docker compose exec pipeline python scripts/run_pipeline.py \
  --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb \
  --verify-idempotency
```

**Testes unitários/de integração disponíveis:**

```bash
docker compose exec pipeline python -m pytest tests/ -v
```

**Etapa 4 — Gold:** ainda deve ser implementada. Após disponibilizar `scripts/gold.py` expondo `materialize(db_path: Path)`, rode:

```bash
docker compose exec pipeline python scripts/run_pipeline.py \
  --input-dir /app/docs/sample-data --db /app/data/marvin.duckdb \
  --with-gold --gold-script /app/scripts/gold.py
```

O comando sem `--with-gold` **não executa nem valida métricas Gold**. A verificação de idempotência incluída cobre somente Bronze, Silver e `dq.issues` — a integração Gold precisa acrescentar testes e snapshots das próprias tabelas Gold.

## Decisões

- Bronze: full refresh transacional dos arquivos de origem com histórico CDC preservado.
- Silver: materialização do estado atual por chave; deletados ausentes, empates ambíguos em `silver.cdc_conflicts`.
- Qualidade: divergências preservadas em `dq.issues` e agregadas em `dq.summary`.
- Não corrigir `merchant_id`, `amount` ou datas por inferência. A comparação interfontes com o estado **atual** não prova erro no estado **histórico**.
- O check de idempotência compara conteúdo lógico ignorando apenas timestamps de processamento (`_ingested_at`, `evaluated_at`) e ordinais de ingestão. Não verifica invariância de performance nem resistência a falhas distribuídas.
- O teste faz duas execuções de substituição; não é teste de ingestão incremental.

## Critérios de aceite

1. `pytest tests/ -v` passa no container.
2. `--verify-idempotency` termina com `Idempotência OK`.
3. SQL: `COUNT(*) = COUNT(DISTINCT transaction_id)` em `silver.transactions_current`.
4. SQL: chaves em `silver.cdc_conflicts` não aparecem em `silver.transactions_current`.
5. `dq.issues` e `dq.summary` acessíveis; divergências não alteram as fontes.
6. Etapa 4: **pendente**, três produtos analíticos e seus contratos precisam ser implementados e testados antes de considerar o desafio completo.

## Troubleshooting

- `service "pipeline" is not running`: `docker compose up -d` e `docker compose ps`.
- `Could not set lock on file`: encerre sessão DuckDB UI/CLI conectada em modo escrita.
- `Script ausente`: extraia o ZIP na raiz do repositório (com as pastas `scripts`, `tests`, `docs`).
- `FALHA na idempotência`: investigue a(s) tabela(s) mencionada(s); evite alterar os dados de origem durante a comparação.

## Evidência de qualidade de dados

Amostra original já mostrou incompatibilidades elevadas entre valores/merchants de reconciliations e transactions. Registrar no README de entrega e tratar o conjunto de dados original como fonte recebida; não ajustar arbitrariamente para obter matches.
