# Etapa 3 — Data Quality

## Objetivo

Gerar diagnósticos persistentes em `dq.issues` e um resumo em `dq.summary`, sem atualizar nem corrigir as tabelas Bronze/Silver. Uma linha representa **uma ocorrência de uma regra por registro**; a mesma transação pode ter vários problemas.

## Execução

Feche a conexão de escrita do DuckDB UI e rode:

```bash
docker compose exec pipeline python scripts/data_quality.py --db /app/data/marvin.duckdb
docker compose exec pipeline pytest tests/test_data_quality.py -v
```

No DuckDB UI:

```sql
SELECT * FROM dq.summary ORDER BY issue_count DESC;
SELECT * FROM dq.issues WHERE rule_id = 'MERCHANT_MISMATCH' LIMIT 20;
SELECT source_table, COUNT(*) FROM dq.issues GROUP BY 1;
```

## Regras implementadas

| Rule ID | Severidade | Semântica |
|---|---|---|
| `RECON_RUN_MISSING` | HIGH | Resultado aponta para run ausente no current |
| `TRANSACTION_MISSING` | HIGH | Resultado que deveria ter transação interna não a encontra no current |
| `MERCHANT_MISMATCH` | HIGH | Merchant da reconciliação diverge daquele na transação atual |
| `INTERNAL_AMOUNT_MISMATCH` | HIGH | Valor interno informado na reconciliação difere do valor atual da transação por mais de R$0,01 |
| `TRANSACTION_OUTSIDE_WINDOW` | MEDIUM | `created_at` (UTC) fora da janela de 7 dias da `reference_date` |
| `RECON_DIFFERENCE_INCORRECT` | HIGH | `difference` diverge do módulo da subtração dos valores disponíveis |
| `MATCHED_ABOVE_TOLERANCE` | HIGH | MATCHED acima da tolerância R$0,01 ou diferença inválida |
| `MERCHANT_NOT_IN_COMPANY` | MEDIUM | Merchant da transação não existe no cadastro atual |
| `TRANSACTION_CRITICAL_NULL` | HIGH | Campo crítico nulo na transação atual |
| `CDC_AMBIGUOUS_LATEST` | HIGH | Estado mais recente ambíguo, já isolado na Silver |

## Interpretação e limitações

- Regras cross-source comparam `reconciliation_results_current` a `transactions_current`, **não** fazem reconstrução point-in-time. Merchant, montante e janela podem divergir por evolução temporal: são indícios de qualidade a investigar, não verdades históricas definitivas.
- `UNRECONCILED_PROCESSOR` sem transação interna é comportamento esperado, não gera automaticamente `TRANSACTION_MISSING`.
- Uma chave excluída ou em quarentena na Silver pode aparecer como missing no current. Consultar `silver.cdc_conflicts` e eventos Bronze antes de concluir que inexiste na origem.
- `dq.issues` é um **snapshot completo da última avaliação**: a execução recria a tabela em transação, evitando duplicatas no rerun. `evaluated_at` muda entre execuções; não se trata de histórico imutável de runs de DQ.
- O script não altera categorias, merchants, valores ou timestamps de origem. Dados com `TRY_CAST` malsucedido podem ficar nulos na Bronze; o payload original deve ser consultado para diagnóstico.
- Os números de issues **não** podem ser somados como "percentual de registros ruins" sem considerar múltiplas regras por registro e diferentes denominadores.
- Não é criada política de bloqueio/alerta nesta etapa. Antes de publicar Gold, documentar consumidores, thresholds e impacto de registros em quarentena.
- Fonte PaySettler é preservada, mas não recebe cruzamento SQL na etapa 3: regras de sua identidade e reconciliação dependem de um contrato de correspondência validado. Esta etapa não afirma ter validado integridade ponta a ponta do processador.
