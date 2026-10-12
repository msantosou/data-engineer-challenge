# Revisão dos contratos Gold

**Código auditado:** `scripts/gold.py` do pacote da Etapa 4. Abaixo estão os contratos efetivamente calculados e suas limitações. Não confundir métricas da fonte com validação independente da reconciliação.

## 1. CFO — `gold.daily_financial_volume`

| Item | Contrato |
|---|---|
| Consumidor | CFO / FP&A |
| Grain | 1 linha por `transaction_date` (UTC de `created_at`) **e `currency`** |
| Fonte | `silver.transactions_current` |
| Elegibilidade | `status = 'COMPLETED'`; `created_at`, `currency`, `amount` não nulos; CDC D / ambíguos fora da current |
| Chave lógica | (`transaction_date`, `currency`) |
| `transaction_count` | COUNT(*) elegíveis |
| `gross_amount` | SUM(`amount`) em `DECIMAL(24,2)` |
| `avg_ticket` | AVG(`amount`) em `DECIMAL(24,2)` |

**Responde:** quanto valor interno em transações atualmente concluídas foi criado por dia/moeda? quantas? qual ticket médio?

**Não responde:** quanto o processador efetivamente liquidou, receita líquida/take rate, caixa disponível, volume “as-of” ou volume líquido de reversões. Updates tardios podem alterar dias anteriores. Não some moedas distintas sem conversão FX externa.

```sql
SELECT transaction_date, currency, transaction_count, gross_amount, avg_ticket
FROM gold.daily_financial_volume
ORDER BY transaction_date DESC, currency;
```

## 2. Operações — `gold.daily_reconciliation_metrics`

| Item | Contrato |
|---|---|
| Consumidor | Operações / reconciliação |
| Grain | 1 linha por `reference_date` (data de negócio UTC) |
| Fontes | `silver.reconciliation_runs_current` + `silver.reconciliation_results_current` |
| Run válida | `UPPER(status) = 'COMPLETED'` |
| Run escolhida | Última por `completed_at` DESC, `started_at` DESC, `id` DESC |
| Chave lógica | `reference_date` |
| `total_results` | COUNT dos resultados da run escolhida |
| `matched_count` | COUNT categoria `MATCHED` |
| `mismatched_count` | COUNT categoria `MISMATCHED` |
| `unreconciled_internal_count` | COUNT categoria `UNRECONCILED_INTERNAL` |
| `unreconciled_processor_count` | COUNT categoria `UNRECONCILED_PROCESSOR` |
| `unknown_category_count` | nulos ou categoria fora das quatro conhecidas |
| `mismatched_difference_amount` | soma absoluta de `difference` apenas `MISMATCHED` |
| `mismatch_rate_pct` | `100 * mismatched_count / total_results` |
| `unreconciled_rate_pct` | `100 * (unreconciled_internal_count + unreconciled_processor_count) / total_results` |
| `matched_rate_pct` | `100 * matched_count / total_results` |

Divisão por zero retorna **NULL**, não zero. Taxas são sobre **todas as linhas** do run escolhido, inclusive categorias desconhecidas no denominador. O campo `difference` reflete valor **declarado** na reconciliação, não recalculado a partir de `transactions`. Tolerância de negócio é R$ 0,01; inconsistências dessa regra são expostas em DQ, não reclassificadas na Gold.

**Responde:** saúde reportada por data, categorias, total e taxa de discrepância. Uma tendência de 7 dias pode ser calculada pelo consumidor a partir da série, mas a Gold **não inclui** coluna pronta de tendência/alerta.

**Não responde:** qual o verdadeiro mismatch apurado de novo em transações/processador, qual a causa raiz, qual taxa por merchant (não há `merchant_id` no grain), SLO de execução.

```sql
SELECT reference_date, selected_run_id, total_results,
       matched_rate_pct, mismatch_rate_pct, unreconciled_rate_pct,
       mismatched_difference_amount
FROM gold.daily_reconciliation_metrics
ORDER BY reference_date;
```

Exemplo de comparação de taxas entre duas datas consecutivas (não é média móvel):

```sql
SELECT reference_date, mismatch_rate_pct,
       mismatch_rate_pct - LAG(mismatch_rate_pct) OVER (ORDER BY reference_date)
         AS delta_percentage_points
FROM gold.daily_reconciliation_metrics
ORDER BY reference_date;
```

## 3. Compliance — `gold.reconciliation_audit_trail`

| Item | Contrato |
|---|---|
| Consumidor | Compliance / investigação |
| Grain | 1 linha **por evento CDC** de `bronze.reconciliation_results` |
| Fonte principal | `bronze.reconciliation_results` |
| Contexto | LEFT JOIN em `silver.reconciliation_runs_current` por `run_id` |
| Identificação | `reconciliation_result_id`, `run_id`, `transaction_id` |
| Evidência | `category`, `internal_amount`, `processor_amount`, `difference` |
| Proveniência | `result_cdc_op`, `result_event_timestamp`, `result_source_file`, `result_ingestion_row_number` |
| Contexto da run | `reference_date`, `run_file_name`, `run_current_status`, `run_started_at`, `run_completed_at`, `missing_run_reference` |

**Responde:** quais resultados/eventos foram recebidos para uma transação ou run? qual categoria/valor registrou a fonte? qual arquivo originou o evento? há referência à run atual?

**Não responde:** estado completo do run no instante de cada evento (a join usa o **current**); autoria humana da modificação; arquivo externo original PaySettler numa cadeia auditável ponta a ponta; eventos de CDC da própria run no mesmo grain. Em particular, `_ingestion_row_number` não é identificador persistente de evento entre recargas.

```sql
SELECT reconciliation_result_id, run_id, transaction_id, category,
       result_cdc_op, result_event_timestamp, result_source_file,
       run_current_status, missing_run_reference
FROM gold.reconciliation_audit_trail
WHERE transaction_id = '1a9f859c-4830-4946-bb05-e091b270d04e'
ORDER BY result_event_timestamp;
```

**Melhoria de produção:** manter eventos de runs e resultados em trilha temporal versionada por source position, reconstruir run “as-of” por timestamp/ordem confiável e assinar/versionar arquivos recebidos.

## Quality gates e qualidade de origem

Os dados fornecidos apresentaram 808 `MERCHANT_MISMATCH`, 804 `INTERNAL_AMOUNT_MISMATCH` e outras ocorrências (ver `dq.summary`). Essas verificações **comparam a reconciliação ao estado atual** da Silver. Não representam prova de 808 reconciliações erradas na data original. A Gold continua publicando a classificação reportada e o volume interno independente, **sem efetuar joins artificiais para consertar dados**. Não há gate de publicação automático. O dashboard deveria exibir aviso de confiabilidade enquanto critérios de aceite por consumidor não forem acordados.

## Checks recomendados antes da entrega

```sql
-- Grain CFO
SELECT transaction_date, currency, COUNT(*) AS n
FROM gold.daily_financial_volume GROUP BY 1,2 HAVING COUNT(*) > 1;

-- Grain Ops
SELECT reference_date, COUNT(*) AS n
FROM gold.daily_reconciliation_metrics GROUP BY 1 HAVING COUNT(*) > 1;

-- Audit trail: evento Bronze preservado
SELECT
  (SELECT COUNT(*) FROM bronze.reconciliation_results) AS events_bronze,
  (SELECT COUNT(*) FROM gold.reconciliation_audit_trail) AS events_gold;

-- Diagnostics
SELECT * FROM dq.summary ORDER BY issue_count DESC;
```

**Evidências locais informadas:** 30 linhas CFO, 9 Ops, 837 eventos de auditoria; 22 testes passaram. O verificador da CLI não hasheia Gold; `tests/test_gold.py` cobre seus comportamentos separadamente.
