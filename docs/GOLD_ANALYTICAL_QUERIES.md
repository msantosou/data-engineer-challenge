# Consultas analíticas — Gold e Data Quality

Consultas para executar no **DuckDB UI**, utilizando as tabelas materializadas pelo pipeline do desafio Marvin. Os nomes das colunas seguem os contratos utilizados nas consultas de desenvolvimento; caso o schema tenha sido alterado, confira com `DESCRIBE gold.<tabela>`.

## 1. CFO — Volume financeiro

Fonte: `gold.daily_financial_volume`. Valores de moedas diferentes **não devem ser somados**. O produto utiliza o estado atual de transações `COMPLETED`, não um livro-razão histórico.

### 1.1 Volume transacionado por dia

```sql
SELECT
    transaction_date,
    currency,
    transaction_count,
    gross_amount,
    avg_ticket
FROM gold.daily_financial_volume
ORDER BY transaction_date DESC, currency;
```

### 1.2 Volume mensal e ticket médio ponderado

```sql
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    currency,
    SUM(transaction_count) AS total_transactions,
    SUM(gross_amount) AS total_amount,
    ROUND(SUM(gross_amount) / NULLIF(SUM(transaction_count), 0), 2) AS avg_ticket
FROM gold.daily_financial_volume
GROUP BY 1, 2
ORDER BY month, currency;
```

### 1.3 Crescimento diário do volume

```sql
WITH daily AS (
    SELECT
        transaction_date,
        currency,
        gross_amount,
        LAG(gross_amount) OVER (
            PARTITION BY currency ORDER BY transaction_date
        ) AS previous_amount
    FROM gold.daily_financial_volume
)
SELECT
    transaction_date,
    currency,
    gross_amount,
    previous_amount,
    ROUND(100.0 * (gross_amount - previous_amount) / NULLIF(previous_amount, 0), 2) AS growth_pct
FROM daily
ORDER BY transaction_date DESC, currency;
```

> `LAG()` compara com o registro anterior **disponível**, não necessariamente o dia anterior do calendário.

## 2. Operações — Saúde da reconciliação

Fonte: `gold.daily_reconciliation_metrics`. Métricas refletem as classificações **reportadas pelo serviço de reconciliação**; não constituem validação independente dos registros internos contra o PaySettler.

### 2.1 Taxa diária de discrepância

```sql
SELECT
    reference_date,
    total_results,
    matched_count,
    mismatched_count,
    mismatch_rate_pct,
    matched_rate_pct
FROM gold.daily_reconciliation_metrics
ORDER BY reference_date;
```

### 2.2 Tendência da taxa de discrepância

```sql
WITH trend AS (
    SELECT
        reference_date,
        mismatch_rate_pct,
        LAG(mismatch_rate_pct) OVER (ORDER BY reference_date) AS previous_rate
    FROM gold.daily_reconciliation_metrics
)
SELECT
    reference_date,
    mismatch_rate_pct,
    previous_rate,
    ROUND(mismatch_rate_pct - previous_rate, 2) AS variation_pp,
    CASE
        WHEN previous_rate IS NULL THEN 'NO_HISTORY'
        WHEN mismatch_rate_pct > previous_rate THEN 'INCREASING'
        WHEN mismatch_rate_pct < previous_rate THEN 'DECREASING'
        ELSE 'STABLE'
    END AS trend
FROM trend
ORDER BY reference_date;
```

> `variation_pp` significa variação em **pontos percentuais**.

### 2.3 Transações não conciliadas

```sql
SELECT
    reference_date,
    total_results,
    unreconciled_internal_count,
    unreconciled_processor_count,
    unreconciled_rate_pct
FROM gold.daily_reconciliation_metrics
ORDER BY reference_date;
```

### 2.4 Impacto financeiro absoluto das discrepâncias

```sql
SELECT
    reference_date,
    mismatched_count,
    mismatched_difference_amount,
    ROUND(mismatched_difference_amount / NULLIF(mismatched_count, 0), 2) AS avg_difference
FROM gold.daily_reconciliation_metrics
ORDER BY reference_date;
```

> O somatório absoluto das diferenças **não é equivalente a prejuízo financeiro líquido**.

### 2.5 Taxa agregada nos últimos sete dias disponíveis de calendário

```sql
WITH last_7_days AS (
    SELECT *
    FROM gold.daily_reconciliation_metrics
    WHERE reference_date BETWEEN
        (SELECT MAX(reference_date) FROM gold.daily_reconciliation_metrics) - INTERVAL '6 days'
        AND (SELECT MAX(reference_date) FROM gold.daily_reconciliation_metrics)
)
SELECT
    SUM(total_results) AS total_reconciliations,
    SUM(mismatched_count) AS total_mismatches,
    ROUND(100.0 * SUM(mismatched_count) / NULLIF(SUM(total_results), 0), 2) AS mismatch_rate_7d
FROM last_7_days;
```

> A taxa é ponderada pelo total de resultados, e não uma média simples das taxas diárias. Datas sem execução não entram no denominador.

## 3. Compliance — Auditoria

Fonte: `gold.reconciliation_audit_trail`. A trilha conserva eventos dos resultados de reconciliação, mas o contexto da execução pode representar seu estado atual; **não é uma reconstrução histórica completa de todas as fontes**.

### 3.1 Histórico de uma transação

```sql
SELECT
    transaction_id,
    merchant_id,
    run_id,
    reference_date,
    category,
    internal_amount,
    processor_amount,
    difference,
    result_cdc_op,
    result_event_timestamp
FROM gold.reconciliation_audit_trail
WHERE transaction_id = '1a9f859c-4830-4946-bb05-e091b270d04e'
ORDER BY result_event_timestamp;
```

### 3.2 Histórico dos resultados de uma execução

```sql
SELECT
    run_id,
    run_file_name,
    transaction_id,
    merchant_id,
    category,
    internal_amount,
    processor_amount,
    difference,
    reconciliation_created_at,
    result_source_file
FROM gold.reconciliation_audit_trail
WHERE run_id = 1
ORDER BY transaction_id;
```

### 3.3 Registros classificados com discrepância

```sql
SELECT
    transaction_id,
    merchant_id,
    reference_date,
    category,
    internal_amount,
    processor_amount,
    difference,
    run_id
FROM gold.reconciliation_audit_trail
WHERE category = 'MISMATCHED'
ORDER BY difference DESC;
```

### 3.4 Transações com múltiplas classificações registradas

```sql
SELECT
    transaction_id,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(DISTINCT run_id) AS total_runs,
    STRING_AGG(DISTINCT category, ', ') AS categories
FROM gold.reconciliation_audit_trail
GROUP BY transaction_id
HAVING COUNT(DISTINCT category) > 1
ORDER BY total_runs DESC;
```

> Classificações diferentes não comprovam, isoladamente, que uma classificação anterior foi corrigida.

### 3.5 Resultados sem execução de referência

```sql
SELECT
    reconciliation_result_id,
    transaction_id,
    run_id,
    category,
    result_source_file
FROM gold.reconciliation_audit_trail
WHERE missing_run_reference = TRUE;
```

### 3.6 Arquivos e operações CDC de origem

```sql
SELECT
    result_source_file,
    result_cdc_op,
    COUNT(*) AS total_events,
    MIN(result_event_timestamp) AS first_event,
    MAX(result_event_timestamp) AS last_event
FROM gold.reconciliation_audit_trail
GROUP BY result_source_file, result_cdc_op
ORDER BY result_source_file, result_cdc_op;
```

## 4. Data Quality — Consultas complementares

A camada DQ registra problemas sem alterar silenciosamente os dados das camadas anteriores. Uma mesma transação pode produzir múltiplas ocorrências; **não some as regras para estimar a quantidade de transações únicas afetadas**.

### 4.1 Resumo das inconsistências

```sql
SELECT *
FROM dq.summary
ORDER BY issue_count DESC;
```

### 4.2 Ocorrências por severidade

```sql
SELECT
    severity,
    COUNT(*) AS total_issues
FROM dq.issues
GROUP BY severity
ORDER BY total_issues DESC;
```

### 4.3 Investigação de divergências financeiras

```sql
SELECT *
FROM dq.issues
WHERE rule_id = 'INTERNAL_AMOUNT_MISMATCH'
LIMIT 50;
```

## Notas de interpretação

- **CFO:** o volume financeiro é apurado com transações `COMPLETED` no estado atual. A abordagem não representa necessariamente o volume reconhecido historicamente na data de fechamento.
- **Operações:** as métricas utilizam categorias do serviço de reconciliação; diferenças de qualidade entre fontes são monitoradas separadamente em `dq`.
- **Compliance:** preserve sempre os eventos Bronze quando for necessário investigar uma mudança histórica, inclusive versões de `reconciliation_runs`.
- **CDC:** registros ambíguos isolados na Silver não entram nos produtos derivados do estado atual; consulte `silver.cdc_conflicts` para investigar exclusões de cobertura.
