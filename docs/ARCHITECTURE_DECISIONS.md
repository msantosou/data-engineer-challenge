# Decisões arquiteturais — Settlement Analytics

**Status:** implementação local validada; decisões registradas após profiling e 22 testes aprovados em 12/10/2026. Este documento descreve o código implementado, não uma arquitetura de produção já existente.

## Objetivo e limites

Produzir indicadores diários de reconciliação (Operações), volume interno de transações (CFO) e trilha de resultados de reconciliação (Compliance), mantendo rastreabilidade de origens sintéticas inconsistentes. Pipeline executado por Docker Compose/Python/DuckDB, sem banco transacional PostgreSQL local.

## Decisões (ADR resumidos)

| ID | Decisão | Por quê | Consequência / trade-off |
|---|---|---|---|
| ADR-01 | DuckDB embedded com arquivo bind-mounted | Executável local reproduzível, SQL sobre Parquet/CSV, stack obrigatória | Writer exclusivo; sem escalabilidade horizontal automática |
| ADR-02 | Bronze com full refresh transacional e `_raw_payload` | Reprocessamento fácil, normalização sem perder entrada | Não append-only físico; timestamps técnicos mudam entre runs |
| ADR-03 | Silver current por chave de negócio e maior `_timestamp` | Evita somar múltiplas versões CDC | Timestamp não é ordem total; empates heterogêneos em quarentena |
| ADR-04 | `D` tratado após identificar o evento mais recente | Evita ressuscitar chaves excluídas | Exclusões desaparecem da current, permanecem na Bronze |
| ADR-05 | PaySettler sem CDC inferido | CSV de liquidação não traz `Op`/`_timestamp` | Eventos externos não são materializados como estado atual |
| ADR-06 | DQ é diagnóstica, não transformadora | Não esconder inconsistências do fixture | Gold pode ser publicada mesmo com severidade HIGH; explicitar ressalva |
| ADR-07 | Gold CFO sem join a reconciliação | Evita propagar inconsistências interfontes no valor transacionado | Não calcula settled volume ou reconciliação independente |
| ADR-08 | Gold Ops: última run `COMPLETED` por data | Série diária não deve multiplicar reprocessamentos | Heurística por `completed_at`, `started_at`, `id`, não semântica autoritativa de substituição |
| ADR-09 | Auditoria via eventos Bronze de resultados | Não descartar versões CDC relevantes à investigação | Enriquecimento de runs está em estado atual, não "as-of" |
| ADR-10 | Reprocessamento idempotente por substituição | Sem chave incremental/offset no fixture | Não prova idempotência incremental nem atomicidade do fluxo inteiro entre scripts |

## Chaves e granularidades

- `bronze.transactions`: um **evento fonte** por linha, não uma transação única.
- `silver.transactions_current`: **`transaction_id` único**, excluídos `D` e conflitos de último timestamp.
- `silver.reconciliation_runs_current`: **`id` único**.
- `silver.reconciliation_results_current`: **`id` único**; `transaction_id` pode repetir entre execuções.
- `silver.enterprise_company_current`: **`id` único**; não presumir `merchant_id` único sem teste.
- `silver.cdc_conflicts`: um ou mais eventos por chave ambígua, com origem e payload.
- `silver.paysettler_events`: evento do CSV, sem regra inferida de dedup/upsert.
- `dq.issues`: ocorrência de regra em uma entidade; pode haver múltiplas regras por transação.

## Dados originais — descoberta e política

No fixture utilizado, a camada DQ retornou:

| Regra | Ocorrências |
|---|---:|
| `MERCHANT_MISMATCH` | 808 |
| `INTERNAL_AMOUNT_MISMATCH` | 804 |
| `TRANSACTION_OUTSIDE_WINDOW` | 601 |
| `RECON_DIFFERENCE_INCORRECT` | 5 |
| `RECON_RUN_MISSING` | 5 |
| `TRANSACTION_MISSING` | 5 |
| `CDC_AMBIGUOUS_LATEST` | 4 |

Esses números **não representam percentuais auditados de reconciliações incorretas**. O comparador usa estado atual, não snapshot das transações à época do run; os conjuntos de problemas se sobrepõem. O teste controlado de 10.000 registros do gerador com `--clean` deu 9.479 correspondências completas e 521 ausências internas, sem divergência de merchant/amount entre os registros encontrados; não foi demonstrado qual processo criou o fixture original.

**Política implementada:** preservar entrada e classificador original; emitir issues e quarentenar somente chaves de CDC indecidível. **Política de produção recomendada:** definir controles específicos por consumidor: falhas de schema/chave CDC impedem current; divergência referencial financeira elevada bloqueia publicação confiável para CFO e dispara incidentes; erros pontuais ficam em quarantine + relatório. Atualmente esses gates de publicação **não estão automatizados**.

## Atomicidade e falhas

Cada estágio atualiza suas próprias tabelas dentro de transações DuckDB, mas **não há transação única envolvendo todos os quatro scripts**. Se Gold falhar após Bronze/Silver/DQ concluírem, a Gold anterior pode permanecer. Produção precisaria de versionamento por `batch_id`/`run_id` e publicação de ponteiro/versão apenas ao final (commit lógico) para impedir que consumidores misturem gerações.

## Idempotência — o que foi demonstrado

- `pytest`: 22/22 aprovados.
- CLI `--verify-idempotency`: duas execuções completas sem alteração lógica detectada pelo `snapshot()`.
- **Precisão:** `snapshot()` compara hash de Bronze, Silver e `dq.issues`, ignorando colunas técnicas voláteis; compara somente contagem de `silver.paysettler_events`. **Gold não está no hash desse CLI**. Os testes de Gold avaliam idempotência da materialização local, separadamente.
- Não demonstra ingestão incremental, reversão em caso de falha após múltiplas etapas ou equivalência histórica point-in-time.

## Extensibilidade — segundo processador

O projeto **não possui registro declarativo de processadores**. Em sua forma atual, tocar `bronze_ingest.py` (localização e mapeamento), `silver_cdc.py` (view de eventos), `run_pipeline.py` (snapshot), `data_quality.py` (regras), testes e documentação. Alterar `gold.py` somente se consumidores precisarem de uma métrica cruzando processadores. O contrato futuro ideal exige um campo `processor_name` e `source_transaction_id`, com composição explícita da chave (`processor_name`, `processor_reference` ou transação, conforme a fonte).

Uma forma sustentável seria adicionar `config/processors.yml`, leitores por adaptador e um modelo canônico comum, com testes de contrato por processador. Isso é **plano**, não implementação.

## Escalabilidade

O case cita ~5M transações/mês para dimensionamento inicial e cenário futuro de ~5M/dia. Esta implementação não foi benchmarkada nessas cargas. Primeiros gargalos prováveis: reread total dos Parquets, janelas CDC globais, custo de reescrita do arquivo DuckDB, materialização do audit trail e cálculo `ORDER BY ALL` no snapshot. Para produção: ingestion CDC incremental com posição confirmada, armazenamento particionado, processamento por referência, teste de carga com percentis e planejamento de memória/disco; se ultrapassar o perfil de um único nó, trocar execução local por plataforma distribuída.

## Troubleshooting sugerido

1. Verificar último arquivo/`reference_date` disponível e timestamps da fonte.
2. Consultar `docker compose logs pipeline`, código de saída e último estágio concluído.
3. Comparar contagens por `_source_file` na Bronze com arquivo fornecido.
4. Inspecionar `silver.cdc_conflicts`, regras de CDC e contagens atuais.
5. Consultar `dq.summary`, últimas runs COMPLETED e datas da Gold.
6. Se falha de origem: acionar proprietário da fonte; se falha DuckDB/volume/lock: runtime/plataforma; se mudança de regras/categories: time de reconciliação.

## Pendências pré-produção prioritárias

- Snapshot versionado por processamento e commit lógico entre camadas.
- Sequência CDC autoritativa (LSN ou equivalente) e replay incremental.
- Contratos e quality gates por processador, com auditoria completa as-of e RBAC.
