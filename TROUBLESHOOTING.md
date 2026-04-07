# Troubleshooting – FinOps Pipeline

## Erro: Nenhuma partição encontrada

Causa:
- datas desalinhadas

Solução:
- validar UsageDate
- verificar inventory e fact_cost

---

## Erro: TryParse overload

Causa:
- uso incorreto de assinatura

Solução:
- usar TryParse com parâmetros corretos

---

## Erro: unallocated alto

Causa:
- falha no mapping

Solução:
- revisar script 04
- revisar overrides RG

---

## Erro: AKS não alocado

Causa:
- falha OpenCost ou alias

Solução:
- validar arquivos:
  - opencost_clusters
  - clientes_alias