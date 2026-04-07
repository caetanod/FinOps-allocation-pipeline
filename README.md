# FinOps Allocation Pipeline Azure

Pipeline de rateio de custos em Azure, construído em PowerShell, com execução via **Azure Automation Runbooks** e persistência em **Data Lake / Blob Storage**.

---

## 🎯 Objetivo

Transformar custo bruto exportado do Azure em **custo alocado por cliente**, garantindo:

- distribuição precisa por cliente, serviço e recurso
- visibilidade granular do consumo
- suporte à tomada de decisão (FinOps)
- base confiável para dashboards (Power BI)
- rastreabilidade e reconciliação financeira

Este pipeline funciona como um **motor de alocação de custos cloud**, preparado para governança e auditoria.

---

## 🧠 Conceito FinOps

- accountability por cliente
- transparência de custos compartilhados
- separação entre custo dedicado e compartilhado
- regras explícitas de rateio
- reconciliação completa entre custo bruto e alocado

---

## 🧱 Arquitetura do Pipeline

### Camadas

- **Bronze** → ingestão (inventory)
- **Silver** → normalização + mapping + shares
- **Gold** → shares especializadas + consolidação
- **Fact final** → custo alocado + reconciliação

---

## 🔄 Fluxo resumido

1. Inventory → ingestão
2. Fact Cost → normalização
3. Mapping → Resource → ServiceKey
4. Weights → cálculo por cliente
5. Shares → base + especializadas (AKS / SQL)
6. Consolidação
7. Fact Allocated Cost

---

## 📅 Lógica de datas

Separação obrigatória:

- **PipelineDate** → data de execução
- **UsageDate** → data real do consumo

✔ Nunca assumir D-1 fixo  
✔ Sempre derivar UsageDate do conteúdo

---

## ⚠️ Pontos críticos

- Qualidade do mapping (`resource_to_service`)
- Separação de datas (Usage vs Pipeline)
- Tratamento de serviços compartilhados
- Reconciliação final obrigatória

---

## 🚀 Evolução para V1

- reconciliação financeira exata (centavos)
- redução de unallocated
- auditoria automática
- versionamento de regras

---

## 🧠 Roadmap

- dashboards Power BI
- forecast de custos
- simulação de cenários
- chargeback automatizado
- multi-cloud support

---

## 📁 Estrutura do repositório

```text
finops-allocation-pipeline/
├── README.md
├── ARCHITECTURE.md
├── TROUBLESHOOTING.md
├── ROADMAP.md
├── scripts/
├── docs/
└── config/


![Azure](https://img.shields.io/badge/Azure-FinOps-blue)
![PowerShell](https://img.shields.io/badge/PowerShell-Automation-darkblue)
![Status](https://img.shields.io/badge/status-production-green)