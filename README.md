# FinOps Allocation Pipeline Azure

Pipeline de rateio de custos em Azure, construído em PowerShell, com execução via **Azure Automation Runbooks** e persistência dos artefatos em **Azure Data Lake / Blob Storage**.

## Objetivo

Transformar custo bruto exportado do Azure em **custo alocado por cliente**, usando uma combinação de:

- inventário diário de recursos
- classificação de recursos compartilhados por `ServiceKey`
- pesos de rateio por custo dedicado
- trilhas especializadas para **SQL Pool** e **AKS**
- consolidação final da alocação
- reconciliação de itens alocados e não alocados

---

## Visão geral do pipeline

O pipeline segue uma lógica em camadas:

- **Bronze**: ingestão do inventário
- **Silver**: normalização de custo, mapeamentos e shares genéricas
- **Gold**: shares especializadas e consolidação final
- **Fact final**: custo alocado por cliente + reconciliação

### Fluxo resumido

1. **01_upload_inventory.ps1**  
   Publica o inventário diário de recursos no lake.

2. **02_build_fact_cost.ps1**  
   Constrói a `fact_cost` a partir do Azure Cost Export, escolhendo o CSV correto pelo **conteúdo** e não apenas por `LastModified`.

3. **03_build_overrides_rg_auto.ps1**  
   Gera sugestões automáticas de override por Resource Group para recursos compartilhados.

4. **04_build_resource_to_service.ps1**  
   Mapeia `ResourceId -> ServiceKey`, etapa central de classificação dos recursos compartilhados.

5. **05_build_client_weights_by_dedicated_cost.ps1**  
   Calcula o peso de cada cliente com base no custo dedicado.

6. **06_build_allocation_share_servicekey_runbook.ps1**  
   Gera a share genérica por `ServiceKey` para tudo que não exige tratamento especializado.

7. **07_build_sqlpool_allocation_share_from_metrics.ps1**  
   Gera share especializada para **SQL Pool**, usando métricas técnicas do ambiente.

8. **08_build_aks_allocation_from_opencost.ps1**  
   Gera share especializada para **AKS**, usando OpenCost, aliases de clientes e tratamento de namespaces de sistema.

9. **09_build_allocation_share_servicekey_final.ps1**  
   Consolida a share genérica com as shares especializadas de SQL Pool e AKS.

10. **10_build_fact_allocated_cost.ps1**  
    Aplica a share final sobre o custo real e gera a tabela final de custo alocado, reconciliação e itens não alocados.

11. **97_run_finops_allocation_pipeline_runbooK.ps1**  
    Runbook maestro que orquestra a execução de todas as etapas acima no Azure Automation.

---

## Lógica de datas

Um dos pontos mais importantes deste projeto é a separação entre:

- **PipelineDate**: data da execução/publicação da partição
- **UsageDate**: data real de competência do custo encontrada no conteúdo do arquivo

### Regra principal

O pipeline **não deve assumir D-1 fixo**.  
A data correta do custo deve ser derivada do conteúdo do export (`UsageDate`, `Date` ou `UsageDateTime`), enquanto a partição do pipeline representa a data da execução/publicação.

Isso evita:

- quebra do pipeline quando o export chega fora da janela esperada
- alocação em data incorreta
- inconsistência entre partição do lake e competência real do custo

---

## Estrutura lógica de dados

### Entradas principais

- Inventário diário de recursos
- Azure Cost Export
- Arquivos de referência de configuração
- Métricas especializadas de SQL Pool
- Métricas de AKS via OpenCost

### Prefixos relevantes

```text
bronze/inventory_daily
silver/fact_cost
silver/resource_to_service
silver/client_weights_by_dedicated_cost
silver/allocation_share_servicekey
gold/sqlpool_allocation_share_metrics
gold/aks_allocation_share_opencost
gold/allocation_share_servicekey_final
```

### Arquivos de referência usados no AKS

```text
config/reference/opencost_clusters.csv
config/reference/clientes_alias.csv
config/reference/aks_system_namespaces.csv
```

---

## Scripts e funções

| Script | Função principal | Saída principal |
|---|---|---|
| `01_upload_inventory.ps1` | Ingestão do inventário bruto | `bronze/inventory_daily` |
| `02_build_fact_cost.ps1` | Normalização do custo exportado | `silver/fact_cost` |
| `03_build_overrides_rg_auto.ps1` | Sugestão de overrides por RG | CSVs de override/candidatos |
| `04_build_resource_to_service.ps1` | Mapeamento `ResourceId -> ServiceKey` | `silver/resource_to_service` |
| `05_build_client_weights_by_dedicated_cost.ps1` | Peso proporcional por cliente | `silver/client_weights_by_dedicated_cost` |
| `06_build_allocation_share_servicekey_runbook.ps1` | Share genérica por `ServiceKey` | `silver/allocation_share_servicekey` |
| `07_build_sqlpool_allocation_share_from_metrics.ps1` | Share especializada de SQL Pool | `gold/sqlpool_allocation_share_metrics` |
| `08_build_aks_allocation_from_opencost.ps1` | Share especializada de AKS | `gold/aks_allocation_share_opencost` |
| `09_build_allocation_share_servicekey_final.ps1` | Consolidação das shares | `gold/allocation_share_servicekey_final` |
| `10_build_fact_allocated_cost.ps1` | Fato final alocado + reconciliação | outputs finais de alocação |
| `99_run_finops_allocation_pipeline_runbooK.ps1` | Orquestração do pipeline | execução ponta a ponta |

---

## Dependências operacionais

### Azure

- Azure Automation Account
- Managed Identity com permissões adequadas
- Storage Account / Data Lake com acesso aos containers usados
- Cost Export publicado regularmente
- Hybrid Worker para a etapa AKS, quando necessário

### PowerShell / módulos

Em ambiente local ou automação, valide a presença dos módulos usados pelo projeto, principalmente:

- `Az.Accounts`
- `Az.Storage`
- `Az.Automation`

---

## Ordem de execução no Azure Automation

O runbook maestro chama os runbooks nesta sequência:

1. `finops-rateio-01-build-inventory`
2. `finops-rateio-02-build-fact-cost`
3. `finops-rateio-03-build-overrides-rg-auto`
4. `finops-rateio-04-build-resource-to-service`
5. `finops-rateio-05-build-client-weights`
6. `finops-rateio-06-build-allocation-share-servicekey`
7. `finops-rateio-07-build-sqlpool-allocation-share-from-metrics`
8. `finops-rateio-08-build-aks-allocation-from-opencost`
9. `finops-rateio-09_build_allocation_share_servicekey_final_runbook`
10. `finops-rateio-10_build_fact_allocated_cost_runbook`

> Observação: a etapa de AKS pode exigir execução em **Hybrid Worker**.

---

## Exemplo de estrutura do repositório

```text
finops-allocation-pipeline/
├── README.md
├── .gitignore
├── docs/
│   ├── pipeline_diagram.drawio
│   └── pipeline_diagram.mmd
├── scripts/
│   ├── 01_upload_inventory.ps1
│   ├── 02_build_fact_cost.ps1
│   ├── 03_build_overrides_rg_auto.ps1
│   ├── 04_build_resource_to_service.ps1
│   ├── 05_build_client_weights_by_dedicated_cost.ps1
│   ├── 06_build_allocation_share_servicekey_runbook.ps1
│   ├── 07_build_sqlpool_allocation_share_from_metrics.ps1
│   ├── 08_build_aks_allocation_from_opencost.ps1
│   ├── 09_build_allocation_share_servicekey_final.ps1
│   ├── 10_build_fact_allocated_cost.ps1
│   └── 97_run_finops_allocation_pipeline_runbooK.ps1
└── config/
    └── reference/
```

---

## Como subir para o GitHub

### 1. Criar a pasta do repositório

```bash
mkdir finops-allocation-pipeline
cd finops-allocation-pipeline
mkdir scripts docs config
```

### 2. Copiar os arquivos

- copie os `.ps1` para `scripts/`
- copie o `README.md` para a raiz
- copie o diagrama para `docs/`

### 3. Inicializar Git

```bash
git init
git branch -M main
```

### 4. Adicionar um `.gitignore`

Exemplo mínimo:

```gitignore
*.csv
*.log
*.tmp
*.bak
/.idea
/.vscode
```

### 5. Commit inicial

```bash
git add .
git commit -m "Initial commit - FinOps allocation pipeline"
```

### 6. Criar o repositório no GitHub

Crie um repositório vazio no GitHub, por exemplo:

```text
finops-allocation-pipeline-azure
```

### 7. Vincular o remoto e subir

```bash
git remote add origin https://github.com/SEU_USUARIO/finops-allocation-pipeline-azure.git
git push -u origin main
```

---

## Recomendações de documentação

Vale manter no repositório:

- um `README.md` executivo
- um documento técnico de troubleshooting por script
- um diagrama atualizado do fluxo
- exemplos de parâmetros de execução
- descrição clara das entradas e saídas por etapa

---

## Pontos críticos do projeto

### 1. Separação entre data de partição e data de uso
É a base para não distorcer o rateio.

### 2. Qualidade do `resource_to_service`
Esse mapeamento impacta diretamente a classificação de custo compartilhado.

### 3. Trilhas especializadas
AKS e SQL Pool não devem cair no rateio genérico quando houver trilha dedicada.

### 4. Reconciliação
A última etapa precisa evidenciar:
- custo alocado
- custo não alocado
- motivo do não alocado
- aderência entre bruto e alocado

---

## Melhorias futuras sugeridas

- versionar outputs de referência
- adicionar testes de validação por etapa
- publicar métricas de execução do pipeline
- gerar diagrama de lineage de dados
- criar documentação por script em arquivos separados
- incluir exemplos de entrada e saída anonimizados

---


