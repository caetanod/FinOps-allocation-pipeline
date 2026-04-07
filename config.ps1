# =========================
# CONFIG - SEU AMBIENTE
# =========================

# Storage Account
$StorageAccountName = "stpslkmmfinopseusprd"

# Container lake (bronze/silver/gold)
$FinopsContainer = "finops"

# Cost Export (container separado)
$CostContainer = "finops-cost-exports"
$CostPrefix    = "kmm/finops-cost-export/"   # <- AJUSTADO CONFORME SUA ESTRUTURA

# Prefixos internos no container finops
$InventoryPrefix       = "bronze/inventory_daily"
$FactCostPrefix        = "silver/fact_cost"
$AllocationSharePrefix = "gold/fact_allocation_share"
$ResourceToServicePrefix = "silver/resource_to_service"

# Pasta temporária local
$FinopsTempFolder = Join-Path $env:TEMP "finops_rateio"

# =========================
# FUNÇÕES COMUNS
# =========================

function Ensure-AzLogin {
  try { $ctx = Get-AzContext -ErrorAction Stop } catch { $ctx = $null }
  if (-not $ctx -or -not $ctx.Account) {
    Write-Host "🔐 Conectando no Azure..."
    Connect-AzAccount | Out-Null
  }
}

function Get-StorageContextByName {
  param([Parameter(Mandatory=$true)][string]$saName)

  $sa = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $saName } | Select-Object -First 1
  if (-not $sa) { throw "Storage Account '$saName' não encontrado nas subscriptions do seu contexto." }
  return $sa.Context
}

function Ensure-Folder {
  param([Parameter(Mandatory=$true)][string]$Path)
  if (-not (Test-Path $Path)) { New-Item -ItemType Directory -Path $Path | Out-Null }
}

function Upload-ToBlob {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$LocalPath,
    [Parameter(Mandatory=$true)][string]$BlobPath
  )
  if (-not (Test-Path $LocalPath)) { throw "Arquivo não encontrado: $LocalPath" }

  Write-Host "⬆️  Upload: $Container/$BlobPath"
  Set-AzStorageBlobContent -Context $Ctx -Container $Container -File $LocalPath -Blob $BlobPath -Force | Out-Null
}

function Download-Blob {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$BlobName,
    [Parameter(Mandatory=$true)][string]$OutFolder
  )
  Ensure-Folder -Path $OutFolder

  $outPath = Join-Path $OutFolder (Split-Path $BlobName -Leaf)
  Write-Host "⬇️  Download: $Container/$BlobName -> $outPath"

  Get-AzStorageBlobContent -Context $Ctx -Container $Container -Blob $BlobName -Destination $outPath -Force | Out-Null
  return $outPath
}

function Get-LatestBlobByPrefixAndExtension {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$Prefix,
    [Parameter(Mandatory=$true)][string]$ExtensionLike # ex: "*.csv"
  )

  Write-Host "🔎 Listando blobs em '$Container' prefix '$Prefix'..."
  $blobs = Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $Prefix |
           Where-Object { $_.Name -like $ExtensionLike }

  if (-not $blobs -or $blobs.Count -eq 0) {
    throw "Nenhum blob encontrado em '$Container' com prefixo '$Prefix' e filtro '$ExtensionLike'."
  }

  return ($blobs | Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending | Select-Object -First 1)
}