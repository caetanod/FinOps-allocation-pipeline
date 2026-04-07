<#
Objetivo:
Gerar automaticamente sugestões de override por Resource Group para recursos compartilhados do inventário.

Função no pipeline:
Apoia a classificação de recursos compartilhados quando o RG por si só já permite inferir uma ServiceKey ou tipo de serviço.

Entrada:
- Inventário publicado no Data Lake
- Recursos com FINOPS-CLIENTE = COMPARTILHADO

Saída:
- CSV de overrides automáticos por RG
- CSV de candidatos com confiança sugerida

Padrão operacional aplicado:
- Não usa D-1 fixo
- Trata timezone Brasil no Azure Automation
- Quando a data não é informada, procura partição disponível em hoje, D-1 e D-2
- Usa a MESMA partição resolvida para leitura e escrita

Exemplos:
  .\03_build_overrides_rg_auto_enterprise_final.ps1
  .\03_build_overrides_rg_auto_enterprise_final.ps1 -PipelineDate "2026-04-01"
  .\03_build_overrides_rg_auto_enterprise_final.ps1 -UseLatestInventoryFromLake $false -InventoryCsvPath "C:\Temp\finops\inventory_2026-04-01.csv"
#>

param(
    [Parameter(Mandatory = $false)]
    [string]$Date = "",

    [Parameter(Mandatory = $false)]
    [string]$PipelineDate = "",

    [Parameter(Mandatory = $false)]
    [bool]$UseLatestInventoryFromLake = $true,

    [Parameter(Mandatory = $false)]
    [string]$InventoryCsvPath = "",

    [Parameter(Mandatory = $false)]
    [string]$StorageAccountName = "stpslkmmfinopseusprd",

    [Parameter(Mandatory = $false)]
    [string]$FinopsContainer = "finops",

    [Parameter(Mandatory = $false)]
    [string]$InventoryPrefix = "bronze/inventory_daily",

    [Parameter(Mandatory = $false)]
    [string]$OverridesPrefix = "silver/overrides_rg",

    [Parameter(Mandatory = $false)]
    [string]$TempFolder = "C:\Temp\finops"
)

# ==========================================
# CONFIG
# ==========================================
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

$TargetSubscriptions = @(
    "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
    "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

# ==========================================
# FUNÇÕES BASE
# ==========================================
function Ensure-Folder {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path $Path)) {
        New-Item -Path $Path -ItemType Directory -Force | Out-Null
    }
}

function Login-Azure {
    Write-Host "🔐 Conectando com Managed Identity..."
    Disable-AzContextAutosave -Scope Process | Out-Null
    Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null
}

function Get-BrazilNow {
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("E. South America Standard Time")
    }
    catch {
        try {
            $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("America/Sao_Paulo")
        }
        catch {
            throw "Não foi possível localizar o timezone do Brasil. Detalhe: $($_.Exception.Message)"
        }
    }

    return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

function Get-StorageContext {
    param(
        [Parameter(Mandatory = $true)]
        [string]$StorageAccountName
    )

    foreach ($subId in $TargetSubscriptions) {
        try {
            Select-AzSubscription -SubscriptionId $subId | Out-Null

            $sa = Get-AzStorageAccount -ErrorAction SilentlyContinue |
                Where-Object { $_.StorageAccountName -eq $StorageAccountName } |
                Select-Object -First 1

            if ($sa) {
                Write-Host "✅ Storage encontrado na subscription: $subId"
                return $sa.Context
            }
        }
        catch {
            Write-Warning "Erro ao buscar storage em $subId. Detalhe: $($_.Exception.Message)"
        }
    }

    throw "Storage Account '$StorageAccountName' não encontrado nas subscriptions informadas."
}

function Upload-Blob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$File,
        [Parameter(Mandatory = $true)][string]$Blob
    )

    if (-not (Test-Path $File)) {
        throw "Arquivo local não encontrado para upload: $File"
    }

    Write-Host "⬆️ Iniciando upload..."
    Write-Host "   Container : $Container"
    Write-Host "   Blob      : $Blob"
    Write-Host "   Arquivo   : $File"

    Set-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -File $File `
        -Blob $Blob `
        -Force `
        -ErrorAction Stop | Out-Null

    Write-Host "✅ Upload concluído com sucesso."
}

function Download-Blob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$BlobName,
        [Parameter(Mandatory = $true)][string]$OutFolder
    )

    Ensure-Folder -Path $OutFolder

    $outPath = Join-Path $OutFolder (Split-Path $BlobName -Leaf)
    Write-Host "⬇️ Download: $Container/$BlobName -> $outPath"

    Get-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -Blob $BlobName `
        -Destination $outPath `
        -Force `
        -ErrorAction Stop | Out-Null

    return $outPath
}

function Get-AvailablePartitionDate {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$BasePrefix,
        [Parameter(Mandatory = $false)][string]$RequestedDate = "",
        [Parameter(Mandatory = $false)][int]$LookbackDays = 2
    )

    if (-not [string]::IsNullOrWhiteSpace($RequestedDate)) {
        $prefix = "$BasePrefix/dt=$RequestedDate/"
        $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop)

        if ($blobs.Count -gt 0) {
            Write-Host "✅ Partição solicitada encontrada: $RequestedDate"
            return $RequestedDate
        }

        throw "A partição solicitada não existe: '$Container/$prefix'"
    }

    $nowLocal = Get-BrazilNow
    $datesToTry = @()

    for ($i = 0; $i -le $LookbackDays; $i++) {
        $datesToTry += $nowLocal.AddDays(-$i).ToString("yyyy-MM-dd")
    }

    Write-Host "🔎 Procurando partição disponível em horário do Brasil: $($datesToTry -join ', ')"

    foreach ($dt in $datesToTry) {
        $prefix = "$BasePrefix/dt=$dt/"
        $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop)

        if ($blobs.Count -gt 0) {
            Write-Host "✅ Usando partição disponível: $dt"
            return $dt
        }
    }

    throw "Nenhuma partição disponível encontrada para '$BasePrefix' (nem hoje, nem D-1, nem D-2 no horário do Brasil)."
}

function Download-BlobByResolvedDatePrefix {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$BasePrefix,
        [Parameter(Mandatory = $true)][string]$Dt,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$OutFolder
    )

    $prefix = "$BasePrefix/dt=$Dt/"
    Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

    $blobs = @(
        Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop |
        Where-Object { $_.Name -like "*.csv" }
    )

    if ($blobs.Count -eq 0) {
        throw "Nenhum CSV encontrado para '$Label' em '$Container/$prefix'."
    }

    $blob = $blobs |
        Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending |
        Select-Object -First 1

    Write-Host "📌 Blob encontrado ($Label): $($blob.Name)"

    return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $OutFolder)
}

# ==========================================
# FUNÇÕES DE NEGÓCIO
# ==========================================
function Normalize-Text {
    param([string]$s)

    if ([string]::IsNullOrWhiteSpace($s)) { return "" }
    return ($s + "").Trim([char]0xFEFF).Trim()
}

function Get-AksClusterFromMcRg {
    param([string]$rg)

    if ([string]::IsNullOrWhiteSpace($rg)) { return $null }
    if (-not $rg.StartsWith("MC_", [System.StringComparison]::OrdinalIgnoreCase)) { return $null }

    $rest = $rg.Substring(3)
    $parts = $rest.Split("_")

    if ($parts.Count -ge 3) {
        return $parts[$parts.Count - 2]
    }

    return $null
}

function Canonicalize-InfraKeyFromRg {
    param([string]$rg)

    if ([string]::IsNullOrWhiteSpace($rg)) { return "INFRA:SHARED" }

    $x = $rg.Trim()
    $x = $x -replace '^(?i)rg[-_]', ''
    $x = $x -replace '^(?i)rg', ''
    $x = $x -replace '_', '-'
    $x = $x -replace '\s+', '-'

    while ($x -match '--') {
        $x = $x -replace '--', '-'
    }

    $x = $x.Trim('-')

    if ([string]::IsNullOrWhiteSpace($x)) {
        $x = "SHARED"
    }

    return ("INFRA:{0}" -f $x.ToUpperInvariant())
}

# ==========================================
# EXECUÇÃO
# ==========================================
Ensure-Folder -Path $TempFolder
Login-Azure

$ctx = Get-StorageContext -StorageAccountName $StorageAccountName
if (-not $ctx) {
    throw "Storage context não foi obtido."
}

Write-Host "✅ Storage context carregado com sucesso."

$nowLocal = Get-BrazilNow
Write-Host "🕒 Horário local Brasil: $($nowLocal.ToString('yyyy-MM-dd HH:mm:ss'))"

$requestedPartitionDate = ""
if (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
    $requestedPartitionDate = $PipelineDate
}
elseif (-not [string]::IsNullOrWhiteSpace($Date)) {
    $requestedPartitionDate = $Date
}

$resolvedPartitionDate = $null

if ($UseLatestInventoryFromLake) {
    $resolvedPartitionDate = Get-AvailablePartitionDate `
        -Ctx $ctx `
        -Container $FinopsContainer `
        -BasePrefix $InventoryPrefix `
        -RequestedDate $requestedPartitionDate

    $invPath = Download-BlobByResolvedDatePrefix `
        -Ctx $ctx `
        -Container $FinopsContainer `
        -BasePrefix $InventoryPrefix `
        -Dt $resolvedPartitionDate `
        -Label "inventory" `
        -OutFolder $TempFolder
}
else {
    if ([string]::IsNullOrWhiteSpace($InventoryCsvPath)) {
        throw "Se UseLatestInventoryFromLake=`$false, informe -InventoryCsvPath."
    }

    if (-not (Test-Path $InventoryCsvPath)) {
        throw "Inventário não encontrado: $InventoryCsvPath"
    }

    $invPath = $InventoryCsvPath

    if (-not [string]::IsNullOrWhiteSpace($requestedPartitionDate)) {
        $resolvedPartitionDate = $requestedPartitionDate
    }
    else {
        $resolvedPartitionDate = $nowLocal.ToString("yyyy-MM-dd")
        Write-Host "📅 Partição de saída não informada. Usando data local Brasil: $resolvedPartitionDate"
    }
}

Write-Host "📅 Partição resolvida do pipeline: $resolvedPartitionDate"
Write-Host "📥 Inventory: $invPath"

$inv = @(Import-Csv -Path $invPath -Delimiter ";")
if ($inv.Count -eq 0) {
    throw "Inventário vazio/não lido: $invPath"
}

$shared = @(
    $inv | Where-Object { (Normalize-Text $_.'FINOPS-CLIENTE') -eq "COMPARTILHADO" }
)

Write-Host "🔎 Shared (FINOPS-CLIENTE=COMPARTILHADO): $($shared.Count) recursos"

$byRg = @(
    $shared |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceGroupName) } |
    Group-Object ResourceGroupName |
    Sort-Object Count -Descending
)

$overrides = @()
$candidates = @()

foreach ($g in $byRg) {
    $rg = Normalize-Text $g.Name
    if (-not $rg) { continue }

    $aks = Get-AksClusterFromMcRg -rg $rg
    $svcKey = $null
    $svcType = $null
    $notes = ""
    $confidence = 0

    if ($aks) {
        $svcKey = "AKS:$aks"
        $svcType = "AKS"
        $notes = "Auto from MC_ RG (cluster=$aks)"
        $confidence = 95
    }
    else {
        $svcKey = Canonicalize-InfraKeyFromRg -rg $rg
        $svcType = "INFRA"
        $notes = "Auto INFRA from RG name (canonical)"
        $confidence = 70
    }

    $pattern = "(?i)^{0}$" -f [regex]::Escape($rg)

    $overrides += [PSCustomObject]@{
        Pattern     = $pattern
        ServiceKey  = $svcKey
        ServiceType = $svcType
        Notes       = $notes
    }

    $candidates += [PSCustomObject]@{
        PipelinePartitionDate = $resolvedPartitionDate
        ResourceGroupName     = $rg
        CountShared           = $g.Count
        SuggestedServiceKey   = $svcKey
        SuggestedServiceType  = $svcType
        Confidence            = $confidence
        Notes                 = $notes
    }
}

$tmpOverrides  = Join-Path $TempFolder "overrides_rg_$resolvedPartitionDate.csv"
$tmpCandidates = Join-Path $TempFolder "overrides_rg_candidates_$resolvedPartitionDate.csv"

$overrides  | Export-Csv -Path $tmpOverrides  -NoTypeInformation -Delimiter ";" -Encoding UTF8
$candidates | Export-Csv -Path $tmpCandidates -NoTypeInformation -Delimiter ";" -Encoding UTF8

Write-Host "✅ overrides gerado: $tmpOverrides"
Write-Host "✅ candidates gerado: $tmpCandidates"

$blobOverrides  = "$OverridesPrefix/dt=$resolvedPartitionDate/overrides_rg_$resolvedPartitionDate.csv"
$blobCandidates = "$OverridesPrefix/dt=$resolvedPartitionDate/overrides_rg_candidates_$resolvedPartitionDate.csv"

Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $tmpOverrides  -Blob $blobOverrides
Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $tmpCandidates -Blob $blobCandidates

Write-Host "🚀 Upload concluído:"
Write-Host " - $FinopsContainer/$blobOverrides"
Write-Host " - $FinopsContainer/$blobCandidates"
