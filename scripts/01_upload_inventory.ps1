<#
Objetivo:
Fazer upload do CSV de inventário de recursos para o Data Lake na partição da data informada.

Função no pipeline:
É a etapa de ingestão do inventário usado nas classificações e análises posteriores.

Entrada:
- CSV local de inventário
- Data de referência

Saída:
- Arquivo enviado para o prefixo de inventory no Storage Account

Observação:
Não transforma os dados; apenas publica o inventário bruto na partição da data.

############
.\01_upload_inventory.ps1 -InventoryCsvPath ".\resource-report-FINOPS-TAGS-NSTECH-2026-03-17.csv" -Date "2026-03-13"
############
#>


param(
    [string]$PipelineDate = "",
    [string]$StorageAccountName = "stpslkmmfinopseusprd",
    [string]$Container = "finops",
    [string]$TempFolder = "C:\Temp\finops"
)

# ==========================================
# CONFIG
# ==========================================
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

# SUBSCRIPTIONS FIXAS
$TargetSubscriptions = @(
    "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
    "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

# TAGS FINOPS
$FinopsTags = @(
    "FINOPS-CLIENTE",
    "FINOPS-AMBIENTE",
    "FINOPS-EMPRESA",
    "FINOPS-BUTORRE"
)

# ==========================================
# FUNÇÕES
# ==========================================
function Ensure-Folder {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -Path $Path -ItemType Directory -Force | Out-Null
    }
}

function Get-BrazilDateString {
    param(
        [string]$Format = "yyyy-MM-dd"
    )

    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("E. South America Standard Time")
    }
    catch {
        try {
            $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("America/Sao_Paulo")
        }
        catch {
            throw "Não foi possível localizar o timezone do Brasil."
        }
    }

    $utcNow = [DateTime]::UtcNow
    $brNow  = [System.TimeZoneInfo]::ConvertTimeFromUtc($utcNow, $tz)
    return $brNow.ToString($Format)
}

function Login-Azure {
    Write-Host "🔐 Conectando com Managed Identity..."
    Disable-AzContextAutosave -Scope Process | Out-Null
    Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null
}

function Get-StorageContext {
    param(
        [Parameter(Mandatory = $true)]
        [string]$StorageAccountName
    )

    foreach ($subId in $TargetSubscriptions) {
        try {
            Select-AzSubscription -SubscriptionId $subId -ErrorAction Stop | Out-Null

            $sa = Get-AzStorageAccount -ErrorAction Stop |
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
        [Parameter(Mandatory = $true)]
        $Ctx,

        [Parameter(Mandatory = $true)]
        [string]$Container,

        [Parameter(Mandatory = $true)]
        [string]$File,

        [Parameter(Mandatory = $true)]
        [string]$Blob
    )

    if (-not (Test-Path -LiteralPath $File)) {
        throw "Arquivo local não encontrado para upload: $File"
    }

    Write-Host "⬆️ Iniciando upload..."
    Write-Host "   Container : $Container"
    Write-Host "   Blob      : $Blob"
    Write-Host "   Arquivo   : $File"

    try {
        Set-AzStorageBlobContent `
            -Context $Ctx `
            -Container $Container `
            -File $File `
            -Blob $Blob `
            -Force `
            -ErrorAction Stop | Out-Null

        Write-Host "✅ Upload concluído com sucesso."
    }
    catch {
        throw "❌ Falha no upload do blob. Detalhe: $($_.Exception.Message)"
    }
}

# ==========================================
# DATA
# ==========================================
# Regra corrigida:
# - não usar AddDays(-1)
# - se PipelineDate não for informada pelo master, usar a data local do Brasil
if ([string]::IsNullOrWhiteSpace($PipelineDate)) {
    $PipelineDate = Get-BrazilDateString
}

Write-Host "📅 Data de processamento (PipelineDate): $PipelineDate"

# ==========================================
# START
# ==========================================
Ensure-Folder -Path $TempFolder
Login-Azure

$ctx = Get-StorageContext -StorageAccountName $StorageAccountName

if (-not $ctx) {
    throw "Storage context não foi obtido."
}

Write-Host "✅ Storage context carregado com sucesso."

# ==========================================
# INVENTÁRIO
# ==========================================
$results = New-Object System.Collections.Generic.List[object]

foreach ($subId in $TargetSubscriptions) {
    Write-Host ""
    Write-Host "➡️ Processando subscription: $subId"

    try {
        Select-AzSubscription -SubscriptionId $subId -ErrorAction Stop | Out-Null
        $resources = Get-AzResource -ErrorAction Stop
    }
    catch {
        Write-Warning "Falha ao listar recursos da subscription $subId. Detalhe: $($_.Exception.Message)"
        continue
    }

    foreach ($r in $resources) {
        $obj = [PSCustomObject]@{
            SubscriptionId    = $subId
            ResourceGroupName = $r.ResourceGroupName
            Type              = $r.Type
            Name              = $r.Name
            ResourceId        = $r.ResourceId
            Location          = $r.Location
        }

        foreach ($tag in $FinopsTags) {
            $value = "SEM TAG"

            if ($null -ne $r.Tags) {
                if ($r.Tags -is [System.Collections.IDictionary]) {
                    $tagKeys = @($r.Tags.Keys | ForEach-Object { [string]$_ })
                    if ($tagKeys -contains [string]$tag) {
                        $tagValue = $r.Tags[$tag]
                        if (-not [string]::IsNullOrWhiteSpace([string]$tagValue)) {
                            $value = [string]$tagValue
                        }
                    }
                }
                elseif ($r.Tags -and $r.Tags.PSObject -and ($r.Tags.PSObject.Properties.Name -contains $tag)) {
                    $tagValue = $r.Tags.$tag
                    if (-not [string]::IsNullOrWhiteSpace([string]$tagValue)) {
                        $value = [string]$tagValue
                    }
                }
            }

            $obj | Add-Member -Name $tag -Value $value -MemberType NoteProperty -Force
        }

        [void]$results.Add($obj)
    }
}

if ($results.Count -eq 0) {
    throw "Nenhum recurso encontrado nas subscriptions informadas."
}

# ==========================================
# EXPORT CSV
# ==========================================
# Padronização obrigatória do pipeline: delimitador ';' e UTF8
$fileName  = "inventory_$PipelineDate.csv"
$localPath = Join-Path $TempFolder $fileName

$results |
    Sort-Object SubscriptionId, ResourceGroupName, Type, Name |
    Export-Csv -Path $localPath -NoTypeInformation -Delimiter ";" -Encoding UTF8

Write-Host "✅ Inventário gerado: $localPath"
Write-Host "📦 Total de recursos exportados: $($results.Count)"

# ==========================================
# UPLOAD
# ==========================================
$blobPath = "bronze/inventory_daily/dt=$PipelineDate/$fileName"

Upload-Blob -Ctx $ctx -Container $Container -File $localPath -Blob $blobPath

Write-Host "🚀 Inventário publicado com sucesso!"
Write-Host "📍 Blob final: $blobPath"
