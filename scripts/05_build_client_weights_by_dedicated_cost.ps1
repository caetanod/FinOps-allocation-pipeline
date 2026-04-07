<#
Objetivo:
Calcular o peso de cada cliente com base no custo dedicado.

Função no pipeline:
Gerar a distribuição proporcional que será usada no rateio genérico dos custos compartilhados.

Princípios aplicados nesta versão:
- NÃO usa D-1 fixo
- Separa PipelinePartitionDate de UsageDate
- PipelinePartitionDate = data de processamento / partição disponível
- UsageDate = data real do custo, derivada do conteúdo do fact_cost
- Não exige dt= em comum entre inventory_daily e fact_cost
- Aplica timezone Brasil + fallback de partição
- Publica a saída particionada por UsageDate

Exemplo:
.\05_build_client_weights_by_dedicated_cost.ps1 `
  -PipelinePartitionDate "2026-04-01" `
  -UsageDate "2026-03-31"
#>

param(
    [string]$PipelinePartitionDate = "",
    [string]$UsageDate = "",

    [string]$StorageAccountName = "stpslkmmfinopseusprd",
    [string]$FinopsContainer = "finops",

    [string]$InventoryPrefix = "bronze/inventory_daily",
    [string]$FactCostPrefix = "silver/fact_cost",
    [string]$OutputPrefix = "silver/client_weights_by_dedicated_cost",

    [string]$TempFolder = "/tmp/finops"
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
# BASE FUNCTIONS
# ==========================================
function Ensure-Folder {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -Path $Path -ItemType Directory -Force | Out-Null
    }
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
            throw "Não foi possível resolver o timezone do Brasil. Erro: $($_.Exception.Message)"
        }
    }

    return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

function Login-Azure {
    Write-Host "🔐 Autenticando com Managed Identity..."
    Disable-AzContextAutosave -Scope Process | Out-Null
    Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null
}

function Get-StorageContext {
    param([Parameter(Mandatory = $true)][string]$StorageAccountName)

    foreach ($subId in $TargetSubscriptions) {
        try {
            Select-AzSubscription -SubscriptionId $subId | Out-Null

            $sa = Get-AzStorageAccount -ErrorAction SilentlyContinue |
                Where-Object { $_.StorageAccountName -eq $StorageAccountName } |
                Select-Object -First 1

            if ($null -ne $sa) {
                Write-Host "✅ Storage encontrado na subscription: $subId"
                return $sa.Context
            }
        }
        catch {
            Write-Warning "Falha ao procurar storage na subscription $subId. Detalhe: $($_.Exception.Message)"
        }
    }

    throw "Storage '$StorageAccountName' não encontrado nas subscriptions alvo."
}

function Get-AvailablePartitionDate {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [string]$PreferredDate = ""
    )

    $datesToTry = New-Object System.Collections.Generic.List[string]

    if (-not [string]::IsNullOrWhiteSpace($PreferredDate)) {
        [void]$datesToTry.Add($PreferredDate)
    }

    $nowLocal = Get-BrazilNow
    foreach ($offset in 0..2) {
        $candidate = $nowLocal.AddDays(-$offset).ToString("yyyy-MM-dd")
        if (-not $datesToTry.Contains($candidate)) {
            [void]$datesToTry.Add($candidate)
        }
    }

    foreach ($dt in $datesToTry) {
        $prefixToCheck = "$Prefix/dt=$dt/"
        $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefixToCheck -ErrorAction SilentlyContinue)
        if ($blobs.Count -gt 0) {
            Write-Host "✅ Partição disponível em $Prefix : $dt"
            return $dt
        }
    }

    throw "Nenhuma partição disponível encontrada para '$Prefix' (tentado PreferredDate + hoje/D-1/D-2 no horário do Brasil)."
}

function Download-LatestCsvFromPartition {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [Parameter(Mandatory = $true)][string]$PartitionDate,
        [Parameter(Mandatory = $true)][string]$OutFolder,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $fullPrefix = "$Prefix/dt=$PartitionDate/"
    $blobs = @(
        Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $fullPrefix -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like "*.csv" }
    )

    if ($blobs.Count -eq 0) {
        throw "Nenhum CSV encontrado para $Label em '$fullPrefix'."
    }

    $blob = $blobs |
        Sort-Object {
            if ($_.ICloudBlob -and $_.ICloudBlob.Properties -and $null -ne $_.ICloudBlob.Properties.LastModified) {
                try { $_.ICloudBlob.Properties.LastModified.UtcDateTime }
                catch {
                    try { $_.ICloudBlob.Properties.LastModified.DateTime.ToUniversalTime() }
                    catch { [datetime]::MinValue }
                }
            }
            elseif ($_.LastModified) {
                try { $_.LastModified.UtcDateTime }
                catch {
                    try { $_.LastModified.DateTime.ToUniversalTime() }
                    catch { [datetime]::MinValue }
                }
            }
            else {
                [datetime]::MinValue
            }
        } -Descending |
        Select-Object -First 1

    $localPath = Join-Path $OutFolder ("{0}_{1}" -f $Label, (Split-Path $blob.Name -Leaf))

    Write-Host "⬇️ Download: $($blob.Name) -> $localPath"
    Get-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -Blob $blob.Name `
        -Destination $localPath `
        -Force | Out-Null

    return $localPath
}

function ConvertTo-InvariantDecimal {
    param([object]$Value)

    if ($null -eq $Value) { return [double]0 }

    $raw = [string]$Value
    $raw = $raw.Trim()

    if ([string]::IsNullOrWhiteSpace($raw)) { return [double]0 }

    $styles = [System.Globalization.NumberStyles]::AllowLeadingSign `
        -bor [System.Globalization.NumberStyles]::AllowDecimalPoint `
        -bor [System.Globalization.NumberStyles]::AllowThousands `
        -bor [System.Globalization.NumberStyles]::AllowLeadingWhite `
        -bor [System.Globalization.NumberStyles]::AllowTrailingWhite

    $cultureInvariant = [System.Globalization.CultureInfo]::InvariantCulture
    $culturePtBr = [System.Globalization.CultureInfo]::GetCultureInfo("pt-BR")
    $parsed = 0.0

    if ([double]::TryParse($raw, $styles, $cultureInvariant, [ref]$parsed)) {
        return [double]$parsed
    }

    if ([double]::TryParse($raw, $styles, $culturePtBr, [ref]$parsed)) {
        return [double]$parsed
    }

    $normalized = $raw -replace '\s', ''

    if ($normalized.Contains('.') -and $normalized.Contains(',')) {
        $lastDot = $normalized.LastIndexOf('.')
        $lastComma = $normalized.LastIndexOf(',')

        if ($lastComma -gt $lastDot) {
            $normalized = $normalized.Replace('.', '')
            $normalized = $normalized.Replace(',', '.')
        }
        else {
            $normalized = $normalized.Replace(',', '')
        }
    }
    elseif ($normalized.Contains(',')) {
        $normalized = $normalized.Replace('.', '')
        $normalized = $normalized.Replace(',', '.')
    }

    if ([double]::TryParse($normalized, $styles, $cultureInvariant, [ref]$parsed)) {
        return [double]$parsed
    }

    throw "Não foi possível converter o valor '$raw' para decimal."
}

function Get-UsageDateColumnName {
    param([object[]]$Rows)

    if ($null -eq $Rows -or $Rows.Count -eq 0) {
        return $null
    }

    $candidateColumns = @(
        "UsageDate",
        "UsageDateTime",
        "Date",
        "UsageDateUtc",
        "ConsumedServiceDate"
    )

    $first = $Rows[0]
    foreach ($col in $candidateColumns) {
        if ($first.PSObject.Properties.Name -contains $col) {
            return $col
        }
    }

    return $null
}

function ConvertTo-DateOnlyString {
    param([object]$Value)

    if ($null -eq $Value) { return $null }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    $dt = [datetime]::MinValue

    if ([datetime]::TryParse($text, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AssumeUniversal, [ref]$dt)) {
        return $dt.ToString("yyyy-MM-dd")
    }

    if ([datetime]::TryParse($text, [System.Globalization.CultureInfo]::GetCultureInfo("pt-BR"), [System.Globalization.DateTimeStyles]::None, [ref]$dt)) {
        return $dt.ToString("yyyy-MM-dd")
    }

    if ($text -match '^\d{4}-\d{2}-\d{2}$') {
        return $text
    }

    if ($text.Length -ge 10 -and $text.Substring(0, 10) -match '^\d{4}-\d{2}-\d{2}$') {
        return $text.Substring(0, 10)
    }

    return $null
}

function Resolve-UsageDateFromFactCost {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [string]$PreferredUsageDate = ""
    )

    if (-not [string]::IsNullOrWhiteSpace($PreferredUsageDate)) {
        return $PreferredUsageDate
    }

    $usageCol = Get-UsageDateColumnName -Rows $Rows
    if ([string]::IsNullOrWhiteSpace($usageCol)) {
        throw "Nenhuma coluna de UsageDate encontrada no fact_cost. Esperado: UsageDate / UsageDateTime / Date."
    }

    $validDates = New-Object System.Collections.Generic.List[datetime]

    foreach ($r in $Rows) {
        $value = $r.$usageCol
        $dateText = ConvertTo-DateOnlyString -Value $value
        if (-not [string]::IsNullOrWhiteSpace($dateText)) {
            $parsedDate = [datetime]::MinValue
            [void][datetime]::TryParseExact(
                $dateText,
                "yyyy-MM-dd",
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.DateTimeStyles]::None,
                [ref]$parsedDate
            )
            if ($parsedDate -ne [datetime]::MinValue) {
                [void]$validDates.Add($parsedDate)
            }
        }
    }

    if ($validDates.Count -eq 0) {
        throw "Não foi possível derivar UsageDate do conteúdo do fact_cost."
    }

    $maxUsageDate = ($validDates | Sort-Object -Descending | Select-Object -First 1)
    return $maxUsageDate.ToString("yyyy-MM-dd")
}

function Upload-Blob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$File,
        [Parameter(Mandatory = $true)][string]$Blob
    )

    Set-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -File $File `
        -Blob $Blob `
        -Force | Out-Null
}

# ==========================================
# EXECUÇÃO
# ==========================================
Ensure-Folder -Path $TempFolder
Login-Azure
$ctx = Get-StorageContext -StorageAccountName $StorageAccountName
$RunbookExecutionDate = (Get-BrazilNow).ToString("yyyy-MM-dd")

Write-Host "📅 PipelinePartitionDate (entrada): $PipelinePartitionDate"
Write-Host "📅 UsageDate (entrada): $UsageDate"
Write-Host "📂 TempFolder: $TempFolder"
Write-Host "📅 RunbookExecutionDate: $RunbookExecutionDate"

$ResolvedInventoryPartitionDate = Get-AvailablePartitionDate `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -Prefix $InventoryPrefix `
    -PreferredDate $PipelinePartitionDate

$ResolvedFactCostPartitionDate = Get-AvailablePartitionDate `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -Prefix $FactCostPrefix `
    -PreferredDate $PipelinePartitionDate

Write-Host "📅 InventoryPartitionDate resolvida: $ResolvedInventoryPartitionDate"
Write-Host "📅 FactCostPartitionDate resolvida : $ResolvedFactCostPartitionDate"

$inventoryPath = Download-LatestCsvFromPartition `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -Prefix $InventoryPrefix `
    -PartitionDate $ResolvedInventoryPartitionDate `
    -OutFolder $TempFolder `
    -Label "inventory"

$factCostPath = Download-LatestCsvFromPartition `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -Prefix $FactCostPrefix `
    -PartitionDate $ResolvedFactCostPartitionDate `
    -OutFolder $TempFolder `
    -Label "fact_cost"

Write-Host "📥 Inventory: $inventoryPath"
Write-Host "📥 FactCost : $factCostPath"

$invRows = @(Import-Csv -Path $inventoryPath -Delimiter ";")
$factRows = @(Import-Csv -Path $factCostPath -Delimiter ";")

if ($invRows.Count -eq 0) {
    throw "Inventory vazio após leitura do CSV."
}

if ($factRows.Count -eq 0) {
    throw "fact_cost vazio após leitura do CSV."
}

$ResolvedUsageDate = Resolve-UsageDateFromFactCost -Rows $factRows -PreferredUsageDate $UsageDate
Write-Host "📅 UsageDate resolvida do dado: $ResolvedUsageDate"

$usageColName = Get-UsageDateColumnName -Rows $factRows
if ([string]::IsNullOrWhiteSpace($usageColName)) {
    throw "Não foi possível identificar a coluna de UsageDate no fact_cost."
}

$factRowsFiltered = @(
    $factRows | Where-Object {
        (ConvertTo-DateOnlyString -Value $_.$usageColName) -eq $ResolvedUsageDate
    }
)

if ($factRowsFiltered.Count -eq 0) {
    throw "Nenhuma linha encontrada no fact_cost para UsageDate=$ResolvedUsageDate."
}

Write-Host "📊 Linhas fact_cost na UsageDate $ResolvedUsageDate : $($factRowsFiltered.Count)"

# ==========================================
# MAPA DE INVENTÁRIO
# ==========================================
$invMap = @{}
foreach ($r in $invRows) {
    $resourceId = [string]$r.ResourceId
    if (-not [string]::IsNullOrWhiteSpace($resourceId)) {
        $invMap[$resourceId.Trim().ToLowerInvariant()] = $r
    }
}

if ($invMap.Count -eq 0) {
    throw "Nenhum ResourceId válido encontrado no inventory."
}

# ==========================================
# JOIN COST + INVENTORY
# ==========================================
$joined = New-Object System.Collections.Generic.List[object]
$skippedWithoutInventory = 0
$skippedWithoutClient = 0
$skippedInvalidCost = 0

foreach ($c in $factRowsFiltered) {
    $rid = [string]$c.ResourceId
    if ([string]::IsNullOrWhiteSpace($rid)) { continue }

    $ridKey = $rid.Trim().ToLowerInvariant()
    if (-not $invMap.ContainsKey($ridKey)) {
        $skippedWithoutInventory++
        continue
    }

    $invRow = $invMap[$ridKey]
    $cliente = [string]$invRow.'FINOPS-CLIENTE'

    if ([string]::IsNullOrWhiteSpace($cliente)) {
        $skippedWithoutClient++
        continue
    }

    try {
        $costValue = ConvertTo-InvariantDecimal -Value $c.Cost
    }
    catch {
        $skippedInvalidCost++
        continue
    }

    $joined.Add([PSCustomObject]@{
        PipelinePartitionDate = $ResolvedFactCostPartitionDate
        UsageDate             = $ResolvedUsageDate
        Cliente               = $cliente.Trim()
        ResourceId            = $rid
        Cost                  = $costValue
    }) | Out-Null
}

Write-Host "📊 Linhas válidas após join: $($joined.Count)"
Write-Host "ℹ️ Ignoradas sem inventory: $skippedWithoutInventory"
Write-Host "ℹ️ Ignoradas sem cliente   : $skippedWithoutClient"
Write-Host "ℹ️ Ignoradas por custo inválido: $skippedInvalidCost"

if ($joined.Count -eq 0) {
    throw "Nenhuma linha válida restou após join entre inventory e fact_cost para UsageDate=$ResolvedUsageDate."
}

# ==========================================
# FILTRAR APENAS DEDICADO
# ==========================================
$dedicated = @(
    $joined | Where-Object {
        $_.Cliente -and $_.Cliente.Trim().ToUpperInvariant() -ne "COMPARTILHADO"
    }
)

if ($dedicated.Count -eq 0) {
    throw "Nenhum custo dedicado encontrado para UsageDate=$ResolvedUsageDate."
}

# ==========================================
# AGRUPAR POR CLIENTE
# ==========================================
$byClient = @(
    $dedicated |
    Group-Object Cliente |
    ForEach-Object {
        $sumCost = ($_.Group | Measure-Object -Property Cost -Sum).Sum

        [PSCustomObject]@{
            PipelinePartitionDate = $ResolvedFactCostPartitionDate
            UsageDate             = $ResolvedUsageDate
            Cliente               = $_.Name
            Cost                  = [double]$sumCost
        }
    }
)

if ($byClient.Count -eq 0) {
    throw "Nenhum agrupamento por cliente foi gerado."
}

$total = [double](($byClient | Measure-Object -Property Cost -Sum).Sum)

if ($total -le 0) {
    throw "Total de custo dedicado inválido para UsageDate=$ResolvedUsageDate. Total=$total"
}

# ==========================================
# CALCULAR PESO
# ==========================================
$weights = @(
    $byClient | Sort-Object Cliente | ForEach-Object {
        $weightValue = if ($total -eq 0) { 0.0 } else { [double]($_.Cost / $total) }

        [PSCustomObject]@{
            PipelinePartitionDate = $_.PipelinePartitionDate
            UsageDate             = $_.UsageDate
            Cliente               = $_.Cliente
            Cost                  = [math]::Round([double]$_.Cost, 6)
            Weight                = [math]::Round($weightValue, 6)
        }
    }
)

# ==========================================
# EXPORT
# ==========================================
$outFile = Join-Path $TempFolder ("client_weights_{0}.csv" -f $ResolvedUsageDate)

$weights |
    Select-Object `
        PipelinePartitionDate,
        UsageDate,
        Cliente,
        @{Name = "Cost"; Expression = { $_.Cost.ToString("F6", [System.Globalization.CultureInfo]::InvariantCulture) }},
        @{Name = "Weight"; Expression = { $_.Weight.ToString("F6", [System.Globalization.CultureInfo]::InvariantCulture) }} |
    Export-Csv -Path $outFile -NoTypeInformation -Delimiter ";" -Encoding UTF8

Write-Host "✅ client_weights gerado: $outFile"

# ==========================================
# UPLOAD
# ==========================================
$blob = "$OutputPrefix/dt=$RunbookExecutionDate/client_weights_$ResolvedUsageDate.csv"
Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $outFile -Blob $blob

Write-Host "🚀 client_weights publicado: $blob"
Write-Host "✅ Script 05 finalizado com sucesso"
