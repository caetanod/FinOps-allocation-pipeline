<#
Objetivo:
Gerar a share de alocação para SQL Pool com base em métricas específicas do ambiente.

Função no pipeline:
Substituir o rateio genérico por uma distribuição especializada para workloads de SQL Pool.

Entrada:
- resource_to_service
- métricas de consumo / composição do SQL Pool

Saída:
- sqlpool_allocation_share_all
- detalhes analíticos do cálculo

Observação:
É uma trilha especializada. Deve ser usada quando o custo de SQL Pool precisa ser distribuído por critério técnico mais preciso do que o peso global.

###################
  .\07_build_sqlpool_allocation_share_from_metrics.ps1 -Date "2026-03-11"
###################
#>

# Ajustes finais aplicados:
# - PipelinePartitionDate separada de UsageDate
# - UsageDate prioriza UsageDateTime/UsageDate do conteúdo
# - Sem dependência de partição comum entre insumos
# - Fallback por insumo (hoje, D-1, D-2)
# - Parse de data robusto compatível com Azure Automation

param(
  [Parameter(Mandatory = $false)]
  [string]$Date = "",

  [Parameter(Mandatory = $false)]
  [string]$PipelineDate = "",

  [Parameter(Mandatory = $false)]
  [string]$StartDateTimeUtc = "",

  [Parameter(Mandatory = $false)]
  [string]$EndDateTimeUtc = "",

  [Parameter(Mandatory = $false)]
  [double]$CpuWeight = 0.97,

  [Parameter(Mandatory = $false)]
  [double]$DiskWeight = 0.03,

  [Parameter(Mandatory = $false)]
  [string]$StorageAccountName = "stpslkmmfinopseusprd",

  [Parameter(Mandatory = $false)]
  [string]$FinopsContainer = "finops",

  [Parameter(Mandatory = $false)]
  [string]$InventoryPrefix = "bronze/inventory_daily",

  [Parameter(Mandatory = $false)]
  [string]$FactCostPrefix = "silver/fact_cost",

  [Parameter(Mandatory = $false)]
  [string]$ResourceToServicePrefix = "silver/resource_to_service",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefix = "gold/sqlpool_allocation_share_metrics",

  [Parameter(Mandatory = $false)]
  [string]$TempFolder = "C:\Temp\finops"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

$TargetSubscriptions = @(
  "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
  "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

function Get-BrazilNow {
  $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("E. South America Standard Time")
  return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

if ($PipelineDate) { $Date = $PipelineDate }
if ([string]::IsNullOrWhiteSpace($Date)) {
  $Date = (Get-BrazilNow).ToString("yyyy-MM-dd")
}

$Date = ($Date + "").Trim().Trim('"').Trim("'")
$UsageDate = $Date
$PipelinePartitionDate = $Date

Write-Host "📅 Pipeline reference date: $Date"

function Ensure-Folder {
  param([Parameter(Mandatory = $true)][string]$Path)
  if (-not (Test-Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Test-AzCli {
  if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI 'az' não encontrado no PATH. Este runbook precisa de Azure CLI disponível."
  }
}

function Login-Azure {
  Write-Host "🔐 Conectando com Managed Identity no Az PowerShell..."
  Disable-AzContextAutosave -Scope Process | Out-Null
  Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null

  Write-Host "🔐 Conectando com Managed Identity no Azure CLI..."
  $null = az login --identity 2>$null

  if ($LASTEXITCODE -ne 0) {
    throw "Falha no 'az login --identity'. O Azure CLI não conseguiu autenticar com Managed Identity."
  }

  Write-Host "✅ Azure CLI autenticado com Managed Identity."
}

function Set-AzCliSubscription {
  param(
    [Parameter(Mandatory = $true)]
    [string]$SubscriptionId
  )

  Write-Host "🔄 Azure CLI set subscription: $SubscriptionId"
  $null = az account set --subscription $SubscriptionId 2>$null

  if ($LASTEXITCODE -ne 0) {
    throw "Falha ao executar 'az account set --subscription $SubscriptionId'"
  }
}

function Get-StorageContext {
  param([Parameter(Mandatory = $true)][string]$StorageAccountName)

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

  throw "Storage Account '$StorageAccountName' não encontrado."
}

function Upload-ToBlob {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$LocalPath,
    [Parameter(Mandatory = $true)][string]$BlobPath
  )

  if (-not (Test-Path $LocalPath)) {
    throw "Arquivo local não encontrado para upload: $LocalPath"
  }

  Set-AzStorageBlobContent `
    -Context $Ctx `
    -Container $Container `
    -File $LocalPath `
    -Blob $BlobPath `
    -Force `
    -ErrorAction Stop | Out-Null
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

  Get-AzStorageBlobContent `
    -Context $Ctx `
    -Container $Container `
    -Blob $BlobName `
    -Destination $outPath `
    -Force `
    -ErrorAction Stop | Out-Null

  return $outPath
}

function Download-BlobByExactDatePrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$Dt,
    [Parameter(Mandatory = $true)][string]$NameRegex,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$OutFolder
  )

  $prefix = "$BasePrefix/dt=$Dt/"
  Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

  $blobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop |
    Where-Object { $_.Name -match $NameRegex }
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

function Download-BlobByDateFallback {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$ReferenceDate,
    [Parameter(Mandatory = $true)][string]$NameRegex,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$OutFolder,
    [int]$LookbackDays = 2
  )

  [datetime]$refDate = [datetime]::ParseExact($ReferenceDate, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)

  for ($i = 0; $i -le $LookbackDays; $i++) {
    $dt = $refDate.AddDays(-$i).ToString('yyyy-MM-dd')
    try {
      $path = Download-BlobByExactDatePrefix -Ctx $Ctx -Container $Container -BasePrefix $BasePrefix -Dt $dt -NameRegex $NameRegex -Label $Label -OutFolder $OutFolder
      return [PSCustomObject]@{
        Path = $path
        PartitionDate = $dt
      }
    }
    catch {
      Write-Host "⚠ $Label não encontrado em dt=$dt. Tentando partição anterior..." -ForegroundColor Yellow
    }
  }

  throw "Nenhum CSV encontrado para '$Label' entre dt=$ReferenceDate e dt=$($refDate.AddDays(-$LookbackDays).ToString('yyyy-MM-dd'))."
}

function Normalize-Text {
  param([string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  return ($s + "").Trim([char]0xFEFF).Trim()
}

function Parse-DoubleInvariant {
  param([string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return 0.0 }

  $inv = [System.Globalization.CultureInfo]::InvariantCulture
  $x = ($s + "").Replace(",", ".")
  [double]$v = 0
  [double]::TryParse($x, [System.Globalization.NumberStyles]::Any, $inv, [ref]$v) | Out-Null
  return $v
}

function Get-RowDateValue {
  param($row)

  if ($null -eq $row) { return "" }

  if ($row.PSObject.Properties.Name -contains "UsageDateTime") {
    $v = Normalize-Text $row.UsageDateTime
    if ($v) { return $v }
  }

  if ($row.PSObject.Properties.Name -contains "UsageDate") {
    $v = Normalize-Text $row.UsageDate
    if ($v) { return $v }
  }

  if ($row.PSObject.Properties.Name -contains "Date") {
    $v = Normalize-Text $row.Date
    if ($v) { return $v }
  }

  return ""
}

function Convert-ToDateInvariant {
  param([string]$Value)

  $raw = Normalize-Text $Value
  if (-not $raw) { return $null }

  $formats = @(
    'yyyy-MM-dd',
    'yyyy-MM-ddTHH:mm:ss',
    'yyyy-MM-ddTHH:mm:ssZ',
    'yyyy-MM-ddTHH:mm:ss.fff',
    'yyyy-MM-ddTHH:mm:ss.fffZ',
    'yyyy-MM-dd HH:mm:ss',
    'MM/dd/yyyy',
    'MM/dd/yyyy HH:mm:ss'
  )

  foreach ($fmt in $formats) {
    try {
      return [datetime]::ParseExact(
        $raw,
        $fmt,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::AssumeUniversal
      )
    }
    catch {
    }
  }

  try {
    return [datetime]::Parse(
      $raw,
      [System.Globalization.CultureInfo]::InvariantCulture,
      [System.Globalization.DateTimeStyles]::AssumeUniversal
    )
  }
  catch {
    return $null
  }
}

function Get-MaxUsageDateFromRows {
  param([array]$Rows)

  $maxDate = $null

  foreach ($row in $Rows) {
    $rawDate = Get-RowDateValue -row $row
    if (-not $rawDate) { continue }

    $parsed = Convert-ToDateInvariant -Value $rawDate
    if ($null -eq $parsed) { continue }

    if ($null -eq $maxDate -or $parsed -gt $maxDate) {
      $maxDate = $parsed
    }
  }

  if ($null -eq $maxDate) {
    throw 'Não foi possível determinar MaxUsageDate a partir do conteúdo de fact_cost.'
  }

  return $maxDate
}

function Build-DayWindowUtc {
  param([string]$day)

  @{
    Start = "{0}T00:00:00Z" -f $day
    End   = "{0}T23:59:59Z" -f $day
  }
}

function Get-PoolInfoFromResourceId {
  param([string]$rid)

  $pattern = '^/subscriptions/(?<sub>[^/]+)/resourceGroups/(?<rg>[^/]+)/providers/Microsoft\.Sql/servers/(?<server>[^/]+)/elasticPools/(?<pool>[^/]+)$'
  $m = [regex]::Match($rid, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if (-not $m.Success) { return $null }

  return @{
    SubscriptionId = $m.Groups["sub"].Value
    ResourceGroup  = $m.Groups["rg"].Value
    Server         = $m.Groups["server"].Value
    Pool           = $m.Groups["pool"].Value
  }
}

function Get-MetricAvg {
  param(
    [string]$SubscriptionId,
    [string]$ResourceGroup,
    [string]$Server,
    [string]$Db,
    [string]$MetricName,
    [string]$StartDate,
    [string]$EndDate,
    [int]$Precision = 4
  )

  $result = az monitor metrics list `
      --resource "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Sql/servers/$Server/databases/$Db" `
      --metric $MetricName `
      --interval PT1H `
      --aggregation Average `
      --start $StartDate `
      --end $EndDate `
      --query "value[].timeseries[].data[].average" -o tsv 2>$null

  if ($LASTEXITCODE -ne 0 -or -not $result) {
    return [math]::Round(0.0, $Precision)
  }

  $values = @($result -split "`n" | Where-Object { $_.Trim() -ne "" })
  if ($values.Count -eq 0) {
    return [math]::Round(0.0, $Precision)
  }

  $sum = 0.0
  $count = 0
  foreach ($v in $values) {
    [double]$d = 0
    if ([double]::TryParse(($v + "").Replace(",", "."), [System.Globalization.NumberStyles]::Any, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$d)) {
      $sum += $d
      $count++
    }
  }

  if ($count -gt 0) {
    return [math]::Round(($sum / $count), $Precision)
  }

  return [math]::Round(0.0, $Precision)
}

function Get-DbNamesInPool {
  param(
    [string]$ResourceGroup,
    [string]$Server,
    [string]$Pool
  )

  $dbListRaw = az sql db list -g $ResourceGroup -s $Server `
    --query "[?elasticPoolId!=null && contains(elasticPoolId, '/elasticPools/$Pool')].name" -o tsv 2>$null

  if ($LASTEXITCODE -ne 0) {
    return @()
  }

  return @($dbListRaw -split "`n" | Where-Object { $_.Trim() -ne "" } | Sort-Object -Unique)
}

Ensure-Folder -Path $TempFolder
Test-AzCli
Login-Azure

$ctx = Get-StorageContext -StorageAccountName $StorageAccountName

$cpuPctLabel  = [int]($CpuWeight * 100)
$diskPctLabel = [int]($DiskWeight * 100)
$notesLabel   = "CPU${cpuPctLabel}_DISK${diskPctLabel}"

Write-Host "`n== DOWNLOAD INPUTS =="

$inventoryBlob = Download-BlobByDateFallback -Ctx $ctx -Container $FinopsContainer -BasePrefix $InventoryPrefix -ReferenceDate $PipelinePartitionDate -NameRegex "\.csv$" -Label "inventory" -OutFolder $TempFolder
$factCostBlob  = Download-BlobByDateFallback -Ctx $ctx -Container $FinopsContainer -BasePrefix $FactCostPrefix -ReferenceDate $PipelinePartitionDate -NameRegex "fact_cost_.*\.csv$" -Label "fact_cost" -OutFolder $TempFolder
$mappingBlob   = Download-BlobByDateFallback -Ctx $ctx -Container $FinopsContainer -BasePrefix $ResourceToServicePrefix -ReferenceDate $PipelinePartitionDate -NameRegex "resource_to_service_shared_.*\.csv$" -Label "resource_to_service_shared" -OutFolder $TempFolder

$inventoryPath = $inventoryBlob.Path
$factCostPath  = $factCostBlob.Path
$mappingPath   = $mappingBlob.Path

Write-Host "📥 Inventory: $inventoryPath (dt=$($inventoryBlob.PartitionDate))"
Write-Host "📥 FactCost : $factCostPath (dt=$($factCostBlob.PartitionDate))"
Write-Host "📥 Mapping  : $mappingPath (dt=$($mappingBlob.PartitionDate))"

$inventory = @(Import-Csv -Path $inventoryPath -Delimiter ";")
$factCost  = @(Import-Csv -Path $factCostPath  -Delimiter ";")
$mapping   = @(Import-Csv -Path $mappingPath   -Delimiter ";")

if ($inventory.Count -eq 0) { throw "Inventário vazio." }
if ($factCost.Count  -eq 0) { throw "fact_cost vazio." }
if ($mapping.Count   -eq 0) { throw "mapping vazio." }

$UsageDate = (Get-MaxUsageDateFromRows -Rows $factCost).ToString('yyyy-MM-dd')
Write-Host "📅 UsageDate derivada do fact_cost: $UsageDate"

if ([string]::IsNullOrWhiteSpace($StartDateTimeUtc) -or [string]::IsNullOrWhiteSpace($EndDateTimeUtc)) {
  $window = Build-DayWindowUtc $UsageDate
  $StartDateTimeUtc = $window.Start
  $EndDateTimeUtc   = $window.End
  Write-Host "🪟 Janela de métricas ajustada pela UsageDate: $StartDateTimeUtc até $EndDateTimeUtc"
}

$ridToMeta = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)
foreach ($r in $inventory) {
  $rid = Normalize-Text $r.ResourceId
  if (-not $rid) { continue }

  $ridToMeta[$rid] = @{
    ResourceName      = Normalize-Text $r.Name
    ResourceType      = Normalize-Text $r.Type
    ResourceGroupName = Normalize-Text $r.ResourceGroupName
    ClienteTag        = Normalize-Text $r.'FINOPS-CLIENTE'
  }
}

$ridToServiceKey = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)
foreach ($m in $mapping) {
  $rid = Normalize-Text $m.ResourceId
  if (-not $rid) { continue }
  $ridToServiceKey[$rid] = Normalize-Text $m.ServiceKey
}

$pools = New-Object System.Collections.Generic.List[object]
$poolSeen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)

foreach ($m in $mapping) {
  $serviceKey = Normalize-Text $m.ServiceKey
  if (-not $serviceKey.StartsWith("SQLPOOL:", [System.StringComparison]::OrdinalIgnoreCase)) { continue }

  $poolInfo = $null
  if ((Normalize-Text $m.ResourceType) -eq "Microsoft.Sql/servers/elasticpools") {
    $poolInfo = Get-PoolInfoFromResourceId (Normalize-Text $m.ResourceId)
  }

  if (-not $poolInfo) { continue }

  $uniq = "{0}|{1}|{2}|{3}" -f $poolInfo.SubscriptionId, $poolInfo.ResourceGroup, $poolInfo.Server, $poolInfo.Pool
  if ($poolSeen.Contains($uniq)) { continue }

  $poolSeen.Add($uniq) | Out-Null

  $pools.Add([PSCustomObject]@{
    SubscriptionId = $poolInfo.SubscriptionId
    ResourceGroup  = $poolInfo.ResourceGroup
    Server         = $poolInfo.Server
    Pool           = $poolInfo.Pool
    ServiceKey     = $serviceKey
  }) | Out-Null
}

$poolsArray = @($pools.ToArray())
if ($poolsArray.Count -eq 0) {
  throw "Nenhum SQLPOOL encontrado no mapping."
}

Write-Host "`n== POOLS DISCOVERED =="
$poolsArray | Format-Table -AutoSize

$dailyCostByServiceKey = @{}
$costByRid = @{}

foreach ($c in $factCost) {
  $rid = Normalize-Text $c.ResourceId
  if (-not $rid) { continue }

  $factDate = Get-RowDateValue $c
  if ($factDate -ne $UsageDate) { continue }

  if (-not $ridToServiceKey.ContainsKey($rid)) { continue }

  $sk = $ridToServiceKey[$rid]
  $val = Parse-DoubleInvariant $c.Cost

  if (-not $dailyCostByServiceKey.ContainsKey($sk)) { $dailyCostByServiceKey[$sk] = 0.0 }
  $dailyCostByServiceKey[$sk] += $val

  if (-not $costByRid.ContainsKey($rid)) { $costByRid[$rid] = 0.0 }
  $costByRid[$rid] += $val
}

Write-Host "`n== DAILY COST BY SERVICEKEY =="
if ($dailyCostByServiceKey.Keys.Count -eq 0) {
  Write-Host "⚠ Nenhum custo diário associado a ServiceKey foi encontrado."
}
else {
  $dailyCostByServiceKey.GetEnumerator() |
    Sort-Object Name |
    ForEach-Object {
      Write-Host ("- {0} => {1}" -f $_.Key, ([double]$_.Value).ToString("F6",[cultureinfo]::InvariantCulture))
    }
}

$allDetail = New-Object System.Collections.Generic.List[object]
$allAllocationShare = New-Object System.Collections.Generic.List[object]
$diagPoolSummary = New-Object System.Collections.Generic.List[object]

$currentSubscription = ""

foreach ($poolRow in $poolsArray) {
  $subId = $poolRow.SubscriptionId
  $poolRg = $poolRow.ResourceGroup
  $server = $poolRow.Server
  $pool   = $poolRow.Pool
  $targetServiceKey = $poolRow.ServiceKey

  Write-Host ""
  Write-Host "============================================================"
  Write-Host "Subscription: $subId"
  Write-Host "Pool        : $pool"
  Write-Host "RG          : $poolRg"
  Write-Host "Server      : $server"
  Write-Host "ServiceKey  : $targetServiceKey"
  Write-Host "============================================================"

  if ($currentSubscription -ne $subId) {
    try {
      Set-AzCliSubscription -SubscriptionId $subId
      $currentSubscription = $subId
    }
    catch {
      Write-Host "⚠ Não foi possível trocar contexto para $subId. Detalhe: $($_.Exception.Message)" -ForegroundColor Yellow
      $diagPoolSummary.Add([PSCustomObject]@{
        ServiceKey     = $targetServiceKey
        Status         = "SKIPPED_SUBSCRIPTION_CONTEXT"
        PoolDailyCost  = 0
        DbCount        = 0
        TotalCpu       = 0
        TotalDiskPct   = 0
        TotalWeighted  = 0
      }) | Out-Null
      continue
    }
  }

  if (-not $dailyCostByServiceKey.ContainsKey($targetServiceKey)) {
    Write-Host "⚠ Sem custo para este ServiceKey na data $UsageDate." -ForegroundColor Yellow

    $poolRidGuess = "/subscriptions/$subId/resourceGroups/$poolRg/providers/Microsoft.Sql/servers/$server/elasticPools/$pool"
    $directPoolCost = 0.0
    if ($costByRid.ContainsKey($poolRidGuess)) {
      $directPoolCost = [double]$costByRid[$poolRidGuess]
    }

    Write-Host "🔎 Pool ResourceId esperado: $poolRidGuess"
    Write-Host "🔎 Custo direto por ResourceId do pool: $($directPoolCost.ToString('F6',[cultureinfo]::InvariantCulture))"

    $diagPoolSummary.Add([PSCustomObject]@{
      ServiceKey     = $targetServiceKey
      Status         = "SKIPPED_NO_DAILY_COST"
      PoolDailyCost  = 0
      DbCount        = 0
      TotalCpu       = 0
      TotalDiskPct   = 0
      TotalWeighted  = 0
    }) | Out-Null
    continue
  }

  $poolDailyCost = [Math]::Round([double]$dailyCostByServiceKey[$targetServiceKey], 6)
  Write-Host "💰 PoolDailyCost = $poolDailyCost"

  $dbNames = @(Get-DbNamesInPool -ResourceGroup $poolRg -Server $server -Pool $pool)

  Write-Host "📚 DBs encontradas no pool: $($dbNames.Count)"
  if ($dbNames.Count -gt 0) {
    $dbNames | ForEach-Object { Write-Host (" - {0}" -f $_) }
  }

  if ($dbNames.Count -eq 0) {
    Write-Host "⚠ Nenhuma DB encontrada no pool." -ForegroundColor Yellow
    $diagPoolSummary.Add([PSCustomObject]@{
      ServiceKey     = $targetServiceKey
      Status         = "SKIPPED_NO_DATABASES"
      PoolDailyCost  = $poolDailyCost
      DbCount        = 0
      TotalCpu       = 0
      TotalDiskPct   = 0
      TotalWeighted  = 0
    }) | Out-Null
    continue
  }

  $detail = New-Object System.Collections.Generic.List[object]

  foreach ($db in $dbNames) {
    $dbRid = "/subscriptions/$subId/resourceGroups/$poolRg/providers/Microsoft.Sql/servers/$server/databases/$db"

    $cliente = "DEFINIR"
    if ($ridToMeta.ContainsKey($dbRid)) {
      $cliente = Normalize-Text $ridToMeta[$dbRid].ClienteTag
      if ([string]::IsNullOrWhiteSpace($cliente) -or $cliente -eq "COMPARTILHADO" -or $cliente -eq "SEM TAG") {
        $cliente = "DEFINIR"
      }
    }

    $dbCpu         = Get-MetricAvg -SubscriptionId $subId -ResourceGroup $poolRg -Server $server -Db $db -MetricName "cpu_percent"     -StartDate $StartDateTimeUtc -EndDate $EndDateTimeUtc -Precision 4
    $dbDiskPercent = Get-MetricAvg -SubscriptionId $subId -ResourceGroup $poolRg -Server $server -Db $db -MetricName "storage_percent" -StartDate $StartDateTimeUtc -EndDate $EndDateTimeUtc -Precision 4
    $dbDiskBytes   = Get-MetricAvg -SubscriptionId $subId -ResourceGroup $poolRg -Server $server -Db $db -MetricName "storage"         -StartDate $StartDateTimeUtc -EndDate $EndDateTimeUtc -Precision 0

    [double]$dbDiskBytesD = 0
    [void][double]::TryParse(($dbDiskBytes.ToString() -replace ",","."), [System.Globalization.NumberStyles]::Any, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$dbDiskBytesD)
    $dbDiskGB = [Math]::Round(($dbDiskBytesD / [Math]::Pow(1024,3)), 4)

    Write-Host ("   DB={0} | Cliente={1} | CPU={2} | DiskPct={3} | DiskGB={4}" -f $db, $cliente, $dbCpu, $dbDiskPercent, $dbDiskGB)

    $detail.Add([PSCustomObject]@{
      UsageDate       = $UsageDate
      ServiceKey      = $targetServiceKey
      Pool            = $pool
      SubscriptionId  = $subId
      ResourceGroup   = $poolRg
      Server          = $server
      Database        = $db
      Cliente         = $cliente
      ResourceId      = $dbRid
      CpuAvgPct       = [Math]::Round($dbCpu, 4)
      DiskPercentAvg  = [Math]::Round($dbDiskPercent, 4)
      DiskGBAvg       = [Math]::Round($dbDiskGB, 4)
    }) | Out-Null
  }

  $detailArray = @($detail.ToArray())
  if ($detailArray.Count -eq 0) {
    $diagPoolSummary.Add([PSCustomObject]@{
      ServiceKey     = $targetServiceKey
      Status         = "SKIPPED_NO_DETAIL"
      PoolDailyCost  = $poolDailyCost
      DbCount        = 0
      TotalCpu       = 0
      TotalDiskPct   = 0
      TotalWeighted  = 0
    }) | Out-Null
    continue
  }

  $totalCpu = (($detailArray | Measure-Object CpuAvgPct -Sum).Sum)
  $totalDiskPct = (($detailArray | Measure-Object DiskPercentAvg -Sum).Sum)

  Write-Host "📈 TotalCpu     = $totalCpu"
  Write-Host "📈 TotalDiskPct = $totalDiskPct"

  $weightedRows = New-Object System.Collections.Generic.List[object]

  foreach ($d in $detailArray) {
    $contribCpu = 0.0
    if ($totalCpu -gt 0) { $contribCpu = ($d.CpuAvgPct / $totalCpu) * 100 }

    $contribDisk = 0.0
    if ($totalDiskPct -gt 0) { $contribDisk = ($d.DiskPercentAvg / $totalDiskPct) * 100 }

    $usageWeighted = ($contribCpu * $CpuWeight) + ($contribDisk * $DiskWeight)

    $weightedRows.Add([PSCustomObject]@{
      UsageDate       = $d.UsageDate
      ServiceKey      = $d.ServiceKey
      Pool            = $d.Pool
      SubscriptionId  = $d.SubscriptionId
      ResourceGroup   = $d.ResourceGroup
      Server          = $d.Server
      Cliente         = $d.Cliente
      Database        = $d.Database
      ResourceId      = $d.ResourceId
      CpuAvgPct       = $d.CpuAvgPct
      DiskPercentAvg  = $d.DiskPercentAvg
      DiskGBAvg       = $d.DiskGBAvg
      ContribCpuPct   = [Math]::Round($contribCpu, 4)
      ContribDiskPct  = [Math]::Round($contribDisk, 4)
      WeightedUsage   = [Math]::Round($usageWeighted, 6)
    }) | Out-Null
  }

  $weightedArray = @($weightedRows.ToArray())
  $totalWeighted = (($weightedArray | Measure-Object WeightedUsage -Sum).Sum)

  Write-Host "📈 TotalWeighted = $totalWeighted"

  if ($totalWeighted -le 0) {
    Write-Host "⚠ TotalWeighted = 0. Aplicando fallback por quantidade de DBs." -ForegroundColor Yellow

    $dbCount = $weightedArray.Count
    if ($dbCount -le 0) {
      $diagPoolSummary.Add([PSCustomObject]@{
        ServiceKey     = $targetServiceKey
        Status         = "SKIPPED_ZERO_WEIGHT_NO_DB"
        PoolDailyCost  = $poolDailyCost
        DbCount        = 0
        TotalCpu       = $totalCpu
        TotalDiskPct   = $totalDiskPct
        TotalWeighted  = $totalWeighted
      }) | Out-Null
      continue
    }

    foreach ($r in $weightedArray) {
      $share = 1.0 / $dbCount
      $allocatedCost = $poolDailyCost * $share

      $allDetail.Add([PSCustomObject]@{
        UsageDate       = $r.UsageDate
        ServiceKey      = $r.ServiceKey
        Pool            = $r.Pool
        SubscriptionId  = $r.SubscriptionId
        ResourceGroup   = $r.ResourceGroup
        Server          = $r.Server
        Cliente         = $r.Cliente
        Database        = $r.Database
        ResourceId      = $r.ResourceId
        CpuAvgPct       = $r.CpuAvgPct
        DiskPercentAvg  = $r.DiskPercentAvg
        DiskGBAvg       = $r.DiskGBAvg
        ContribCpuPct   = $r.ContribCpuPct
        ContribDiskPct  = $r.ContribDiskPct
        WeightedUsage   = $r.WeightedUsage
        Share           = [Math]::Round($share, 6)
        AllocatedCost   = [Math]::Round($allocatedCost, 6)
        PoolDailyCost   = $poolDailyCost
        DriverFallback  = "DB_COUNT_EQUAL_SPLIT"
      }) | Out-Null

      $allAllocationShare.Add([PSCustomObject]@{
        Date        = $UsageDate
        ServiceKey  = $r.ServiceKey
        Cliente     = $r.Cliente
        Share       = [Math]::Round($share, 6)
        DriverType  = "SQLPOOL_DB_COUNT_FALLBACK"
        Notes       = "Fallback equal split by DB count"
      }) | Out-Null
    }

    $diagPoolSummary.Add([PSCustomObject]@{
      ServiceKey     = $targetServiceKey
      Status         = "PROCESSED_FALLBACK_DB_COUNT"
      PoolDailyCost  = $poolDailyCost
      DbCount        = $dbCount
      TotalCpu       = $totalCpu
      TotalDiskPct   = $totalDiskPct
      TotalWeighted  = $totalWeighted
    }) | Out-Null

    continue
  }

  foreach ($r in $weightedArray) {
    $share = $r.WeightedUsage / $totalWeighted
    $allocatedCost = $poolDailyCost * $share

    $allDetail.Add([PSCustomObject]@{
      UsageDate       = $r.UsageDate
      ServiceKey      = $r.ServiceKey
      Pool            = $r.Pool
      SubscriptionId  = $r.SubscriptionId
      ResourceGroup   = $r.ResourceGroup
      Server          = $r.Server
      Cliente         = $r.Cliente
      Database        = $r.Database
      ResourceId      = $r.ResourceId
      CpuAvgPct       = $r.CpuAvgPct
      DiskPercentAvg  = $r.DiskPercentAvg
      DiskGBAvg       = $r.DiskGBAvg
      ContribCpuPct   = $r.ContribCpuPct
      ContribDiskPct  = $r.ContribDiskPct
      WeightedUsage   = $r.WeightedUsage
      Share           = [Math]::Round($share, 6)
      AllocatedCost   = [Math]::Round($allocatedCost, 6)
      PoolDailyCost   = $poolDailyCost
      DriverFallback  = ""
    }) | Out-Null

    $allAllocationShare.Add([PSCustomObject]@{
      Date        = $UsageDate
      ServiceKey  = $r.ServiceKey
      Cliente     = $r.Cliente
      Share       = [Math]::Round($share, 6)
      DriverType  = "SQLPOOL_DB_METRICS"
      Notes       = $notesLabel
    }) | Out-Null
  }

  $diagPoolSummary.Add([PSCustomObject]@{
    ServiceKey     = $targetServiceKey
    Status         = "PROCESSED_METRICS"
    PoolDailyCost  = $poolDailyCost
    DbCount        = $weightedArray.Count
    TotalCpu       = $totalCpu
    TotalDiskPct   = $totalDiskPct
    TotalWeighted  = $totalWeighted
  }) | Out-Null
}

$allDetailArray = @($allDetail.ToArray())
$allAllocationShareArray = @($allAllocationShare.ToArray())
$diagPoolSummaryArray = @($diagPoolSummary.ToArray())

Write-Host "`n== DIAGNOSTIC SUMMARY =="
if ($diagPoolSummaryArray.Count -eq 0) {
  Write-Host "⚠ Nenhum pool chegou a gerar resumo diagnóstico."
}
else {
  $diagPoolSummaryArray | Format-Table -AutoSize
}

if ($allDetailArray.Count -eq 0) {
  throw "Nenhum pool SQL com custo diário e métricas válidas foi processado."
}

$allocationShareGrouped = @(
  $allAllocationShareArray |
  Group-Object Date, ServiceKey, Cliente |
  ForEach-Object {
    $first = $_.Group[0]
    [PSCustomObject]@{
      Date        = $first.Date
      ServiceKey  = $first.ServiceKey
      Cliente     = $first.Cliente
      Share       = [Math]::Round((($_.Group | Measure-Object Share -Sum).Sum), 6)
      DriverType  = $first.DriverType
      Notes       = $first.Notes
    }
  }
)

$outDetail = Join-Path $TempFolder ("sqlpool_detail_all_{0}.csv" -f $UsageDate)
$outShare  = Join-Path $TempFolder ("sqlpool_allocation_share_all_{0}.csv" -f $UsageDate)

$allDetailArray | Export-Csv -Path $outDetail -Delimiter ";" -NoTypeInformation -Encoding UTF8
$allocationShareGrouped | Export-Csv -Path $outShare -Delimiter ";" -NoTypeInformation -Encoding UTF8

$blobDetail = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $outDetail -Leaf)"
$blobShare  = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $outShare -Leaf)"

Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outDetail -BlobPath $blobDetail
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outShare  -BlobPath $blobShare

Write-Host "✅ Upload concluído:"
Write-Host " - $FinopsContainer/$blobDetail"
Write-Host " - $FinopsContainer/$blobShare"