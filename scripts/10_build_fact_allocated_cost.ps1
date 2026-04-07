<#
Objetivo:
Aplicar a share final sobre o custo real e gerar a fact table de custo alocado por cliente.

Função no pipeline:
É a etapa final de materialização da alocação, transformando custo bruto + ServiceKey + share em custo efetivamente atribuído a cada cliente.

Entrada:
- fact_cost
- resource_to_service
- allocation_share_servicekey_final
- inventário

Saída:
- fact_allocated_cost
- final_cost_by_client
- final_cost_detailed
- reconciliation
- unallocated_cost

Observação:
Também registra os itens não alocados e os motivos da não alocação, sendo essencial para reconciliação e troubleshooting do pipeline.

Exemplo:
.\10_build_fact_allocated_cost_runbook.ps1 -Date "2026-03-12"
#>

param(
  [Parameter(Mandatory = $false)]
  [string]$Date = "",

  [Parameter(Mandatory = $false)]
  [string]$PipelineDate = "",

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
  [string]$ShareFinalPrefix = "gold/allocation_share_servicekey_final",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefixFact = "gold/fact_allocated_cost",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefixFinalByClient = "gold/final_cost_by_client",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefixFinalDetailed = "gold/final_cost_detailed",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefixReconciliation = "gold/reconciliation",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefixUnallocated = "gold/unallocated_cost",

  [Parameter(Mandatory = $false)]
  [string]$TempFolder = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

trap {
  Write-Error ("❌ Step 10 falhou. Linha: {0}. Comando: {1}. Erro: {2}" -f $_.InvocationInfo.ScriptLineNumber, $_.InvocationInfo.Line.Trim(), $_.Exception.Message)
  throw
}

$TargetSubscriptions = @(
  "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
  "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

function Get-BrazilNow {
  try {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("E. South America Standard Time")
  }
  catch {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("America/Sao_Paulo")
  }

  return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

function Get-CandidatePartitionDates {
  param(
    [int]$DaysBack = 2
  )

  $nowLocal = Get-BrazilNow
  $dates = New-Object System.Collections.Generic.List[string]

  for ($i = 0; $i -le $DaysBack; $i++) {
    $dates.Add($nowLocal.AddDays(-$i).ToString("yyyy-MM-dd")) | Out-Null
  }

  return $dates
}

function Resolve-PipelinePartitionDate {
  param(
    [string]$PreferredDate
  )

  $cleanPreferred = ($PreferredDate + "").Trim().Trim([char[]]@('"', "'"))
  if (-not [string]::IsNullOrWhiteSpace($cleanPreferred)) {
    return $cleanPreferred
  }

  return (Get-BrazilNow).ToString("yyyy-MM-dd")
}

$Date = ($Date + "").Trim().Trim([char[]]@('"', "'"))
$PipelineDate = ($PipelineDate + "").Trim().Trim([char[]]@('"', "'"))
$UsageDate = $Date
$PipelinePartitionDate = Resolve-PipelinePartitionDate -PreferredDate $PipelineDate

if ([string]::IsNullOrWhiteSpace($TempFolder)) {
  if ($env:TEMP) {
    $TempFolder = Join-Path $env:TEMP "finops"
  }
  elseif ($IsLinux -or $IsMacOS) {
    $TempFolder = "/tmp/finops"
  }
  else {
    $TempFolder = "C:\Temp\finops"
  }
}
elseif (($IsLinux -or $IsMacOS) -and -not [string]::IsNullOrWhiteSpace($TempFolder) -and $TempFolder.Length -ge 2 -and $TempFolder.Substring(1,1) -eq ':') {
  Write-Warning "TempFolder com caminho Windows detectado em worker Linux/macOS. Ajustando para /tmp/finops."
  $TempFolder = "/tmp/finops"
}

$FinopsTempFolder = $TempFolder

Write-Host "📅 Date: $Date"
Write-Host "📂 TempFolder: $FinopsTempFolder"

function Ensure-Folder {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  if ([string]::IsNullOrWhiteSpace($Path)) {
    throw "Ensure-Folder recebeu Path vazio."
  }

  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -Path $Path -ItemType Directory -Force -ErrorAction Stop | Out-Null
  }
}

function Ensure-AzLogin {
  Write-Host "🔐 Conectando com Managed Identity..."
  Disable-AzContextAutosave -Scope Process | Out-Null
  Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null
}

function Get-StorageContextByName {
  param(
    [Parameter(Mandatory = $true)]
    [string]$saName
  )

  foreach ($subId in $TargetSubscriptions) {
    try {
      Select-AzSubscription -SubscriptionId $subId | Out-Null

      $sa = Get-AzStorageAccount -ErrorAction SilentlyContinue |
        Where-Object { $_.StorageAccountName -eq $saName } |
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

  throw "Storage Account '$saName' não encontrado nas subscriptions informadas."
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

  Write-Host "⬆️ Iniciando upload..."
  Write-Host "   Container : $Container"
  Write-Host "   Blob      : $BlobPath"
  Write-Host "   Arquivo   : $LocalPath"

  Set-AzStorageBlobContent `
    -Context $Ctx `
    -Container $Container `
    -File $LocalPath `
    -Blob $BlobPath `
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

function Test-AnyBlobUnderPrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$Prefix,
    [Parameter(Mandatory = $false)][string]$NameRegex = ".*"
  )

  $items = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $Prefix -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match $NameRegex }
  )

  return ($items.Count -gt 0)
}

function Resolve-CommonPartitionDate {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][System.Collections.IEnumerable]$Checks,
    [Parameter(Mandatory = $false)][string]$PreferredDate = ""
  )

  $preferred = ($PreferredDate + "").Trim().Trim([char[]]@('"', "'"))
  $datesToTry = New-Object System.Collections.Generic.List[string]

  if (-not [string]::IsNullOrWhiteSpace($preferred)) {
    $datesToTry.Add($preferred) | Out-Null
  }

  foreach ($candidate in (Get-CandidatePartitionDates -DaysBack 2)) {
    if (-not ($datesToTry -contains $candidate)) {
      $datesToTry.Add($candidate) | Out-Null
    }
  }

  foreach ($dt in $datesToTry) {
    $allOk = $true

    foreach ($check in $Checks) {
      $prefix = "{0}/dt={1}/" -f $check.BasePrefix, $dt
      $ok = Test-AnyBlobUnderPrefix -Ctx $Ctx -Container $Container -Prefix $prefix -NameRegex $check.NameRegex
      if (-not $ok) {
        $allOk = $false
        break
      }
    }

    if ($allOk) {
      Write-Host "✅ Partição comum encontrada: $dt"
      return $dt
    }
  }

  throw "Nenhuma partição comum encontrada para os insumos obrigatórios (nem hoje, nem D-1, nem D-2 no horário do Brasil)."
}

function Get-AvailablePartitionDatesForPrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$NameRegex
  )

  $prefix = "$BasePrefix/"
  $blobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match $NameRegex }
  )

  $dates = New-Object System.Collections.Generic.List[string]
  foreach ($blob in $blobs) {
    if ($blob.Name -match '/dt=(\d{4}-\d{2}-\d{2})/') {
      $dt = $Matches[1]
      if (-not ($dates -contains $dt)) {
        $dates.Add($dt) | Out-Null
      }
    }
  }

  return @($dates | Sort-Object -Descending)
}

function Resolve-BestPartitionDateForInput {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$NameRegex,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $false)][string[]]$PreferredDates = @(),
    [Parameter(Mandatory = $false)][bool]$Optional = $false
  )

  $datesToTry = New-Object System.Collections.Generic.List[string]

  foreach ($d in @($PreferredDates)) {
    $clean = ($d + "").Trim().Trim([char[]]@('"', "'"))
    if (-not [string]::IsNullOrWhiteSpace($clean) -and -not ($datesToTry -contains $clean)) {
      $datesToTry.Add($clean) | Out-Null
    }
  }

  foreach ($candidate in (Get-CandidatePartitionDates -DaysBack 2)) {
    if (-not ($datesToTry -contains $candidate)) {
      $datesToTry.Add($candidate) | Out-Null
    }
  }

  foreach ($dt in $datesToTry) {
    $prefix = "{0}/dt={1}/" -f $BasePrefix, $dt
    if (Test-AnyBlobUnderPrefix -Ctx $Ctx -Container $Container -Prefix $prefix -NameRegex $NameRegex) {
      Write-Host ("✅ Partição selecionada para {0}: {1}" -f $Label, $dt)
      return $dt
    }
  }

  $available = @(Get-AvailablePartitionDatesForPrefix -Ctx $Ctx -Container $Container -BasePrefix $BasePrefix -NameRegex $NameRegex)
  if ($available.Count -gt 0) {
    $dt = $available[0]
    Write-Warning "Nenhuma partição encontrada para $Label nas datas preferidas. Usando partição mais recente disponível: $dt"
    return $dt
  }

  if ($Optional) {
    Write-Warning "Nenhuma partição disponível encontrada para $Label em '$Container/$BasePrefix'."
    return $null
  }

  throw "Nenhuma partição disponível encontrada para $Label em '$Container/$BasePrefix'."
}

function Try-ParseDateFlexible {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Value
  )

  $raw = Normalize-Text $Value
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }

  $cultures = @(
    [System.Globalization.CultureInfo]::InvariantCulture,
    [System.Globalization.CultureInfo]::GetCultureInfo("pt-BR"),
    [System.Globalization.CultureInfo]::GetCultureInfo("en-US")
  )

  $styles = [System.Globalization.DateTimeStyles]::AssumeLocal
  $formats = @(
    "yyyy-MM-dd",
    "yyyy-MM-ddTHH:mm:ss",
    "yyyy-MM-ddTHH:mm:ssZ",
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy",
    "MM/dd/yyyy HH:mm:ss",
    "dd/MM/yyyy",
    "dd/MM/yyyy HH:mm:ss"
  )

  foreach ($fmt in $formats) {
    foreach ($culture in $cultures) {
      $parsed = [datetime]::MinValue
      if ([datetime]::TryParseExact($raw, $fmt, $culture, $styles, [ref]$parsed)) {
        return $parsed.Date
      }
    }
  }

  foreach ($culture in $cultures) {
    $parsed = [datetime]::MinValue
    if ([datetime]::TryParse($raw, $culture, $styles, [ref]$parsed)) {
      return $parsed.Date
    }
  }

  return $null
}

function Get-MaxUsageDateFromRows {
  param(
    [Parameter(Mandatory = $true)]
    [System.Collections.IEnumerable]$Rows
  )

  $maxDate = $null

  foreach ($row in $Rows) {
    $raw = Get-RowDateValue $row
    if ([string]::IsNullOrWhiteSpace($raw)) { continue }

    $parsed = Try-ParseDateFlexible -Value $raw
    if ($null -eq $parsed) { continue }

    $d = $parsed.Date
    if ($null -eq $maxDate -or $d -gt $maxDate) {
      $maxDate = $d
    }
  }

  if ($null -eq $maxDate) {
    throw "Não foi possível derivar UsageDate a partir do conteúdo do CSV selecionado."
  }

  return $maxDate
}

function Get-DateDistribution {
  param(
    [Parameter(Mandatory = $true)]
    [System.Collections.IEnumerable]$Rows
  )

  $counts = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)

  foreach ($row in $Rows) {
    $raw = Get-RowDateValue $row
    if ([string]::IsNullOrWhiteSpace($raw)) { continue }

    $parsed = Try-ParseDateFlexible -Value $raw
    if ($null -eq $parsed) { continue }

    $key = $parsed.ToString("yyyy-MM-dd")
    if (-not $counts.ContainsKey($key)) {
      $counts[$key] = 0
    }

    $counts[$key] = [int]$counts[$key] + 1
  }

  return $counts
}

function Download-BlobByExactDatePrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$Dt,
    [Parameter(Mandatory = $true)][string]$NameRegex,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $false)][bool]$Optional = $false
  )

  $Dt = ($Dt + "").Trim().Trim('"').Trim("'")
  $prefix = "$BasePrefix/dt=$Dt/"
  Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

  $blobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop |
    Where-Object { $_.Name -match $NameRegex }
  )

  if (-not $blobs -or $blobs.Count -eq 0) {
    if ($Optional) {
      Write-Host "ℹ Nenhum arquivo encontrado para '$Label' em '$Container/$prefix'." -ForegroundColor Yellow
      return $null
    }

    throw "Nenhum CSV encontrado para '$Label' em '$Container/$prefix'."
  }

  $blob = $blobs |
    Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending |
    Select-Object -First 1

  Write-Host "📌 Blob encontrado ($Label): $($blob.Name)"
  return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $FinopsTempFolder)
}

function Download-ResourceToServiceMappingFinal {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$Dt,
    [Parameter(Mandatory = $true)][string]$OutFolder
  )

  $Dt = ($Dt + "").Trim().Trim('"').Trim("'")
  $prefix = "$BasePrefix/dt=$Dt/"
  Write-Host "🔎 Procurando mapping final em '$Container/$prefix'..."

  $blobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop |
    Where-Object {
      $leaf = Split-Path $_.Name -Leaf
      $leaf -match '^resource_to_service(_shared)?_.*\.csv$'
    }
  )

  if (-not $blobs -or $blobs.Count -eq 0) {
    throw "Nenhum mapping final resource_to_service encontrado em '$Container/$prefix'."
  }

  $blob = $blobs |
    Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending |
    Select-Object -First 1

  Write-Host "📌 Blob encontrado (resource_to_service): $($blob.Name)"
  return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $OutFolder)
}

function Normalize-Text {
  param([string]$s)

  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  return ($s + "").Trim([char]0xFEFF).Trim()
}

function Normalize-Upper {
  param([string]$s)
  return (Normalize-Text $s).ToUpperInvariant()
}

function Normalize-Key {
  param([string]$s)

  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  return ((Normalize-Text $s).TrimEnd("/")).ToLowerInvariant()
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

function Format-Decimal6 {
  param([double]$Value)
  return $Value.ToString("F3", [System.Globalization.CultureInfo]::InvariantCulture)
}

function Get-RowDateValue {
  param($row)

  if ($null -eq $row) { return "" }

  if ($row.PSObject.Properties.Name -contains "Date") {
    $v = Normalize-Text $row.Date
    if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
  }

  if ($row.PSObject.Properties.Name -contains "UsageDate") {
    $v = Normalize-Text $row.UsageDate
    if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
  }

  return ""
}

function Get-ColumnValue {
  param(
    [Parameter(Mandatory = $true)]$Row,
    [Parameter(Mandatory = $true)][string[]]$CandidateNames
  )

  foreach ($name in $CandidateNames) {
    if ($Row.PSObject.Properties.Name -contains $name) {
      return Normalize-Text $Row.$name
    }
  }

  return ""
}

function Test-IsInvalidDirectClient {
  param([string]$cliente)

  $c = Normalize-Upper $cliente
  if ([string]::IsNullOrWhiteSpace($c)) { return $true }

  $invalid = @(
    "",
    "COMPARTILHADO",
    "SEM TAG",
    "SEMTAG",
    "DEFAULT",
    "UNKNOWN",
    "UNALLOCATED",
    "INFRA_PSL",
    "N/A",
    "NA",
    "NULL"
  )

  return $invalid -contains $c
}

function Extract-ResourceGroupFromResourceId {
  param([string]$resourceId)

  $rid = Normalize-Text $resourceId
  if ([string]::IsNullOrWhiteSpace($rid)) { return "" }

  $m = [regex]::Match($rid, '(?i)/resourcegroups/([^/]+)')
  if ($m.Success) {
    return Normalize-Text $m.Groups[1].Value
  }

  return ""
}

function Extract-ResourceNameFromResourceId {
  param([string]$resourceId)

  $rid = Normalize-Text $resourceId
  if ([string]::IsNullOrWhiteSpace($rid)) { return "" }

  $parts = $rid.TrimEnd("/") -split "/"
  if ($parts.Count -gt 0) {
    return Normalize-Text $parts[$parts.Count - 1]
  }

  return ""
}

function Extract-ResourceTypeFromResourceId {
  param([string]$resourceId)

  $rid = Normalize-Text $resourceId
  if ([string]::IsNullOrWhiteSpace($rid)) { return "" }

  $parts = $rid.Trim("/") -split "/"
  if (-not $parts -or $parts.Count -lt 2) { return "" }

  $providersIdx = -1
  for ($i = 0; $i -lt $parts.Count; $i++) {
    if ($parts[$i].ToLowerInvariant() -eq "providers") {
      $providersIdx = $i
      break
    }
  }

  if ($providersIdx -lt 0) { return "" }
  if (($providersIdx + 2) -ge $parts.Count) { return "" }

  $providerNamespace = $parts[$providersIdx + 1]

  $typeSegments = New-Object System.Collections.Generic.List[string]
  for ($j = $providersIdx + 2; $j -lt $parts.Count; $j += 2) {
    $typeSegments.Add($parts[$j]) | Out-Null
  }

  if ($typeSegments.Count -eq 0) { return $providerNamespace }

  return ("{0}/{1}" -f $providerNamespace, ($typeSegments -join "/"))
}

function Get-RgServiceKeyCandidates {
  param(
    [Parameter(Mandatory = $true)]
    [System.Collections.IEnumerable]$Items
  )

  $result = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)

  foreach ($item in $Items) {
    $rg = Normalize-Text $item.ResourceGroup
    $sk = Normalize-Text $item.ServiceKey

    if ([string]::IsNullOrWhiteSpace($rg)) { continue }
    if ([string]::IsNullOrWhiteSpace($sk)) { continue }

    if (-not $result.ContainsKey($rg)) {
      $result[$rg] = New-Object System.Collections.Generic.List[string]
    }

    if (-not ($result[$rg] -contains $sk)) {
      $result[$rg].Add($sk) | Out-Null
    }
  }

  return $result
}

Ensure-AzLogin
$ctx = Get-StorageContextByName -saName $StorageAccountName
Ensure-Folder -Path $FinopsTempFolder

Write-Host "📅 PipelinePartitionDate informado/resolvido: $PipelinePartitionDate"
Write-Host "📅 UsageDate informado: $(if ([string]::IsNullOrWhiteSpace($UsageDate)) { '<será derivado do CSV>' } else { $UsageDate })"

$requestedPipelinePartitionDate = $PipelinePartitionDate

$factCostPartitionDate = Resolve-BestPartitionDateForInput `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $FactCostPrefix `
  -NameRegex "fact_cost_.*\.csv$|.*fact_cost.*\.csv$" `
  -Label "fact_cost" `
  -PreferredDates @($requestedPipelinePartitionDate)

$sharePartitionDate = Resolve-BestPartitionDateForInput `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $ShareFinalPrefix `
  -NameRegex "allocation_share_servicekey_final_.*\.csv$|allocation_share_.*final.*\.csv$" `
  -Label "allocation_share_servicekey_final" `
  -PreferredDates @($requestedPipelinePartitionDate, $factCostPartitionDate)

$mappingPartitionDate = Resolve-BestPartitionDateForInput `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $ResourceToServicePrefix `
  -NameRegex '^.*resource_to_service(_shared)?_.*\.csv$' `
  -Label "resource_to_service" `
  -PreferredDates @($requestedPipelinePartitionDate, $sharePartitionDate, $factCostPartitionDate)

$inventoryPartitionDate = Resolve-BestPartitionDateForInput `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $InventoryPrefix `
  -NameRegex "inventory.*\.csv$" `
  -Label "inventory" `
  -PreferredDates @($requestedPipelinePartitionDate, $mappingPartitionDate, $sharePartitionDate, $factCostPartitionDate)

if ([string]::IsNullOrWhiteSpace($requestedPipelinePartitionDate)) {
  $PipelinePartitionDate = $sharePartitionDate
}
else {
  $PipelinePartitionDate = $requestedPipelinePartitionDate
}

Write-Host "`n== DOWNLOAD INPUTS =="

$inventoryPath = Download-BlobByExactDatePrefix `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $InventoryPrefix `
  -Dt $inventoryPartitionDate `
  -NameRegex "inventory.*\.csv$" `
  -Label "inventory"

$factCostPath = Download-BlobByExactDatePrefix `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $FactCostPrefix `
  -Dt $factCostPartitionDate `
  -NameRegex "fact_cost_.*\.csv$|.*fact_cost.*\.csv$" `
  -Label "fact_cost"

$mappingPath = Download-ResourceToServiceMappingFinal `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $ResourceToServicePrefix `
  -Dt $mappingPartitionDate `
  -OutFolder $FinopsTempFolder

$sharePath = Download-BlobByExactDatePrefix `
  -Ctx $ctx `
  -Container $FinopsContainer `
  -BasePrefix $ShareFinalPrefix `
  -Dt $sharePartitionDate `
  -NameRegex "allocation_share_servicekey_final_.*\.csv$|allocation_share_.*final.*\.csv$" `
  -Label "allocation_share_servicekey_final"

Write-Host "`n📥 Inventory : $inventoryPath"
Write-Host "📥 FactCost  : $factCostPath"
Write-Host "📥 Mapping   : $mappingPath"
Write-Host "📥 Share     : $sharePath"
Write-Host "📅 Date      : $Date"
Write-Host "📅 PipelinePartitionDate lógico/output : $PipelinePartitionDate"
Write-Host "📅 Input partition - inventory         : $inventoryPartitionDate"
Write-Host "📅 Input partition - fact_cost         : $factCostPartitionDate"
Write-Host "📅 Input partition - resource_mapping  : $mappingPartitionDate"
Write-Host "📅 Input partition - share_final       : $sharePartitionDate"

$inventory = @(Import-Csv $inventoryPath -Delimiter ";")
$factCost  = @(Import-Csv $factCostPath  -Delimiter ";")
$mapping   = @(Import-Csv $mappingPath   -Delimiter ";")
$shares    = @(Import-Csv $sharePath     -Delimiter ";")

if (-not $inventory -or $inventory.Count -eq 0) { throw "inventory vazio." }
if (-not $factCost  -or $factCost.Count -eq 0)  { throw "fact_cost vazio." }
if (-not $mapping   -or $mapping.Count -eq 0)   { throw "resource_to_service vazio." }
if (-not $shares    -or $shares.Count -eq 0)    { throw "allocation_share_servicekey_final vazio." }

if ([string]::IsNullOrWhiteSpace($UsageDate)) {
  $UsageDate = (Get-MaxUsageDateFromRows -Rows $factCost).ToString("yyyy-MM-dd")
}

$factCostDateDistribution = Get-DateDistribution -Rows $factCost
$shareDateDistribution    = Get-DateDistribution -Rows $shares

Write-Host "📅 UsageDate efetivo derivado/aplicado: $UsageDate"
Write-Host "📅 PipelinePartitionDate efetivo      : $PipelinePartitionDate"
Write-Host ("📌 fact_cost - linhas totais          : {0}" -f $factCost.Count)
Write-Host ("📌 share final - linhas totais        : {0}" -f $shares.Count)

if ($factCostDateDistribution.Count -gt 0) {
  Write-Host "📌 Distribuição de datas no fact_cost:"
  foreach ($k in ($factCostDateDistribution.Keys | Sort-Object)) {
    Write-Host ("   - {0}: {1}" -f $k, $factCostDateDistribution[$k])
  }
}

if ($shareDateDistribution.Count -gt 0) {
  Write-Host "📌 Distribuição de datas no share final:"
  foreach ($k in ($shareDateDistribution.Keys | Sort-Object)) {
    Write-Host ("   - {0}: {1}" -f $k, $shareDateDistribution[$k])
  }
}

Write-Host "`n== BUILD INDEXES =="

$inventoryByRid = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)
foreach ($r in $inventory) {
  $rid = Get-ColumnValue -Row $r -CandidateNames @("ResourceId","resourceId","RESOURCEID","Id")
  $ridKey = Normalize-Key $rid
  if ([string]::IsNullOrWhiteSpace($ridKey)) { continue }

  $cliente = Get-ColumnValue -Row $r -CandidateNames @("FINOPS-CLIENTE","Cliente","Client","FINOPS_CLIENTE")
  $rg      = Get-ColumnValue -Row $r -CandidateNames @("ResourceGroup","ResourceGroupName","RG","resourceGroup")
  $rtype   = Get-ColumnValue -Row $r -CandidateNames @("ResourceType","Type","resourceType")
  $rname   = Get-ColumnValue -Row $r -CandidateNames @("ResourceName","Name","resourceName")

  if ([string]::IsNullOrWhiteSpace($rg))    { $rg    = Extract-ResourceGroupFromResourceId $rid }
  if ([string]::IsNullOrWhiteSpace($rtype)) { $rtype = Extract-ResourceTypeFromResourceId $rid }
  if ([string]::IsNullOrWhiteSpace($rname)) { $rname = Extract-ResourceNameFromResourceId $rid }

  $inventoryByRid[$ridKey] = [PSCustomObject]@{
    ResourceId     = $rid
    ClienteTag     = $cliente
    ResourceGroup  = $rg
    ResourceType   = $rtype
    ResourceName   = $rname
  }
}

$ridToServiceKey = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)
$mappingRgRows = New-Object System.Collections.Generic.List[object]

foreach ($m in $mapping) {
  $rid = Get-ColumnValue -Row $m -CandidateNames @("ResourceId","resourceId","RESOURCEID","Id")
  $sk  = Get-ColumnValue -Row $m -CandidateNames @("ServiceKey","servicekey","SERVICEKEY")
  $rg  = Get-ColumnValue -Row $m -CandidateNames @("ResourceGroup","ResourceGroupName","RG","resourceGroup")

  $ridKey = Normalize-Key $rid
  if (-not [string]::IsNullOrWhiteSpace($ridKey) -and -not [string]::IsNullOrWhiteSpace($sk)) {
    $ridToServiceKey[$ridKey] = $sk
  }

  if ([string]::IsNullOrWhiteSpace($rg)) {
    $rg = Extract-ResourceGroupFromResourceId $rid
  }

  if (-not [string]::IsNullOrWhiteSpace($rg) -and -not [string]::IsNullOrWhiteSpace($sk)) {
    $mappingRgRows.Add([PSCustomObject]@{
      ResourceGroup = $rg
      ServiceKey    = $sk
    }) | Out-Null
  }
}

$rgToServiceKeyCandidates = Get-RgServiceKeyCandidates -Items $mappingRgRows

$rgToSingleServiceKey = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)
$rgToMultiServiceKey  = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)

foreach ($rg in $rgToServiceKeyCandidates.Keys) {
  $list = $rgToServiceKeyCandidates[$rg]
  if ($list.Count -eq 1) {
    $rgToSingleServiceKey[$rg] = $list[0]
  }
  elseif ($list.Count -gt 1) {
    $rgToMultiServiceKey[$rg] = (($list | Sort-Object) -join "|")
  }
}

$shareByServiceKey = New-Object 'System.Collections.Hashtable' ([System.StringComparer]::OrdinalIgnoreCase)

$shareRowsForUsageDate = @(
  $shares | Where-Object { (Get-RowDateValue $_) -eq $UsageDate }
)

$shareDateStrategy = "MATCH_USAGE_DATE"

if ($shareRowsForUsageDate.Count -eq 0) {
  Write-Warning ("Nenhuma linha de share encontrada com Date/UsageDate = {0}. Será aplicado fallback para o conteúdo do arquivo selecionado da partição {1}." -f $UsageDate, $PipelinePartitionDate)
  $shareRowsForUsageDate = @($shares)
  $shareDateStrategy = "FULL_FILE_FALLBACK_NORMALIZED_TO_USAGE_DATE"
}

foreach ($s in $shareRowsForUsageDate) {
  $serviceKey     = Get-ColumnValue -Row $s -CandidateNames @("ServiceKey","servicekey","SERVICEKEY")
  $cliente        = Get-ColumnValue -Row $s -CandidateNames @("Cliente","Client","CLIENTE")
  $driverType     = Get-ColumnValue -Row $s -CandidateNames @("DriverType","Driver","DRIVERTYPE")
  $allocationMode = Get-ColumnValue -Row $s -CandidateNames @("AllocationMode","Mode","ALLOCATIONMODE")
  $notes          = Get-ColumnValue -Row $s -CandidateNames @("Notes","Comment","COMMENTS","Observacao","Observações")
  $share          = Parse-DoubleInvariant (Get-ColumnValue -Row $s -CandidateNames @("Share","SHARE"))

  if ([string]::IsNullOrWhiteSpace($serviceKey)) { continue }
  if ([string]::IsNullOrWhiteSpace($cliente))    { continue }
  if ($share -le 0)                              { continue }

  if (-not $shareByServiceKey.ContainsKey($serviceKey)) {
    $shareByServiceKey[$serviceKey] = New-Object System.Collections.Generic.List[object]
  }

  $normalizedNotes = $notes
  if ($shareDateStrategy -ne "MATCH_USAGE_DATE") {
    if ([string]::IsNullOrWhiteSpace($normalizedNotes)) {
      $normalizedNotes = "share_date_normalized_from_pipeline_partition"
    }
    else {
      $normalizedNotes = "{0} | share_date_normalized_from_pipeline_partition" -f $normalizedNotes
    }
  }

  $shareByServiceKey[$serviceKey].Add([PSCustomObject]@{
    Date           = $UsageDate
    Cliente        = $cliente
    Share          = [double]$share
    DriverType     = $driverType
    AllocationMode = $allocationMode
    Notes          = $normalizedNotes
  }) | Out-Null
}

Write-Host ("📌 Inventory index            : {0}" -f $inventoryByRid.Count)
Write-Host ("📌 Mapping index (RID)        : {0}" -f $ridToServiceKey.Count)
Write-Host ("📌 RG únicos -> single SK     : {0}" -f $rgToSingleServiceKey.Count)
Write-Host ("📌 RG ambíguos -> múltiplas SK: {0}" -f $rgToMultiServiceKey.Count)
Write-Host ("📌 ServiceKeys com share      : {0}" -f $shareByServiceKey.Count)
Write-Host ("📌 Estratégia de data do share: {0}" -f $shareDateStrategy)

Write-Host "`n== ALLOCATE COST =="

$factRowsForUsageDateCount = @($factCost | Where-Object { (Get-RowDateValue $_) -eq $UsageDate }).Count
$shareRowsForUsageDateCount = $shareRowsForUsageDate.Count

Write-Host ("📌 FactCost na UsageDate      : {0}" -f $factRowsForUsageDateCount)
Write-Host ("📌 Shares considerados        : {0}" -f $shareRowsForUsageDateCount)

$factAllocated = New-Object System.Collections.Generic.List[object]
$unallocated   = New-Object System.Collections.Generic.List[object]

[int]$countServiceKeyByRid = 0
[int]$countServiceKeyByRg = 0
[int]$countRgAmbiguous = 0
[int]$countNoMapping = 0

foreach ($c in $factCost) {
  $rowDate = Get-RowDateValue $c
  if ($rowDate -ne $UsageDate) { continue }

  $rid  = Get-ColumnValue -Row $c -CandidateNames @("ResourceId","resourceId","RESOURCEID","Id")
  $cost = Parse-DoubleInvariant (Get-ColumnValue -Row $c -CandidateNames @("Cost","FinalCost","AmortizedCost","Custo"))

  if ([string]::IsNullOrWhiteSpace($rid)) { continue }
  if ($cost -eq 0) { continue }

  $ridKey = Normalize-Key $rid

  $inv = $null
  if ($inventoryByRid.ContainsKey($ridKey)) {
    $inv = $inventoryByRid[$ridKey]
  }

  $clienteTag    = ""
  $resourceGroup = ""
  $resourceType  = ""
  $resourceName  = ""

  if ($null -ne $inv) {
    $clienteTag    = Normalize-Text $inv.ClienteTag
    $resourceGroup = Normalize-Text $inv.ResourceGroup
    $resourceType  = Normalize-Text $inv.ResourceType
    $resourceName  = Normalize-Text $inv.ResourceName
  }

  if ([string]::IsNullOrWhiteSpace($resourceGroup)) { $resourceGroup = Extract-ResourceGroupFromResourceId $rid }
  if ([string]::IsNullOrWhiteSpace($resourceType))  { $resourceType  = Extract-ResourceTypeFromResourceId $rid }
  if ([string]::IsNullOrWhiteSpace($resourceName))  { $resourceName  = Extract-ResourceNameFromResourceId $rid }

  $serviceKey = ""
  $serviceKeySource = ""

  if ($ridToServiceKey.ContainsKey($ridKey)) {
    $serviceKey = Normalize-Text $ridToServiceKey[$ridKey]
    $serviceKeySource = "RESOURCEID"
    $countServiceKeyByRid++
  }
  elseif (-not [string]::IsNullOrWhiteSpace($resourceGroup) -and $rgToSingleServiceKey.ContainsKey($resourceGroup)) {
    $serviceKey = Normalize-Text $rgToSingleServiceKey[$resourceGroup]
    $serviceKeySource = "RG_FALLBACK"
    $countServiceKeyByRg++
  }
  elseif (-not [string]::IsNullOrWhiteSpace($resourceGroup) -and $rgToMultiServiceKey.ContainsKey($resourceGroup)) {
    $serviceKey = ""
    $serviceKeySource = "RG_AMBIGUOUS"
    $countRgAmbiguous++
  }
  else {
    $countNoMapping++
  }

  $hasValidDirectClient  = -not (Test-IsInvalidDirectClient $clienteTag)
  $hasShareForServiceKey = (-not [string]::IsNullOrWhiteSpace($serviceKey)) -and $shareByServiceKey.ContainsKey($serviceKey)

  if ($hasValidDirectClient -and -not $hasShareForServiceKey) {
    $factAllocated.Add([PSCustomObject]@{
      Date              = $UsageDate
      Cliente           = $clienteTag
      ResourceId        = $rid
      ResourceName      = $resourceName
      ResourceType      = $resourceType
      ResourceGroup     = $resourceGroup
      ResourceGroupName = $resourceGroup
      ServiceKey        = $serviceKey
      ServiceKeySource  = $serviceKeySource
      AllocationType    = "DIRECT"
      DriverType        = "DIRECT_TAG"
      AllocationMode    = "DIRECT"
      Cost              = [Math]::Round($cost, 6)
      OriginalCost      = [Math]::Round($cost, 6)
      Share             = 1.0
      AllocatedCost     = [Math]::Round($cost, 6)
      Notes             = "DIRECT_VALID_CLIENT_NO_SHARE"
    }) | Out-Null

    continue
  }

  if ($hasShareForServiceKey) {
    $shareRows = $shareByServiceKey[$serviceKey]

    foreach ($sr in $shareRows) {
      $allocatedCost = [Math]::Round(($cost * [double]$sr.Share), 6)
      if ($allocatedCost -eq 0) { continue }

      $factAllocated.Add([PSCustomObject]@{
        Date              = $UsageDate
        Cliente           = $sr.Cliente
        ResourceId        = $rid
        ResourceName      = $resourceName
        ResourceType      = $resourceType
        ResourceGroup     = $resourceGroup
        ResourceGroupName = $resourceGroup
        ServiceKey        = $serviceKey
        ServiceKeySource  = $serviceKeySource
        AllocationType    = "SHARED"
        DriverType        = $sr.DriverType
        AllocationMode    = $sr.AllocationMode
        Cost              = [Math]::Round($cost, 6)
        OriginalCost      = [Math]::Round($cost, 6)
        Share             = [Math]::Round([double]$sr.Share, 6)
        AllocatedCost     = $allocatedCost
        Notes             = $sr.Notes
      }) | Out-Null
    }

    continue
  }

  $reason = ""
  if ([string]::IsNullOrWhiteSpace($serviceKey)) {
    if ($serviceKeySource -eq "RG_AMBIGUOUS") {
      $reason = "RG_MULTIPLE_SERVICEKEY_CANDIDATES"
    }
    else {
      $reason = "NO_MAPPING_BY_RESOURCEID_OR_RG"
    }
  }
  elseif (-not $hasShareForServiceKey) {
    if ($hasValidDirectClient) {
      $reason = "VALID_DIRECT_CLIENT_BUT_SERVICEKEY_WITHOUT_SHARE"
    }
    else {
      $reason = "HAS_SERVICEKEY_NO_SHARE"
    }
  }
  else {
    $reason = "UNCLASSIFIED"
  }

  $unallocated.Add([PSCustomObject]@{
    Date              = $UsageDate
    ResourceId        = $rid
    ResourceName      = $resourceName
    ResourceType      = $resourceType
    ResourceGroup     = $resourceGroup
    ResourceGroupName = $resourceGroup
    ServiceKey        = $serviceKey
    ServiceKeySource  = $serviceKeySource
    ClienteTag        = $clienteTag
    Cost              = [Math]::Round($cost, 6)
    Reason            = $reason
  }) | Out-Null
}

$factCostUsageDateCount = @($factCost | Where-Object { (Get-RowDateValue $_) -eq $UsageDate }).Count
$sharesUsageDateCount   = @($shares   | Where-Object { (Get-RowDateValue $_) -eq $UsageDate }).Count

Write-Host ("📌 FactCost na UsageDate      : {0}" -f $factCostUsageDateCount)
Write-Host ("📌 Shares na UsageDate        : {0}" -f $sharesUsageDateCount)
Write-Host ("📌 Linhas fact_allocated_cost : {0}" -f $factAllocated.Count)
Write-Host ("📌 Linhas unallocated         : {0}" -f $unallocated.Count)

if ($factAllocated.Count -eq 0 -and $unallocated.Count -eq 0) {
  throw ("Nenhuma linha foi gerada em fact_allocated_cost nem em unallocated. UsageDate={0}; FactCostRows={1}; ShareRowsConsiderados={2}; ServiceKeysComShare={3}. Verifique filtros de UsageDate, ResourceId, mapping e share final." -f $UsageDate, $factRowsForUsageDateCount, $shareRowsForUsageDateCount, $shareByServiceKey.Count)
}

if ($factAllocated.Count -eq 0 -and $unallocated.Count -gt 0) {
  Write-Warning "Nenhuma linha foi gerada em fact_allocated_cost. Todos os custos ficaram em unallocated."
}

Write-Host "`n== LOOKUP SUMMARY =="
Write-Host ("📌 ServiceKey por ResourceId  : {0}" -f $countServiceKeyByRid)
Write-Host ("📌 ServiceKey por RG fallback : {0}" -f $countServiceKeyByRg)
Write-Host ("📌 RG ambíguo                 : {0}" -f $countRgAmbiguous)
Write-Host ("📌 Sem mapping                : {0}" -f $countNoMapping)

Write-Host "`n== BUILD FINAL AGGREGATIONS =="

$finalDetailed = @(
  $factAllocated |
  ForEach-Object {
    [PSCustomObject]@{
      Date              = $_.Date
      Cliente           = $_.Cliente
      ResourceId        = $_.ResourceId
      ResourceName      = $_.ResourceName
      ResourceType      = $_.ResourceType
      ResourceGroup     = $_.ResourceGroup
      ResourceGroupName = $_.ResourceGroupName
      ServiceKey        = $_.ServiceKey
      ServiceKeySource  = $_.ServiceKeySource
      AllocationType    = $_.AllocationType
      DriverType        = $_.DriverType
      AllocationMode    = $_.AllocationMode
      OriginalCost      = $_.OriginalCost
      Cost              = $_.Cost
      Share             = $_.Share
      AllocatedCost     = $_.AllocatedCost
      Notes             = $_.Notes
    }
  }
)

$finalByClient = @(
  $factAllocated |
  Group-Object Date, Cliente |
  ForEach-Object {
    $first = $_.Group[0]
    $allocatedCost = [double](($_.Group | Measure-Object AllocatedCost -Sum).Sum)

    [PSCustomObject]@{
      Date                 = $first.Date
      Cliente              = $first.Cliente
      AllocatedCost        = [Math]::Round($allocatedCost, 6)
      Rows                 = $_.Count
      AllocationPercentage = 0.0
    }
  }
)

$totalFactCost = 0.0
$factRowsForDate = $factCost | Where-Object { (Get-RowDateValue $_) -eq $UsageDate }

foreach ($r in $factRowsForDate) {
  $value = Parse-DoubleInvariant (Get-ColumnValue -Row $r -CandidateNames @("Cost","FinalCost","AmortizedCost","Custo"))
  $totalFactCost += $value
}

$totalAllocated = [double](($factAllocated | Measure-Object AllocatedCost -Sum).Sum)
$totalUnallocated = [double](($unallocated | Measure-Object Cost -Sum).Sum)

$reconciliation = @(
  [PSCustomObject]@{
    Date             = $UsageDate
    TotalFactCost    = [Math]::Round($totalFactCost, 6)
    TotalAllocated   = [Math]::Round($totalAllocated, 6)
    TotalUnallocated = [Math]::Round($totalUnallocated, 6)
    Difference       = [Math]::Round(($totalFactCost - ($totalAllocated + $totalUnallocated)), 6)
  }
)

if ($totalFactCost -ne 0) {
  foreach ($row in $factAllocated) {
    $row | Add-Member -NotePropertyName AllocationPercentage -NotePropertyValue ([Math]::Round((([double]$row.AllocatedCost / [double]$totalFactCost) * 100), 3)) -Force
  }

  foreach ($row in $finalDetailed) {
    $row | Add-Member -NotePropertyName AllocationPercentage -NotePropertyValue ([Math]::Round((([double]$row.AllocatedCost / [double]$totalFactCost) * 100), 3)) -Force
  }

  foreach ($row in $finalByClient) {
    $row.AllocationPercentage = [Math]::Round((([double]$row.AllocatedCost / [double]$totalFactCost) * 100), 3)
  }
}
else {
  foreach ($row in $factAllocated) {
    $row | Add-Member -NotePropertyName AllocationPercentage -NotePropertyValue 0.0 -Force
  }

  foreach ($row in $finalDetailed) {
    $row | Add-Member -NotePropertyName AllocationPercentage -NotePropertyValue 0.0 -Force
  }

  foreach ($row in $finalByClient) {
    $row.AllocationPercentage = 0.0
  }
}

Write-Host "`n== VALIDATION =="

Write-Host ("📌 Total fact_cost    : {0}" -f ([Math]::Round($totalFactCost, 6)))
Write-Host ("📌 Total alocado      : {0}" -f ([Math]::Round($totalAllocated, 6)))
Write-Host ("📌 Total unallocated  : {0}" -f ([Math]::Round($totalUnallocated, 6)))
Write-Host ("📌 Diferença          : {0}" -f ([Math]::Round(($totalFactCost - ($totalAllocated + $totalUnallocated)), 6)))

if ($unallocated.Count -gt 0) {
  Write-Host "`n⚠ Recursos sem alocação encontrados:" -ForegroundColor Yellow
  $unallocated | Select-Object -First 20 | Format-Table -AutoSize
}

Write-Host "`n== EXPORT =="

$factAllocatedPath = Join-Path $FinopsTempFolder ("fact_allocated_cost_{0}.csv" -f $UsageDate)
$finalByClientPath = Join-Path $FinopsTempFolder ("final_cost_by_client_{0}.csv" -f $UsageDate)
$finalDetailedPath = Join-Path $FinopsTempFolder ("final_cost_detailed_{0}.csv" -f $UsageDate)
$reconPath         = Join-Path $FinopsTempFolder ("reconciliation_{0}.csv" -f $UsageDate)
$unallocatedPath   = Join-Path $FinopsTempFolder ("unallocated_cost_{0}.csv" -f $UsageDate)

$factAllocatedExport = @(
  $factAllocated |
  Sort-Object Cliente, ServiceKey, ResourceId |
  Select-Object `
    Date,
    Cliente,
    ResourceId,
    ResourceName,
    ResourceType,
    ResourceGroup,
    ResourceGroupName,
    ServiceKey,
    ServiceKeySource,
    AllocationType,
    DriverType,
    AllocationMode,
    @{Name='OriginalCost';Expression={ Format-Decimal6 ([double]$_.OriginalCost) }},
    @{Name='Cost';Expression={ Format-Decimal6 ([double]$_.Cost) }},
    @{Name='Share';Expression={ Format-Decimal6 ([double]$_.Share) }},
    @{Name='AllocatedCost';Expression={ Format-Decimal6 ([double]$_.AllocatedCost) }},
    @{Name='AllocationPercentage';Expression={ Format-Decimal6 ([double]$_.AllocationPercentage) }},
    Notes
)

$factAllocatedExport |
  Export-Csv -Path $factAllocatedPath -Delimiter ";" -NoTypeInformation -Encoding UTF8

$finalByClientExport = @(
  $finalByClient |
  Sort-Object Cliente |
  Select-Object `
    Date,
    Cliente,
    @{Name='AllocatedCost';Expression={ Format-Decimal6 ([double]$_.AllocatedCost) }},
    @{Name='AllocationPercentage';Expression={ Format-Decimal6 ([double]$_.AllocationPercentage) }},
    Rows
)

$finalByClientExport |
  Export-Csv -Path $finalByClientPath -Delimiter ";" -NoTypeInformation -Encoding UTF8

$finalDetailedExport = @(
  $finalDetailed |
  Sort-Object Cliente, ServiceKey, ResourceId |
  Select-Object `
    Date,
    Cliente,
    ResourceId,
    ResourceName,
    ResourceType,
    ResourceGroup,
    ResourceGroupName,
    ServiceKey,
    ServiceKeySource,
    AllocationType,
    DriverType,
    AllocationMode,
    @{Name='OriginalCost';Expression={ Format-Decimal6 ([double]$_.OriginalCost) }},
    @{Name='Cost';Expression={ Format-Decimal6 ([double]$_.Cost) }},
    @{Name='Share';Expression={ Format-Decimal6 ([double]$_.Share) }},
    @{Name='AllocatedCost';Expression={ Format-Decimal6 ([double]$_.AllocatedCost) }},
    @{Name='AllocationPercentage';Expression={ Format-Decimal6 ([double]$_.AllocationPercentage) }},
    Notes
)

$finalDetailedExport |
  Export-Csv -Path $finalDetailedPath -Delimiter ";" -NoTypeInformation -Encoding UTF8

$reconciliationExport = @(
  $reconciliation |
  Select-Object `
    Date,
    @{Name='TotalFactCost';Expression={ Format-Decimal6 ([double]$_.TotalFactCost) }},
    @{Name='TotalAllocated';Expression={ Format-Decimal6 ([double]$_.TotalAllocated) }},
    @{Name='TotalUnallocated';Expression={ Format-Decimal6 ([double]$_.TotalUnallocated) }},
    @{Name='Difference';Expression={ Format-Decimal6 ([double]$_.Difference) }}
)

$reconciliationExport |
  Export-Csv -Path $reconPath -Delimiter ";" -NoTypeInformation -Encoding UTF8

$unallocatedExport = @(
  $unallocated |
  Sort-Object Reason, ClienteTag, ServiceKey |
  Select-Object `
    Date,
    ResourceId,
    ResourceName,
    ResourceType,
    ResourceGroup,
    ResourceGroupName,
    ServiceKey,
    ServiceKeySource,
    ClienteTag,
    @{Name='Cost';Expression={ Format-Decimal6 ([double]$_.Cost) }},
    Reason
)

$unallocatedExport |
  Export-Csv -Path $unallocatedPath -Delimiter ";" -NoTypeInformation -Encoding UTF8

Write-Host "✅ fact_allocated_cost : $factAllocatedPath"
Write-Host "✅ final_cost_by_client: $finalByClientPath"
Write-Host "✅ final_cost_detailed : $finalDetailedPath"
Write-Host "✅ reconciliation      : $reconPath"
Write-Host "✅ unallocated_cost    : $unallocatedPath"

$blobFact     = "$OutPrefixFact/dt=$PipelinePartitionDate/$(Split-Path $factAllocatedPath -Leaf)"
$blobFinal    = "$OutPrefixFinalByClient/dt=$PipelinePartitionDate/$(Split-Path $finalByClientPath -Leaf)"
$blobDetailed = "$OutPrefixFinalDetailed/dt=$PipelinePartitionDate/$(Split-Path $finalDetailedPath -Leaf)"
$blobRecon    = "$OutPrefixReconciliation/dt=$PipelinePartitionDate/$(Split-Path $reconPath -Leaf)"
$blobUnall    = "$OutPrefixUnallocated/dt=$PipelinePartitionDate/$(Split-Path $unallocatedPath -Leaf)"

Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $factAllocatedPath -BlobPath $blobFact
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $finalByClientPath -BlobPath $blobFinal
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $finalDetailedPath -BlobPath $blobDetailed
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $reconPath -BlobPath $blobRecon
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $unallocatedPath -BlobPath $blobUnall

Write-Host "⬆️ Upload:"
Write-Host " - $FinopsContainer/$blobFact"
Write-Host " - $FinopsContainer/$blobFinal"
Write-Host " - $FinopsContainer/$blobDetailed"
Write-Host " - $FinopsContainer/$blobRecon"
Write-Host " - $FinopsContainer/$blobUnall"

Write-Host "`n== SUMMARY =="
$finalByClient | Sort-Object AllocatedCost -Descending | Format-Table -AutoSize
