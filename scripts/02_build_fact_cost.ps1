<#!
.SYNOPSIS
  Script 02 - Build fact_cost for FinOps Allocation Pipeline (Azure Automation Runbook)

.DESCRIPTION
  - Compatible with Azure Automation Runbook
  - Uses Managed Identity
  - Sets explicit subscription context
  - Chooses the correct Cost Export CSV by CONTENT, not by LastModified
  - Ignores empty/invalid CSVs
  - Traverses recent periods until it finds a valid export
  - Does NOT force D-1
  - Uses Brazil timezone for PipelinePartitionDate in Azure Automation
  - When UsageDate is not provided, derives it from the selected file's MaxUsageDate
  - Standardizes generated fact_cost CSV with ';' delimiter and UTF-8, preserving business logic
#>

param(
  [Parameter(Mandatory=$false)]
  [string]$Date,

  [Parameter(Mandatory=$false)]
  [string]$PipelineDate,

  [Parameter(Mandatory=$false)]
  [string]$UsageDate,

  [Parameter(Mandatory=$false)]
  [string]$SubscriptionId = "52d4423b-7ed9-4673-b8e2-fa21cdb83176",

  [Parameter(Mandatory=$false)]
  [string]$StorageAccountName = "stpslkmmfinopseusprd",

  [Parameter(Mandatory=$false)]
  [string]$StorageAccountResourceGroupName = "rg-psl-kmm-finops-eus-prd",

  [Parameter(Mandatory=$false)]
  [string]$CostContainer = "finops-cost-exports",

  [Parameter(Mandatory=$false)]
  [string]$CostPrefix = "kmm/finops-cost-export/",

  [Parameter(Mandatory=$false)]
  [string]$FinopsContainer = "finops",

  [Parameter(Mandatory=$false)]
  [string]$FactCostPrefix = "silver/fact_cost",

  [Parameter(Mandatory=$false)]
  [string]$FinopsTempFolder = "/tmp/finops_rateio",

  [Parameter(Mandatory=$false)]
  [int]$MaxPeriodsToInspect = 6
)

$ErrorActionPreference = "Stop"

function Get-BrazilNow {
  $tzIds = @('E. South America Standard Time','America/Sao_Paulo')
  $tz = $null

  foreach ($tzId in $tzIds) {
    try {
      $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($tzId)
      if ($tz) { break }
    }
    catch {
    }
  }

  if (-not $tz) {
    throw "Não foi possível localizar o timezone do Brasil para o pipeline."
  }

  return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

if (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
  $Date = $PipelineDate
}
elseif ([string]::IsNullOrWhiteSpace($Date)) {
  $Date = (Get-BrazilNow).ToString('yyyy-MM-dd')
}

$InternalCsvDelimiter = ';'

function Write-Log {
  param(
    [string]$Message,
    [ValidateSet('INFO','WARN','ERROR')][string]$Level = 'INFO'
  )
  $prefix = switch ($Level) {
    'INFO'  { '[INFO ]' }
    'WARN'  { '[WARN ]' }
    'ERROR' { '[ERROR]' }
  }
  Write-Host "$prefix $Message"
}

function Get-AutomationVariableSafe {
  param([Parameter(Mandatory=$true)][string]$Name)

  try {
    $cmd = Get-Command -Name Get-AutomationVariable -ErrorAction SilentlyContinue
    if ($cmd) {
      return Get-AutomationVariable -Name $Name
    }
  }
  catch {
    Write-Log "Não foi possível ler Automation Variable '$Name': $($_.Exception.Message)" 'WARN'
  }

  return $null
}

function Resolve-Setting {
  param(
    [AllowNull()]
    [AllowEmptyString()]
    [string]$Value,

    [Parameter(Mandatory=$true)]
    [string]$AutomationVariableName,

    [Parameter(Mandatory=$true)]
    [string]$Label
  )

  if (-not [string]::IsNullOrWhiteSpace($Value)) {
    return $Value
  }

  $autoValue = Get-AutomationVariableSafe -Name $AutomationVariableName
  if (-not [string]::IsNullOrWhiteSpace($autoValue)) {
    return $autoValue
  }

  throw "Parâmetro obrigatório não informado: $Label. Informe no runbook ou crie a Automation Variable '$AutomationVariableName'."
}


function Resolve-TempFolder {
  param(
    [AllowNull()]
    [AllowEmptyString()]
    [string]$PreferredPath
  )

  if (-not [string]::IsNullOrWhiteSpace($PreferredPath)) {
    return $PreferredPath
  }

  $candidates = @(
    $env:TEMP,
    $env:TMP,
    '/tmp/finops_rateio',
    'C:\Temp\finops_rateio'
  ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

  foreach ($candidate in $candidates) {
    try {
      if (-not (Test-Path -LiteralPath $candidate)) {
        New-Item -ItemType Directory -Path $candidate -Force | Out-Null
      }
      return $candidate
    }
    catch {
    }
  }

  throw "Não foi possível resolver uma pasta temporária válida para o runbook."
}


function Ensure-AzLogin {
  param([string]$SubscriptionId)

  $ctx = Get-AzContext -ErrorAction SilentlyContinue
  if (-not $ctx) {
    Write-Log "Autenticando com Managed Identity..."
    Connect-AzAccount -Identity | Out-Null
  }

  if (-not [string]::IsNullOrWhiteSpace($SubscriptionId)) {
    Write-Log "Ajustando contexto para subscription: $SubscriptionId"
    Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
  }

  $finalCtx = Get-AzContext
  if (-not $finalCtx) {
    throw "Falha ao obter contexto Azure após autenticação."
  }

  Write-Log "Contexto Azure ativo: SubscriptionId=$($finalCtx.Subscription.Id) | Account=$($finalCtx.Account.Id)"
}

function Ensure-Folder {
  param([Parameter(Mandatory=$true)][string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
  }
}


function Combine-PathSafe {
  param(
    [AllowNull()][AllowEmptyString()][string]$BasePath,
    [AllowNull()][AllowEmptyString()][string]$ChildPath,
    [string]$Label = 'caminho'
  )

  if ([string]::IsNullOrWhiteSpace($BasePath)) {
    throw "BasePath inválido ao montar $Label."
  }
  if ([string]::IsNullOrWhiteSpace($ChildPath)) {
    throw "ChildPath inválido ao montar $Label."
  }

  return [System.IO.Path]::Combine($BasePath, $ChildPath)
}

function Get-FileNameSafe {
  param(
    [AllowNull()][AllowEmptyString()][string]$Path,
    [string]$Label = 'arquivo'
  )

  if ([string]::IsNullOrWhiteSpace($Path)) {
    throw "Path inválido ao resolver nome de $Label."
  }

  $name = [System.IO.Path]::GetFileName($Path)
  if ([string]::IsNullOrWhiteSpace($name)) {
    throw "Não foi possível extrair o nome do $Label a partir do path: $Path"
  }

  return $name
}

function Resolve-ColumnName {
  param($Headers, [string[]]$Candidates)

  foreach ($c in $Candidates) {
    if ($Headers -contains $c) { return $c }
  }

  foreach ($c in $Candidates) {
    $hit = $Headers | Where-Object { $_.ToLowerInvariant() -eq $c.ToLowerInvariant() } | Select-Object -First 1
    if ($hit) { return $hit }
  }

  return $null
}


function ConvertTo-InvariantDecimal {
  param(
    [AllowNull()]
    [AllowEmptyString()]
    [string]$Value
  )

  $inv = [System.Globalization.CultureInfo]::InvariantCulture

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return [double]0
  }

  $normalized = $Value.Trim().Trim('"').Replace(' ', '')
  if ([string]::IsNullOrWhiteSpace($normalized)) {
    return [double]0
  }

  $hasComma = $normalized.Contains(',')
  $hasDot = $normalized.Contains('.')

  if ($hasComma -and $hasDot) {
    $lastComma = $normalized.LastIndexOf(',')
    $lastDot = $normalized.LastIndexOf('.')

    if ($lastComma -gt $lastDot) {
      $normalized = $normalized.Replace('.', '')
      $normalized = $normalized.Replace(',', '.')
    }
    else {
      $normalized = $normalized.Replace(',', '')
    }
  }
  elseif ($hasComma) {
    $normalized = $normalized.Replace(',', '.')
  }

  [double]$parsed = 0
  $styles = [System.Globalization.NumberStyles]::Float
  $ok = [double]::TryParse($normalized, $styles, $inv, [ref]$parsed)

  if (-not $ok) {
    throw "Não foi possível converter valor de custo '$Value' para decimal invariant. Valor normalizado: '$normalized'."
  }

  return [math]::Round($parsed, 6, [System.MidpointRounding]::AwayFromZero)
}

function Get-StorageContextByName {
  param(
    [Parameter(Mandatory=$true)][string]$StorageAccountName,
    [Parameter(Mandatory=$true)][string]$ResourceGroupName
  )

  $sa = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
  if (-not $sa) {
    throw "Storage Account não encontrado: RG='$ResourceGroupName' Name='$StorageAccountName'"
  }

  return $sa.Context
}

function Download-Blob {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$BlobName,
    [Parameter(Mandatory=$true)][string]$OutFolder
  )

  Ensure-Folder -Path $OutFolder
  $leaf = Get-FileNameSafe -Path $BlobName -Label 'blob'
  $safeName = if ([string]::IsNullOrWhiteSpace($leaf)) { [guid]::NewGuid().ToString() + '.csv' } else { $leaf }
  $outPath = Combine-PathSafe -BasePath $OutFolder -ChildPath $safeName -Label 'download do blob'

  Get-AzStorageBlobContent -Context $Ctx -Container $Container -Blob $BlobName -Destination $outPath -Force | Out-Null
  return $outPath
}

function Upload-ToBlob {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$LocalPath,
    [Parameter(Mandatory=$true)][string]$BlobPath
  )

  Set-AzStorageBlobContent -Context $Ctx -Container $Container -File $LocalPath -Blob $BlobPath -Force | Out-Null
}

function Get-CsvAnalysis {
  param(
    [Parameter(Mandatory=$true)][string]$CsvPath
  )

  $result = [ordered]@{
    CsvPath         = $CsvPath
    FileSizeBytes   = 0
    Delimiter       = $null
    RowCount        = 0
    Headers         = @()
    DateColumn      = $null
    ResourceIdColumn= $null
    CostColumn      = $null
    SampleDates     = @()
    MaxUsageDate    = $null
    IsValid         = $false
    Reason          = $null
  }

  try {
    if (-not (Test-Path -LiteralPath $CsvPath)) {
      $result.Reason = 'Arquivo local não encontrado.'
      return [PSCustomObject]$result
    }

    $fileInfo = Get-Item -LiteralPath $CsvPath
    $result.FileSizeBytes = [int64]$fileInfo.Length

    if ($fileInfo.Length -le 0) {
      $result.Reason = 'Arquivo vazio.'
      return [PSCustomObject]$result
    }

    $firstLine = Get-Content -LiteralPath $CsvPath -TotalCount 1
    if ([string]::IsNullOrWhiteSpace($firstLine)) {
      $result.Reason = 'Primeira linha vazia.'
      return [PSCustomObject]$result
    }

    $delimiter = if (($firstLine -split ';').Count -gt ($firstLine -split ',').Count) { ';' } else { ',' }
    $result.Delimiter = $delimiter

    $rows = Import-Csv -Path $CsvPath -Delimiter $delimiter
    if (-not $rows) {
      $result.Reason = 'Import-Csv não retornou linhas.'
      return [PSCustomObject]$result
    }

    $result.RowCount = @($rows).Count
    if ($result.RowCount -eq 0) {
      $result.Reason = 'CSV sem linhas de dados.'
      return [PSCustomObject]$result
    }

    $headers = @($rows[0].PSObject.Properties.Name)
    $result.Headers = $headers

    $dateCol = Resolve-ColumnName $headers @('UsageDateTime','Usage DateTime','UsageDate','Usage Date','Date')
    $resourceCol = Resolve-ColumnName $headers @('ResourceId','Resource Id','resourceId','ResourceID','InstanceId','Instance Id','instanceId','InstanceID')
    $costCol = Resolve-ColumnName $headers @('PreTaxCost','PreTax Cost','CostInBillingCurrency','Cost In Billing Currency','Cost')

    $result.DateColumn = $dateCol
    $result.ResourceIdColumn = $resourceCol
    $result.CostColumn = $costCol

    if (-not $dateCol) {
      $result.Reason = 'Coluna de data não encontrada.'
      return [PSCustomObject]$result
    }

    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $samples = New-Object System.Collections.Generic.List[string]
    $maxDt = $null

    foreach ($r in $rows) {
      $d = [string]$r.$dateCol
      if ([string]::IsNullOrWhiteSpace($d)) { continue }

      if ($samples.Count -lt 3) {
        $samples.Add($d)
      }

      $dtParsed = $null
      try {
        $dto = [DateTimeOffset]::Parse($d, $inv)
        $dtParsed = $dto.UtcDateTime
      }
      catch {
        try {
          $dtParsed = [DateTime]::Parse($d, $inv)
        }
        catch {
          $d2 = if ($d.Length -ge 10) { $d.Substring(0,10) } else { $null }
          if (-not $d2) { continue }
          try {
            $dtParsed = [DateTime]::ParseExact($d2, 'yyyy-MM-dd', $inv)
          }
          catch {
            continue
          }
        }
      }

      if (-not $maxDt -or $dtParsed -gt $maxDt) {
        $maxDt = $dtParsed
      }
    }

    $result.SampleDates = @($samples)
    $result.MaxUsageDate = $maxDt

    if (-not $maxDt) {
      $result.Reason = 'Nenhuma data válida pôde ser interpretada.'
      return [PSCustomObject]$result
    }

    if (-not $resourceCol) {
      $result.Reason = 'Coluna ResourceId/InstanceId não encontrada.'
      return [PSCustomObject]$result
    }

    if (-not $costCol) {
      $result.Reason = 'Coluna de custo não encontrada.'
      return [PSCustomObject]$result
    }

    $result.IsValid = $true
    $result.Reason = 'OK'
    return [PSCustomObject]$result
  }
  catch {
    $result.Reason = $_.Exception.Message
    return [PSCustomObject]$result
  }
}

function Build-FactCostPartitions {
  param(
    [Parameter(Mandatory=$true)][string]$CostExportCsvPath,
    [Parameter(Mandatory=$true)][string]$OutFolder,
    [string]$UsageDate
  )

  Write-Log "Lendo cost export: $CostExportCsvPath"

  $firstLine = Get-Content -LiteralPath $CostExportCsvPath -TotalCount 1
  $delimiter = if (($firstLine -split ';').Count -gt ($firstLine -split ',').Count) { ';' } else { ',' }
  $raw = Import-Csv -Path $CostExportCsvPath -Delimiter $delimiter

  if (-not $raw -or @($raw).Count -eq 0) {
    throw "Cost export vazio/não lido: $CostExportCsvPath"
  }

  $headers = @($raw[0].PSObject.Properties.Name)
  $colResourceId = Resolve-ColumnName $headers @('ResourceId','Resource Id','resourceId','ResourceID','InstanceId','Instance Id','instanceId','InstanceID')
  $colDate       = Resolve-ColumnName $headers @('UsageDateTime','Usage DateTime','UsageDate','Usage Date','Date')
  $colCost       = Resolve-ColumnName $headers @('PreTaxCost','PreTax Cost','CostInBillingCurrency','Cost In Billing Currency','Cost')

  if (-not $colResourceId) { throw "Não achei ResourceId/InstanceId. Headers: $($headers -join ', ')" }
  if (-not $colDate)       { throw "Não achei Date/UsageDateTime. Headers: $($headers -join ', ')" }
  if (-not $colCost)       { throw "Não achei Cost/PreTaxCost. Headers: $($headers -join ', ')" }

  $inv = [System.Globalization.CultureInfo]::InvariantCulture
  $usageDateFilter = $null
  if (-not [string]::IsNullOrWhiteSpace($UsageDate)) {
    $usageDateFilter = [datetime]::ParseExact($UsageDate, 'yyyy-MM-dd', $inv).ToString('yyyy-MM-dd')
    Write-Log "Filtrando cost export para UsageDate explícita: $usageDateFilter"
  }
  else {
    Write-Log "UsageDate não informada. O script irá agrupar e publicar todas as UsageDates presentes no CSV selecionado."
  }

  $fact = foreach ($r in $raw) {
    $rid = [string]$r.$colResourceId
    if ([string]::IsNullOrWhiteSpace($rid)) { continue }

    $d = [string]$r.$colDate
    if ([string]::IsNullOrWhiteSpace($d)) { continue }

    $dtParsed = $null
    try {
      $dto = [DateTimeOffset]::Parse($d, $inv)
      $dtParsed = $dto.UtcDateTime
    }
    catch {
      try {
        $dtParsed = [DateTime]::Parse($d, $inv)
      }
      catch {
        $d2 = if ($d.Length -ge 10) { $d.Substring(0,10) } else { $null }
        if (-not $d2) { continue }
        try {
          $dtParsed = [DateTime]::ParseExact($d2, 'yyyy-MM-dd', $inv)
        }
        catch {
          continue
        }
      }
    }

    $dateOut = $dtParsed.ToString('yyyy-MM-dd')
    if ($usageDateFilter -and $dateOut -ne $usageDateFilter) { continue }

    $costVal = [string]$r.$colCost
    $cost = ConvertTo-InvariantDecimal -Value $costVal

    [PSCustomObject]@{
      Date       = $dateOut
      ResourceId = $rid
      Cost       = $cost
    }
  }

  if (-not $fact -or @($fact).Count -eq 0) {
    if ($usageDateFilter) {
      throw "Nenhuma linha válida encontrada para UsageDate=$usageDateFilter no cost export selecionado."
    }
    throw "Nenhuma linha válida encontrada no cost export selecionado após normalização de UsageDate/Cost."
  }

  $grouped = $fact | Group-Object Date, ResourceId | ForEach-Object {
    $first = $_.Group[0]
    [PSCustomObject]@{
      Date       = $first.Date
      ResourceId = $first.ResourceId
      Cost       = ($_.Group | Measure-Object Cost -Sum).Sum
    }
  }

  Ensure-Folder -Path $OutFolder
  $results = @()

  foreach ($usageGroup in ($grouped | Group-Object Date | Sort-Object Name)) {
    $usageDateCurrent = $usageGroup.Name
    if ([string]::IsNullOrWhiteSpace($usageDateCurrent)) { throw 'UsageDate do agrupamento veio vazia ao gerar fact_cost.' }
    $fileName = ("fact_cost_{0}_pipeline_{1}.csv" -f $usageDateCurrent, $Date)
    $outPath = Combine-PathSafe -BasePath $OutFolder -ChildPath $fileName -Label 'arquivo local do fact_cost'

    $groupedFormatted = $usageGroup.Group |
      Sort-Object ResourceId |
      Select-Object Date, ResourceId, @{ Name = 'Cost'; Expression = { ([math]::Round([double]$_.Cost, 6, [System.MidpointRounding]::AwayFromZero)).ToString('0.000000', $inv) } }

    $groupedFormatted | Export-Csv -Path $outPath -NoTypeInformation -Delimiter $InternalCsvDelimiter -Encoding UTF8
    Write-Log "fact_cost exportado em CSV padronizado com delimitador '$InternalCsvDelimiter'"

    $results += [PSCustomObject]@{
      UsageDate = $usageDateCurrent
      LocalPath = $outPath
      RowCount  = @($groupedFormatted).Count
    }

    Write-Log "fact_cost gerado para UsageDate=$usageDateCurrent | Linhas=$(@($groupedFormatted).Count) | Arquivo=$outPath"
  }

  return $results
}

function Get-LatestCostExportByContentDate {
  param(
    [Parameter(Mandatory=$true)]$Ctx,
    [Parameter(Mandatory=$true)][string]$Container,
    [Parameter(Mandatory=$true)][string]$Prefix,
    [Parameter(Mandatory=$true)][string]$TempFolder,
    [Parameter(Mandatory=$true)][int]$MaxPeriodsToInspect
  )

  Write-Log "Listando blobs em '$Container' prefix '$Prefix'..."

  $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $Prefix | Where-Object { $_.Name -like '*.csv' })
  if (-not $blobs -or $blobs.Count -eq 0) {
    throw "Nenhum blob CSV encontrado em '$Container/$Prefix'."
  }

  $withPeriod = foreach ($b in $blobs) {
    $m = [regex]::Match($b.Name, '/(?<period>\d{8}-\d{8})/')
    if (-not $m.Success) { continue }

    [PSCustomObject]@{
      Blob       = $b
      PeriodText = $m.Groups['period'].Value
      PeriodDate = [datetime]::ParseExact($m.Groups['period'].Value.Substring(0,8), 'yyyyMMdd', [System.Globalization.CultureInfo]::InvariantCulture)
    }
  }

  if (-not $withPeriod -or $withPeriod.Count -eq 0) {
    throw 'Não consegui identificar períodos no path dos cost exports.'
  }

  $periods = $withPeriod |
    Sort-Object PeriodDate -Descending |
    Select-Object -ExpandProperty PeriodText -Unique |
    Select-Object -First $MaxPeriodsToInspect

  foreach ($period in $periods) {
    Write-Log "Inspecionando período: $period"
    $periodBlobs = @($withPeriod | Where-Object { $_.PeriodText -eq $period })
    $candidates = @()

    foreach ($item in $periodBlobs) {
      $blob = $item.Blob
      $localPath = Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $TempFolder
      $analysis = Get-CsvAnalysis -CsvPath $localPath

      Write-Log "Blob analisado: $($blob.Name)"
      Write-Log "  - FileSizeBytes: $($analysis.FileSizeBytes)"
      Write-Log "  - Delimiter: $($analysis.Delimiter)"
      Write-Log "  - RowCount: $($analysis.RowCount)"
      Write-Log "  - Headers: $(@($analysis.Headers) -join ', ')"
      Write-Log "  - DateColumn: $($analysis.DateColumn)"
      Write-Log "  - SampleDates: $(@($analysis.SampleDates) -join ' | ')"
      Write-Log "  - MaxUsageDate: $($analysis.MaxUsageDate)"
      Write-Log "  - IsValid: $($analysis.IsValid)"
      Write-Log "  - Reason: $($analysis.Reason)"

      $candidates += [PSCustomObject]@{
        BlobName      = $blob.Name
        LocalPath     = $localPath
        MaxUsageDate  = $analysis.MaxUsageDate
        Analysis      = $analysis
      }
    }

    $chosen = $candidates |
      Where-Object { $_.Analysis.IsValid -and $_.MaxUsageDate -ne $null } |
      Sort-Object MaxUsageDate -Descending |
      Select-Object -First 1

    if ($chosen) {
      Write-Log "Cost export selecionado pelo conteúdo: $($chosen.BlobName)"
      Write-Log "MaxUsageDate do arquivo selecionado: $($chosen.MaxUsageDate.ToString('yyyy-MM-dd'))"
      return $chosen
    }

    Write-Log "Nenhum arquivo válido encontrado no período $period. Tentando período anterior..." 'WARN'
  }

  throw 'Não foi possível determinar o arquivo mais recente pelo conteúdo.'
}

$SubscriptionId = Resolve-Setting -Value $SubscriptionId -AutomationVariableName 'FinOps-SubscriptionId' -Label 'SubscriptionId'
$StorageAccountName = Resolve-Setting -Value $StorageAccountName -AutomationVariableName 'FinOps-StorageAccountName' -Label 'StorageAccountName'
$StorageAccountResourceGroupName = Resolve-Setting -Value $StorageAccountResourceGroupName -AutomationVariableName 'FinOps-StorageAccountResourceGroupName' -Label 'StorageAccountResourceGroupName'
$CostContainer = Resolve-Setting -Value $CostContainer -AutomationVariableName 'FinOps-CostContainer' -Label 'CostContainer'
$CostPrefix = Resolve-Setting -Value $CostPrefix -AutomationVariableName 'FinOps-CostPrefix' -Label 'CostPrefix'
$FinopsContainer = Resolve-Setting -Value $FinopsContainer -AutomationVariableName 'FinOps-FinopsContainer' -Label 'FinopsContainer'
$FactCostPrefix = Resolve-Setting -Value $FactCostPrefix -AutomationVariableName 'FinOps-FactCostPrefix' -Label 'FactCostPrefix'
$FinopsTempFolder = Resolve-TempFolder -PreferredPath $FinopsTempFolder

Ensure-AzLogin -SubscriptionId $SubscriptionId
Ensure-Folder -Path $FinopsTempFolder
Write-Log "Pasta temporária local: $FinopsTempFolder"

$nowBrazil = Get-BrazilNow
Write-Log "PipelinePartitionDate (Brasil): $Date"
Write-Log "Data/hora local Brasil: $($nowBrazil.ToString('yyyy-MM-dd HH:mm:ss'))"

$ctx = Get-StorageContextByName -StorageAccountName $StorageAccountName -ResourceGroupName $StorageAccountResourceGroupName
$selected = Get-LatestCostExportByContentDate -Ctx $ctx -Container $CostContainer -Prefix $CostPrefix -TempFolder $FinopsTempFolder -MaxPeriodsToInspect $MaxPeriodsToInspect
if (-not $selected) { throw 'Falha ao resolver o blob de cost export selecionado.' }
if ([string]::IsNullOrWhiteSpace($selected.LocalPath)) { throw "O cost export selecionado não retornou LocalPath válido. Blob=$($selected.BlobName)" }
if (-not (Test-Path -LiteralPath $selected.LocalPath)) { throw "O arquivo local do cost export não existe: $($selected.LocalPath)" }

if ([string]::IsNullOrWhiteSpace($UsageDate)) {
  Write-Log "UsageDate não informada explicitamente. Será derivada por linha do conteúdo do CSV e publicada por partição de UsageDate."
}
else {
  Write-Log "UsageDate informada explicitamente: $UsageDate"
}

$safePipelineDate = if ([string]::IsNullOrWhiteSpace($Date)) { (Get-BrazilNow).ToString('yyyy-MM-dd') } else { $Date }
$outFactFolder = Combine-PathSafe -BasePath $FinopsTempFolder -ChildPath ("fact_cost_pipeline_{0}" -f $safePipelineDate) -Label 'pasta de saída local do fact_cost'
Write-Log "Pasta de saída local do fact_cost (staging local por PipelineDate): $outFactFolder"
$factOutputs = Build-FactCostPartitions -CostExportCsvPath $selected.LocalPath -OutFolder $outFactFolder -UsageDate $UsageDate

if (-not $factOutputs -or @($factOutputs).Count -eq 0) {
  throw "Nenhum arquivo de fact_cost foi gerado para upload."
}

$uploaded = @()
foreach ($item in $factOutputs) {
  if ([string]::IsNullOrWhiteSpace($item.UsageDate)) { throw 'Item de saída sem UsageDate para upload do fact_cost.' }
  if ([string]::IsNullOrWhiteSpace($item.LocalPath)) { throw "Item de saída sem LocalPath para UsageDate=$($item.UsageDate)" }
  if (-not (Test-Path -LiteralPath $item.LocalPath)) { throw "Arquivo local de saída não encontrado para UsageDate=$($item.UsageDate): $($item.LocalPath)" }
  $leafName = Get-FileNameSafe -Path $item.LocalPath -Label 'fact_cost local'
  $blobPath = "$FactCostPrefix/dt=$($item.UsageDate)/$leafName"
  Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $item.LocalPath -BlobPath $blobPath
  $uploaded += [PSCustomObject]@{
    UsageDate = $item.UsageDate
    BlobPath  = $blobPath
    RowCount  = $item.RowCount
  }
  Write-Log "OK - fact_cost enviado para: $FinopsContainer/$blobPath | ParticaoDT=$($item.UsageDate) | PipelineDate=$Date | Linhas=$($item.RowCount)"
}

$usageDatesUploaded = ($uploaded | Select-Object -ExpandProperty UsageDate) -join ', '
Write-Log "Resumo final: PipelineDate=$Date | ParticoesDTPublicadas=$usageDatesUploaded | SourceBlob=$($selected.BlobName)"
