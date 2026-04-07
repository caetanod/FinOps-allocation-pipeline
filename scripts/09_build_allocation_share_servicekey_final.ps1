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
  [string]$BaseAllocationPrefix = "silver/allocation_share_servicekey",

  [Parameter(Mandatory = $false)]
  [string]$SqlPoolPrefix = "gold/sqlpool_allocation_share_metrics",

  [Parameter(Mandatory = $false)]
  [string]$AksPrefix = "gold/aks_allocation_share_opencost",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefix = "gold/allocation_share_servicekey_final",

  [Parameter(Mandatory = $false)]
  [string]$TempFolder = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

trap {
  Write-Error ("❌ Step 09 falhou. Linha: {0}. Comando: {1}. Erro: {2}" -f $_.InvocationInfo.ScriptLineNumber, $_.InvocationInfo.Line.Trim(), $_.Exception.Message)
  throw
}

$TargetSubscriptions = @(
  "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
  "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

$Date = ($Date + "").Trim().Trim([char[]]@('"', "'"))
$PipelineDate = ($PipelineDate + "").Trim().Trim([char[]]@('"', "'"))
$RequestedUsageDate = $Date
$PipelinePartitionDate = $PipelineDate

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

function Ensure-Folder {
  param([Parameter(Mandatory = $true)][string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path)) {
    throw "Ensure-Folder recebeu Path vazio."
  }

  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -Path $Path -ItemType Directory -Force -ErrorAction Stop | Out-Null
  }
}

function Get-BrazilNow {
  $tzIds = @(
    "E. South America Standard Time",
    "America/Sao_Paulo"
  )

  foreach ($tzId in $tzIds) {
    try {
      $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($tzId)
      return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
    }
    catch {
    }
  }

  throw "Não foi possível resolver o timezone do Brasil (America/Sao_Paulo)."
}

function Login-Azure {
  Write-Host "🔐 Conectando com Managed Identity..."
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

  if (-not (Test-Path -LiteralPath $LocalPath)) {
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

function Get-AvailablePartitionDate {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $false)][int]$LookbackDays = 2
  )

  $nowLocal = Get-BrazilNow
  $datesToTry = New-Object System.Collections.ArrayList
  for ($i = 0; $i -le $LookbackDays; $i++) {
    [void]$datesToTry.Add($nowLocal.AddDays(-$i).ToString("yyyy-MM-dd"))
  }

  foreach ($dt in $datesToTry) {
    $prefix = "$BasePrefix/dt=$dt/"
    $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction SilentlyContinue)
    if ($blobs.Count -gt 0) {
      Write-Host "✅ Partição disponível encontrada: $dt ($Container/$prefix)"
      return $dt
    }
  }

  throw "Nenhuma partição disponível encontrada em '$Container/$BasePrefix' para hoje, D-1 ou D-2 no horário do Brasil."
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

  $Dt = ($Dt + "").Trim().Trim([char[]]@('"', "'"))
  $prefix = "$BasePrefix/dt=$Dt/"
  Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

  $blobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix -ErrorAction Stop |
    Where-Object { $_.Name -match $NameRegex }
  )

  if ($blobs.Count -eq 0) {
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
  return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $TempFolder)
}

function Normalize-Text {
  param([AllowNull()][string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  return ($s + "").Trim([char[]]@([char]0xFEFF)).Trim().Trim([char[]]@('"'))
}

function Normalize-Cliente {
  param([string]$Cliente)

  $c = Normalize-Text $Cliente
  if ([string]::IsNullOrWhiteSpace($c)) { return "UNKNOWN" }

  $c = $c.Trim().ToUpperInvariant()

  if ($c -in @("DEFAULT", "DEFINIR", "NAN")) {
    return "UNKNOWN"
  }

  return $c
}

function Parse-DoubleInvariant {
  param([string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return 0.0 }
  $inv = [System.Globalization.CultureInfo]::InvariantCulture
  $x = ($s + "").Trim()

  [double]$v = 0
  if ([double]::TryParse($x, [System.Globalization.NumberStyles]::Any, $inv, [ref]$v)) {
    return $v
  }

  $x2 = $x.Replace(".", "").Replace(",", ".")
  if ([double]::TryParse($x2, [System.Globalization.NumberStyles]::Any, $inv, [ref]$v)) {
    return $v
  }

  return 0.0
}

function Format-DoubleInvariant {
  param([double]$Value)
  return $Value.ToString("F6", [System.Globalization.CultureInfo]::InvariantCulture)
}

function Normalize-ShareRowsByServiceKey {
  param(
    [Parameter(Mandatory = $true)]
    [object[]]$Rows
  )

  if ($null -eq $Rows -or $Rows.Count -eq 0) {
    return @()
  }

  $normalized = New-Object System.Collections.ArrayList

  $groups = @(
    $Rows |
    Group-Object -Property Date, ServiceKey
  )

  foreach ($g in $groups) {
    $items = @($g.Group)
    if ($items.Count -eq 0) { continue }

    [double]$sum = 0.0
    foreach ($item in $items) {
      $sum += [double]$item.Share
    }

    if ($sum -le 0) {
      foreach ($item in $items) {
        [void]$normalized.Add($item)
      }
      continue
    }

    $work = @()
    foreach ($item in $items) {
      $newItem = [PSCustomObject]@{
        Date           = $item.Date
        ServiceKey     = $item.ServiceKey
        Cliente        = $item.Cliente
        Share          = [Math]::Round(([double]$item.Share / $sum), 6)
        DriverType     = $item.DriverType
        AllocationMode = $item.AllocationMode
        Notes          = $item.Notes
      }
      $work += $newItem
    }

    [double]$roundedSum = 0.0
    foreach ($item in $work) {
      $roundedSum += [double]$item.Share
    }

    $delta = [Math]::Round((1.0 - $roundedSum), 6)

    if ([Math]::Abs($delta) -gt 0 -and $work.Count -gt 0) {
      $target = $work | Sort-Object -Property Share -Descending | Select-Object -First 1
      $target.Share = [Math]::Round(([double]$target.Share + $delta), 6)
    }

    foreach ($item in $work) {
      [void]$normalized.Add($item)
    }
  }

  return @($normalized)
}


function Get-RowDateValue {
  param($row)

  if ($null -eq $row) { return "" }

  foreach ($candidate in @("Date","UsageDate","UsageDateTime","usage_date","date","USAGEDATE","USAGEDATETIME")) {
    if ($row.PSObject.Properties.Name -contains $candidate) {
      $v = Normalize-Text $row.$candidate
      if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    }
  }

  return ""
}

function Try-ParseDateValue {
  param(
    [Parameter(Mandatory = $true)][string]$RawValue,
    [ref]$ParsedDate
  )

  $ParsedDate.Value = [datetime]::MinValue
  $value = Normalize-Text $RawValue
  if ([string]::IsNullOrWhiteSpace($value)) { return $false }

  if ($value.Length -ge 10) {
    $value10 = $value.Substring(0,10)
    if ($value10 -match '^\d{4}-\d{2}-\d{2}$') {
      try {
        $ParsedDate.Value = [datetime]::ParseExact(
          $value10,
          "yyyy-MM-dd",
          [System.Globalization.CultureInfo]::InvariantCulture
        )
        return $true
      }
      catch {
      }
    }
  }

  $formats = @(
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "yyyy-MM-ddTHH:mm:ss",
    "yyyy-MM-ddTHH:mm:ssZ",
    "yyyy-MM-ddTHH:mm:ss.fffZ",
    "yyyy-MM-ddTHH:mm:ssK",
    "yyyy-MM-ddTHH:mm:ss.fffK",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss"
  )

  foreach ($fmt in $formats) {
    try {
      $ParsedDate.Value = [datetime]::ParseExact(
        $value,
        $fmt,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::AllowWhiteSpaces
      )
      return $true
    }
    catch {
    }
  }

  try {
    $ParsedDate.Value = [datetime]::Parse(
      $value,
      [System.Globalization.CultureInfo]::InvariantCulture,
      [System.Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    return $true
  }
  catch {
  }

  try {
    $ParsedDate.Value = [datetime]::Parse(
      $value,
      [System.Globalization.CultureInfo]::CurrentCulture,
      [System.Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    return $true
  }
  catch {
  }

  return $false
}

function Resolve-UsageDatesFromRows {
  param(
    [Parameter(Mandatory = $true)]$Rows,
    [Parameter(Mandatory = $true)][string]$Label
  )

  $validDates = New-Object System.Collections.ArrayList

  foreach ($row in @($Rows)) {
    $raw = Get-RowDateValue $row
    if ([string]::IsNullOrWhiteSpace($raw)) { continue }

    $parsed = [datetime]::MinValue
    if (Try-ParseDateValue -RawValue $raw -ParsedDate ([ref]$parsed)) {
      $dateText = $parsed.Date.ToString("yyyy-MM-dd")
      if (-not ($validDates -contains $dateText)) {
        [void]$validDates.Add($dateText)
      }
    }
  }

  if ($validDates.Count -eq 0) {
    throw "Não foi possível derivar UsageDate a partir do conteúdo de '$Label'. Nenhuma data válida encontrada nas colunas Date/UsageDate/UsageDateTime."
  }

  return @($validDates | Sort-Object)
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

function Normalize-AllocationRows {
  param(
    [Parameter(Mandatory = $true)]$Rows,
    [Parameter(Mandatory = $true)][string[]]$ExpectedDates,
    [Parameter(Mandatory = $true)][string]$SourceName
  )

  $dateSet = @{}
  foreach ($d in @($ExpectedDates)) {
    if (-not [string]::IsNullOrWhiteSpace($d)) {
      $dateSet[$d] = $true
    }
  }

  $normalized = New-Object System.Collections.ArrayList

  foreach ($r in @($Rows)) {
    $rowDateRaw = Get-RowDateValue $r
    if ([string]::IsNullOrWhiteSpace($rowDateRaw)) { continue }

    $parsedDate = [datetime]::MinValue
    if (-not (Try-ParseDateValue -RawValue $rowDateRaw -ParsedDate ([ref]$parsedDate))) { continue }

    $rowDate = $parsedDate.Date.ToString("yyyy-MM-dd")
    if (-not $dateSet.ContainsKey($rowDate)) { continue }

    $serviceKey     = Get-ColumnValue -Row $r -CandidateNames @("ServiceKey","servicekey","SERVICEKEY")
    $cliente        = Normalize-Cliente (Get-ColumnValue -Row $r -CandidateNames @("Cliente","Client","CLIENTE"))
    $driverType     = Get-ColumnValue -Row $r -CandidateNames @("DriverType","Driver","DRIVERTYPE")
    $allocationMode = Get-ColumnValue -Row $r -CandidateNames @("AllocationMode","Mode","ALLOCATIONMODE")
    $notes          = Get-ColumnValue -Row $r -CandidateNames @("Notes","Comment","COMMENTS","Observacao","Observações")
    $share          = Parse-DoubleInvariant (Get-ColumnValue -Row $r -CandidateNames @("Share","SHARE"))

    if ([string]::IsNullOrWhiteSpace($serviceKey)) { continue }
    if ([string]::IsNullOrWhiteSpace($cliente))    { continue }
    if ($share -le 0)                              { continue }

    if ([string]::IsNullOrWhiteSpace($driverType)) {
      $driverType = $SourceName
    }

    if ([string]::IsNullOrWhiteSpace($allocationMode)) {
      switch ($SourceName) {
        "BASE_SHARED"            { $allocationMode = "GLOBAL_WEIGHT" }
        "SQLPOOL_DB_METRICS"     { $allocationMode = "SQLPOOL_SCRIPT_07" }
        "AKS_OPENCOST_NAMESPACE" { $allocationMode = "AKS_SCRIPT_08" }
        default                  { $allocationMode = $SourceName }
      }
    }

    [void]$normalized.Add([PSCustomObject]@{
      Date           = $rowDate
      ServiceKey     = $serviceKey
      Cliente        = $cliente
      Share          = [Math]::Round([double]$share, 6)
      DriverType     = $driverType
      AllocationMode = $allocationMode
      Notes          = $notes
      Source         = $SourceName
    })
  }

  return ,$normalized
}

function Get-ServiceKeyPrefix {
  param([string]$serviceKey)
  $sk = Normalize-Text $serviceKey
  if ([string]::IsNullOrWhiteSpace($sk)) { return "" }

  $idx = $sk.IndexOf(":")
  if ($idx -lt 0) { return $sk.ToUpperInvariant() }
  return $sk.Substring(0, $idx).ToUpperInvariant()
}

Ensure-Folder -Path $TempFolder
Login-Azure
$ctx = Get-StorageContext -StorageAccountName $StorageAccountName

if ([string]::IsNullOrWhiteSpace($PipelinePartitionDate)) {
  $PipelinePartitionDate = Get-AvailablePartitionDate -Ctx $ctx -Container $FinopsContainer -BasePrefix $BaseAllocationPrefix
}

Write-Host "📅 PipelinePartitionDate: $PipelinePartitionDate"
if (-not [string]::IsNullOrWhiteSpace($RequestedUsageDate)) {
  Write-Host "📅 UsageDate solicitada : $RequestedUsageDate"
}
Write-Host "📂 TempFolder: $TempFolder"

Write-Host "`n== DOWNLOAD INPUTS =="
$basePath = Download-BlobByExactDatePrefix -Ctx $ctx -Container $FinopsContainer -BasePrefix $BaseAllocationPrefix -Dt $PipelinePartitionDate -NameRegex "allocation_share_.*\.csv$" -Label "base_allocation"
$sqlPath  = Download-BlobByExactDatePrefix -Ctx $ctx -Container $FinopsContainer -BasePrefix $SqlPoolPrefix        -Dt $PipelinePartitionDate -NameRegex "sqlpool_allocation_share_.*\.csv$" -Label "sqlpool_allocation" -Optional $true
$aksPath  = Download-BlobByExactDatePrefix -Ctx $ctx -Container $FinopsContainer -BasePrefix $AksPrefix            -Dt $PipelinePartitionDate -NameRegex "aks_allocation_share_.*\.csv$" -Label "aks_allocation" -Optional $true

$baseRows = @(Import-Csv -Path $basePath -Delimiter ";")
if ($baseRows.Count -eq 0) { throw "Arquivo base allocation vazio." }

$usageDates = @(Resolve-UsageDatesFromRows -Rows $baseRows -Label "base_allocation")

if (-not [string]::IsNullOrWhiteSpace($RequestedUsageDate)) {
  if ($usageDates -notcontains $RequestedUsageDate) {
    throw "A UsageDate solicitada '$RequestedUsageDate' não existe no base_allocation selecionado. Datas disponíveis: $($usageDates -join ', ')"
  }
  $usageDates = @($RequestedUsageDate)
}

Write-Host "📅 UsageDates derivadas do conteúdo: $($usageDates -join ', ')"

$sqlRows = @()
if (-not [string]::IsNullOrWhiteSpace($sqlPath) -and (Test-Path -LiteralPath $sqlPath)) {
  $sqlRows = @(Import-Csv -Path $sqlPath -Delimiter ";")
}

$aksRows = @()
if (-not [string]::IsNullOrWhiteSpace($aksPath) -and (Test-Path -LiteralPath $aksPath)) {
  $aksRows = @(Import-Csv -Path $aksPath -Delimiter ";")
}

Write-Host "`n== NORMALIZE INPUTS =="
$baseNorm = @(Normalize-AllocationRows -Rows $baseRows -ExpectedDates $usageDates -SourceName "BASE_SHARED")
$sqlNorm  = @(Normalize-AllocationRows -Rows $sqlRows  -ExpectedDates $usageDates -SourceName "SQLPOOL_DB_METRICS")
$aksNorm  = @(Normalize-AllocationRows -Rows $aksRows  -ExpectedDates $usageDates -SourceName "AKS_OPENCOST_NAMESPACE")

$baseNormRows = @($baseNorm | ForEach-Object { $_ })
$sqlNormRows  = @($sqlNorm  | ForEach-Object { $_ })
$aksNormRows  = @($aksNorm  | ForEach-Object { $_ })

Write-Host ("📌 Base rows normalizadas : {0}" -f $baseNormRows.Count)
Write-Host ("📌 SQL rows normalizadas  : {0}" -f $sqlNormRows.Count)
Write-Host ("📌 AKS rows normalizadas  : {0}" -f $aksNormRows.Count)

if ($sqlNormRows.Count -eq 0) {
  Write-Host "⚠ SQL allocation vazio ou ausente. Mantendo base para SQLPOOL/SQLDB quando necessário." -ForegroundColor Yellow
}

if ($aksNormRows.Count -eq 0) {
  Write-Host "⚠ AKS allocation vazio ou ausente. Mantendo base para AKS quando necessário." -ForegroundColor Yellow
}

Write-Host "`n== MERGE WITH PRECEDENCE =="
$finalRows = New-Object System.Collections.ArrayList
$hasSqlSpecialized = ($sqlNormRows.Count -gt 0)
$hasAksSpecialized = ($aksNormRows.Count -gt 0)

foreach ($r in $baseNormRows) {
  $prefix = Get-ServiceKeyPrefix $r.ServiceKey

  if ($prefix -in @("SQLPOOL","SQLDB") -and $hasSqlSpecialized) { continue }
  if ($prefix -eq "AKS" -and $hasAksSpecialized) { continue }

  [void]$finalRows.Add($r)
}

foreach ($r in $sqlNormRows) { [void]$finalRows.Add($r) }
foreach ($r in $aksNormRows) { [void]$finalRows.Add($r) }

if ($finalRows.Count -eq 0) { throw "Nenhuma linha resultante após merge." }
Write-Host ("📌 Total linhas merged: {0}" -f $finalRows.Count)

Write-Host "`n== GROUP FINAL =="
$groupedFinal = @(
  @($finalRows) |
  Group-Object -Property Date, ServiceKey, Cliente |
  ForEach-Object {
    $first = $_.Group[0]
    $shareSum = [double](($_.Group | Measure-Object -Property Share -Sum).Sum)

    $driverTypes = @($_.Group | Select-Object -ExpandProperty DriverType -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $allocationModes = @($_.Group | Select-Object -ExpandProperty AllocationMode -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $notesList = @($_.Group | Select-Object -ExpandProperty Notes -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    [PSCustomObject]@{
      Date           = $first.Date
      ServiceKey     = $first.ServiceKey
      Cliente        = Normalize-Cliente $first.Cliente
      Share          = [Math]::Round($shareSum, 6)
      DriverType     = ($driverTypes -join "|")
      AllocationMode = ($allocationModes -join "|")
      Notes          = ($notesList -join "|")
    }
  }
)

if ($groupedFinal.Count -eq 0) { throw "Nenhuma linha final agrupada foi gerada." }
Write-Host ("📌 Total linhas finais agrupadas: {0}" -f $groupedFinal.Count)

Write-Host "`n== REGROUP AFTER CLIENT NORMALIZATION =="
$groupedFinal = @(
  $groupedFinal |
  Group-Object -Property Date, ServiceKey, Cliente |
  ForEach-Object {
    $first = $_.Group[0]
    $shareSum = [double](($_.Group | Measure-Object -Property Share -Sum).Sum)

    $driverTypes = @($_.Group | Select-Object -ExpandProperty DriverType -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $allocationModes = @($_.Group | Select-Object -ExpandProperty AllocationMode -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $notesList = @($_.Group | Select-Object -ExpandProperty Notes -Unique | ForEach-Object { Normalize-Text $_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    [PSCustomObject]@{
      Date           = $first.Date
      ServiceKey     = $first.ServiceKey
      Cliente        = $first.Cliente
      Share          = [Math]::Round($shareSum, 6)
      DriverType     = ($driverTypes -join "|")
      AllocationMode = ($allocationModes -join "|")
      Notes          = ($notesList -join "|")
    }
  }
)

Write-Host ("📌 Total linhas após reagrupamento: {0}" -f $groupedFinal.Count)

Write-Host "`n== NORMALIZE SHARES BY SERVICEKEY =="
$groupedFinal = @(Normalize-ShareRowsByServiceKey -Rows @($groupedFinal))
if ($groupedFinal.Count -eq 0) { throw "Nenhuma linha final após normalização por ServiceKey." }
Write-Host ("📌 Total linhas após normalização: {0}" -f $groupedFinal.Count)

Write-Host "`n== VALIDATION =="
$validation = @(
  $groupedFinal |
  Group-Object -Property Date, ServiceKey |
  ForEach-Object {
    $first = $_.Group[0]
    $sum = [double](($_.Group | Measure-Object -Property Share -Sum).Sum)
    [PSCustomObject]@{
      Date       = $first.Date
      ServiceKey = $first.ServiceKey
      ShareSum   = [Math]::Round($sum, 6)
      Rows       = $_.Count
    }
  }
)

$bad = @($validation | Where-Object { [Math]::Abs($_.ShareSum - 1.0) -gt 0.01 })
Write-Host ("📌 ServiceKeys finais           : {0}" -f $validation.Count)
Write-Host ("📌 ServiceKeys fora de ~1       : {0}" -f $bad.Count)

if ($bad.Count -gt 0) {
  Write-Warning "⚠ Existem ServiceKeys com share fora de 1"
  $bad | Select-Object -First 10 | Format-Table | Out-String | Write-Host
}
else {
  Write-Host "✅ Todos os ServiceKeys finais estão com share somando aproximadamente 1."
}

Write-Host "`n== EXPORT =="
$outFile = Join-Path $TempFolder ("allocation_share_servicekey_final_{0}.csv" -f $PipelinePartitionDate)

$exportRows = @(
  $groupedFinal |
  Where-Object {
    -not [string]::IsNullOrWhiteSpace($_.Date) -and
    -not [string]::IsNullOrWhiteSpace($_.ServiceKey) -and
    -not [string]::IsNullOrWhiteSpace($_.Cliente) -and
    ([double]$_.Share -gt 0)
  } |
  Sort-Object -Property Date, ServiceKey, Cliente |
  Select-Object `
    @{Name='Date';Expression={$_.Date}},
    @{Name='ServiceKey';Expression={$_.ServiceKey}},
    @{Name='Cliente';Expression={$_.Cliente}},
    @{Name='Share';Expression={ Format-DoubleInvariant ([double]$_.Share) }},
    @{Name='DriverType';Expression={$_.DriverType}},
    @{Name='AllocationMode';Expression={$_.AllocationMode}},
    @{Name='Notes';Expression={$_.Notes}}
)

if ($exportRows.Count -eq 0) {
  throw "Nenhuma linha pronta para exportação foi gerada."
}

$exportRows | Export-Csv -Path $outFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$blobOut = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $outFile -Leaf)"
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outFile -BlobPath $blobOut

Write-Host "✅ Final local: $outFile"
Write-Host "✅ Blob output folder: $FinopsContainer/$OutPrefix/dt=$PipelinePartitionDate/"
Write-Host "✅ Blob output file  : $FinopsContainer/$blobOut"
Write-Host "✅ Exported rows     : $($exportRows.Count)"
Write-Host "✅ UsageDates exportadas: $($usageDates -join ', ')"
