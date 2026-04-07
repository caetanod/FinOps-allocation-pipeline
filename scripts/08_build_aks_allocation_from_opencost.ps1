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
  [string]$ReferencePrefix = "config/reference",

  [Parameter(Mandatory = $false)]
  [string]$OpenCostClustersFileName = "opencost_clusters.csv",

  [Parameter(Mandatory = $false)]
  [string]$ClientesAliasFileName = "clientes_alias.csv",

  [Parameter(Mandatory = $false)]
  [string]$SystemNamespacesFileName = "aks_system_namespaces.csv",

  [Parameter(Mandatory = $false)]
  [string]$ResourceToServicePrefix = "silver/resource_to_service",

  [Parameter(Mandatory = $false)]
  [string]$FactCostPrefix = "silver/fact_cost",

  [Parameter(Mandatory = $false)]
  [string]$OutPrefix = "gold/aks_allocation_share_opencost",

  [Parameter(Mandatory = $false)]
  [string]$BearerToken = "",

  [Parameter(Mandatory = $false)]
  [int]$OpenCostTimeoutSec = 60,

  [Parameter(Mandatory = $false)]
  [int]$RetryCount = 2,

  [Parameter(Mandatory = $false)]
  [int]$RetryDelaySeconds = 5,

  [Parameter(Mandatory = $false)]
  [switch]$IgnoreSslErrors,

  [Parameter(Mandatory = $false)]
  [string]$TempFolder = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"


function Get-BrazilNow {
  try {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("E. South America Standard Time")
  }
  catch {
    try { $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("America/Sao_Paulo") }
    catch { throw "Não foi possível resolver o timezone do Brasil. Detalhe: $($_.Exception.Message)" }
  }

  return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

$TargetSubscriptions = @(
  "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
  "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

$Date = ($Date + "").Trim().Trim('"').Trim("'")
$PipelineDate = ($PipelineDate + "").Trim().Trim('"').Trim("'")

# REGRA DO SCRIPT 08:
# - PublishPartitionDate = dia corrente no Brasil (pasta dt= da publicação)
# - PipelineDate         = apenas pista opcional para localizar insumos
# - UsageDate            = derivada do conteúdo do fact_cost selecionado
$BrazilNow = Get-BrazilNow
if ([string]::IsNullOrWhiteSpace($Date)) {
  $Date = $BrazilNow.ToString("yyyy-MM-dd")
}

$Date = ($Date + "").Trim().Trim('"').Trim("'")
$UsageDate = ""
$PipelinePartitionDate = $PipelineDate
$PublishPartitionDate = $BrazilNow.ToString("yyyy-MM-dd")

Write-Host "📅 PublishPartitionDate: $PublishPartitionDate"
Write-Host "📅 PipelinePartitionDate: será resolvida por timezone Brasil + fallback de partição"
Write-Host "📅 UsageDate: será derivada do conteúdo do fact_cost selecionado"

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
elseif (($IsLinux -or $IsMacOS) -and $TempFolder.Length -ge 2 -and $TempFolder.Substring(1,1) -eq ':') {
  Write-Warning "TempFolder Windows detectado em worker Linux/macOS. Ajustando para /tmp/finops. Valor original: $TempFolder"
  $TempFolder = "/tmp/finops"
}

Write-Host "📂 TempFolder: $TempFolder"

function Test-DateString {
  param([Parameter(Mandatory = $true)][string]$Value)
  return ($Value -match '^\d{4}-\d{2}-\d{2}$')
}

function Resolve-PipelinePartitionDate {
  param(
    [Parameter(Mandatory = $false)][string]$ExplicitPipelineDate,
    [Parameter(Mandatory = $false)][string]$FallbackDate
  )

  $explicit = Normalize-Text $ExplicitPipelineDate
  if (-not [string]::IsNullOrWhiteSpace($explicit)) {
    if (-not (Test-DateString -Value $explicit)) {
      throw "PipelineDate inválida. Use yyyy-MM-dd. Valor recebido: $explicit"
    }
    return $explicit
  }

  $fallback = Normalize-Text $FallbackDate
  if (-not [string]::IsNullOrWhiteSpace($fallback)) {
    if (-not (Test-DateString -Value $fallback)) {
      throw "Date inválida. Use yyyy-MM-dd. Valor recebido: $fallback"
    }
    return $fallback
  }

  return (Get-BrazilNow).ToString("yyyy-MM-dd")
}


function Convert-ToSafeArray {
  param(
    [Parameter(Mandatory = $false)]
    [object]$InputObject
  )

  if ($null -eq $InputObject) {
    return @()
  }

  if ($InputObject -is [string]) {
    return @($InputObject)
  }

  if ($InputObject -is [System.Array]) {
    return @($InputObject)
  }

  if ($InputObject -is [System.Collections.IEnumerable]) {
    $result = New-Object System.Collections.ArrayList
    foreach ($item in $InputObject) {
      [void]$result.Add($item)
    }
    return @($result.ToArray())
  }

  return @($InputObject)
}

function Get-DtValuesFromPrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$Prefix
  )

  $prefixNorm = $Prefix.Trim('/')
  $searchPrefix = "$prefixNorm/"
  $values = New-Object System.Collections.Generic.HashSet[string]

  $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $searchPrefix -ErrorAction Stop)
  foreach ($b in $blobs) {
    $name = $b.Name + ''
    if ($name -match [regex]::Escape($searchPrefix) + 'dt=(\d{4}-\d{2}-\d{2})/') {
      [void]$values.Add($matches[1])
    }
  }

  return @((Convert-ToSafeArray -InputObject $values) | Sort-Object)
}

function Get-CommonPartitionDate {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string[]]$Prefixes,
    [Parameter(Mandatory = $false)][string]$PreferredDate
  )

  $dateSets = @{}
  foreach ($prefix in $Prefixes) {
    $vals = @(Get-DtValuesFromPrefix -Ctx $Ctx -Container $Container -Prefix $prefix)
    if (-not $vals -or $vals.Count -eq 0) {
      throw "Nenhuma partição dt= encontrada em $Container/$prefix"
    }
    $dateSets[$prefix] = @($vals)
  }

  $common = $null
  foreach ($prefix in $Prefixes) {
    $vals = @($dateSets[$prefix])
    if ($null -eq $common) {
      $common = @($vals)
    }
    else {
      $common = @($common | Where-Object { $vals -contains $_ })
    }
  }

  if (-not $common -or $common.Count -eq 0) {
    throw "Nenhuma partição comum encontrada para os insumos obrigatórios."
  }

  $preferred = Normalize-Text $PreferredDate
  if (-not [string]::IsNullOrWhiteSpace($preferred) -and ($common -contains $preferred)) {
    return $preferred
  }

  $nowLocal = Get-BrazilNow
  $candidates = @(
    $nowLocal.ToString('yyyy-MM-dd'),
    $nowLocal.AddDays(-1).ToString('yyyy-MM-dd'),
    $nowLocal.AddDays(-2).ToString('yyyy-MM-dd')
  )

  foreach ($dt in $candidates) {
    if ($common -contains $dt) { return $dt }
  }

  return @($common | Sort-Object -Descending | Select-Object -First 1)[0]
}

function Get-LatestAvailablePartitionDate {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$Prefix,
    [Parameter(Mandatory = $false)][string]$PreferredDate
  )

  $vals = @(Get-DtValuesFromPrefix -Ctx $Ctx -Container $Container -Prefix $Prefix)
  if (-not $vals -or $vals.Count -eq 0) {
    throw "Nenhuma partição dt= encontrada em $Container/$Prefix"
  }

  $preferred = Normalize-Text $PreferredDate
  if (-not [string]::IsNullOrWhiteSpace($preferred) -and ($vals -contains $preferred)) {
    return $preferred
  }

  $nowLocal = Get-BrazilNow
  $candidates = @(
    $nowLocal.ToString('yyyy-MM-dd'),
    $nowLocal.AddDays(-1).ToString('yyyy-MM-dd'),
    $nowLocal.AddDays(-2).ToString('yyyy-MM-dd')
  )

  foreach ($dt in $candidates) {
    if ($vals -contains $dt) { return $dt }
  }

  return @($vals | Sort-Object -Descending | Select-Object -First 1)[0]
}

function Get-FactCostUsageDateFromFileName {
  param([Parameter(Mandatory = $true)][string]$Path)

  $name = [System.IO.Path]::GetFileName($Path)
  if ([string]::IsNullOrWhiteSpace($name)) { return $null }

  if ($name -match '^fact_cost_(\d{4}-\d{2}-\d{2})(?:_pipeline_\d{4}-\d{2}-\d{2})?\.csv$') {
    return $matches[1]
  }

  if ($name -match 'fact_cost_(\d{4}-\d{2}-\d{2})') {
    return $matches[1]
  }

  return $null
}

function Convert-ToUsageDateString {
  param([Parameter(Mandatory = $true)][object]$Value)

  if ($null -eq $Value) { return $null }
  $text = Normalize-Text ($Value + '')
  if ([string]::IsNullOrWhiteSpace($text)) { return $null }

  if ($text -match '^(\d{4}-\d{2}-\d{2})') { return $matches[1] }
  if ($text -match '^(\d{2})/(\d{2})/(\d{4})') { return ('{0}-{1}-{2}' -f $matches[3], $matches[2], $matches[1]) }

  $patterns = @(
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    'yyyy-MM-ddTHH:mm:ss',
    'yyyy-MM-ddTHH:mm:ssZ',
    'yyyy-MM-ddTHH:mm:ss.fffZ',
    'yyyy-MM-ddTHH:mm:ss.FFFFFFFK',
    'MM/dd/yyyy',
    'MM/dd/yyyy HH:mm:ss',
    'M/d/yyyy',
    'M/d/yyyy HH:mm:ss',
    'dd/MM/yyyy',
    'dd/MM/yyyy HH:mm:ss'
  )

  foreach ($pattern in $patterns) {
    try {
      $dt = [datetime]::ParseExact($text, $pattern, [System.Globalization.CultureInfo]::InvariantCulture)
      return $dt.ToString('yyyy-MM-dd')
    }
    catch {}
  }

  try {
    $dt2 = [datetime]::Parse($text, [System.Globalization.CultureInfo]::InvariantCulture)
    return $dt2.ToString('yyyy-MM-dd')
  }
  catch {}

  return $null
}

function Get-MaxUsageDateFromFactCost {
  param([Parameter(Mandatory = $true)]$Rows)

  $maxDate = $null
  foreach ($row in @($Rows)) {
    if ($null -eq $row) { continue }

    $candidates = New-Object System.Collections.Generic.List[string]
    foreach ($propName in @('UsageDateTime','UsageDate','Date')) {
      if ($row.PSObject.Properties.Name -contains $propName) {
        $v = Convert-ToUsageDateString -Value $row.$propName
        if (-not [string]::IsNullOrWhiteSpace($v)) { $candidates.Add($v) | Out-Null }
      }
    }

    foreach ($candidate in $candidates) {
      try {
        $dt = [datetime]::ParseExact($candidate, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
        if ($null -eq $maxDate -or $dt -gt $maxDate) { $maxDate = $dt }
      }
      catch {}
    }
  }

  if ($null -eq $maxDate) {
    throw "Não foi possível derivar UsageDate do conteúdo do fact_cost. Verifique as colunas UsageDateTime/UsageDate/Date."
  }

  return $maxDate.ToString('yyyy-MM-dd')
}

function Ensure-Folder {
  param([Parameter(Mandatory = $true)][string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path)) {
    throw "Ensure-Folder recebeu Path vazio."
  }

  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Login-Azure {
  Write-Host "🔐 Conectando com Managed Identity..."

  if (-not (Get-Command Connect-AzAccount -ErrorAction SilentlyContinue)) {
    try {
      Import-Module Az.Accounts -ErrorAction Stop
    }
    catch {
      throw "O módulo Az.Accounts não está disponível no worker. Instale o módulo Az.Accounts ou Az. Detalhe: $($_.Exception.Message)"
    }
  }

  if (Get-Command Disable-AzContextAutosave -ErrorAction SilentlyContinue) {
    Disable-AzContextAutosave -Scope Process | Out-Null
  }

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

  if (-not (Test-Path $LocalPath)) {
    throw "Arquivo local não encontrado para upload: $LocalPath"
  }

  Write-Host "⬆️ Upload: $Container/$BlobPath"
  Set-AzStorageBlobContent -Context $Ctx -Container $Container -File $LocalPath -Blob $BlobPath -Force -ErrorAction Stop | Out-Null
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

  Get-AzStorageBlobContent -Context $Ctx -Container $Container -Blob $BlobName -Destination $outPath -Force -ErrorAction Stop | Out-Null
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
    throw "Nenhum arquivo encontrado para '$Label' em '$Container/$prefix'."
  }

  $blob = $blobs |
    Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending |
    Select-Object -First 1

  Write-Host "📌 Blob encontrado ($Label): $($blob.Name)"
  return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $OutFolder)
}

function Download-ReferenceBlobFile {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$Prefix,
    [Parameter(Mandatory = $true)][string]$FileName,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$OutFolder
  )

  $blobName = ($Prefix.TrimEnd("/") + "/" + $FileName.TrimStart("/"))
  Write-Host "🔎 Procurando '$Label' em '$Container/$blobName'..."

  $blob = Get-AzStorageBlob -Context $Ctx -Container $Container -Blob $blobName -ErrorAction SilentlyContinue
  if (-not $blob) {
    throw "Arquivo de referência não encontrado: $Container/$blobName"
  }

  Write-Host "📌 Blob encontrado ($Label): $blobName"
  return (Download-Blob -Ctx $Ctx -Container $Container -BlobName $blobName -OutFolder $OutFolder)
}

function Normalize-Text([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  return ($s + "").Trim([char]0xFEFF).Trim().Trim('"')
}

function Normalize-Comparable([string]$s) {
  $v = Normalize-Text $s
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }
  $v = $v.ToLowerInvariant()
  $v = $v -replace '[^a-z0-9]+', '-'
  $v = $v.Trim('-')
  return $v
}

function Compress-Comparable([string]$s) {
  $v = Normalize-Comparable $s
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }
  return ($v -replace '-', '')
}

function Parse-DoubleInvariant([object]$s) {
  if ($null -eq $s) { return 0.0 }

  if ($s -is [double] -or $s -is [float] -or $s -is [decimal] -or $s -is [int] -or $s -is [long]) {
    return [double]$s
  }

  $text = ("{0}" -f $s).Trim()
  if ([string]::IsNullOrWhiteSpace($text)) { return 0.0 }

  $inv = [System.Globalization.CultureInfo]::InvariantCulture
  $x = $text.Replace(",", ".")
  [double]$v = 0
  [double]::TryParse($x, [System.Globalization.NumberStyles]::Any, $inv, [ref]$v) | Out-Null
  return $v
}
$CsvCulture = [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR')

function Format-DecimalForCsv([object]$s) {
  [double]$v = Parse-DoubleInvariant $s
  return $v.ToString('0.######', $CsvCulture)
}


function Safe-Sum {
  param(
    [Parameter(Mandatory = $false)]
    $Collection,

    [Parameter(Mandatory = $true)]
    [string]$Property
  )

  if ($null -eq $Collection) { return 0.0 }

  $list = @($Collection)
  if ($list.Count -eq 0) { return 0.0 }

  $measure = $list | Measure-Object -Property $Property -Sum
  if ($null -eq $measure) { return 0.0 }

  $sumProp = $measure.PSObject.Properties['Sum']
  if ($null -eq $sumProp) { return 0.0 }
  if ($null -eq $sumProp.Value) { return 0.0 }

  return Parse-DoubleInvariant $sumProp.Value
}

function Safe-SumValues {
  param(
    [Parameter(Mandatory = $false)]
    $Collection
  )

  if ($null -eq $Collection) { return 0.0 }

  $list = @($Collection)
  if ($list.Count -eq 0) { return 0.0 }

  $measure = $list | Measure-Object -Sum
  if ($null -eq $measure) { return 0.0 }

  $sumProp = $measure.PSObject.Properties['Sum']
  if ($null -eq $sumProp) { return 0.0 }
  if ($null -eq $sumProp.Value) { return 0.0 }

  return Parse-DoubleInvariant $sumProp.Value
}

function Get-FactCostClusterDailyCost {
  param(
    [Parameter(Mandatory = $true)]$Rows,
    [Parameter(Mandatory = $true)][string]$UsageDate,
    [Parameter(Mandatory = $true)][string]$ClusterName
  )

  $clusterNeedle = Normalize-Text $ClusterName
  if ([string]::IsNullOrWhiteSpace($clusterNeedle)) { return 0.0 }
  $clusterNeedle = $clusterNeedle.ToLowerInvariant()

  $sum = 0.0
  foreach ($r in $Rows) {
    $rowDate = ''
    if ($r.PSObject.Properties.Name -contains 'Date') { $rowDate = Normalize-Text $r.Date }
    if ($rowDate -ne $UsageDate) { continue }

    $resourceId = ''
    if ($r.PSObject.Properties.Name -contains 'ResourceId') { $resourceId = Normalize-Text $r.ResourceId }
    if ([string]::IsNullOrWhiteSpace($resourceId)) { continue }

    if ($resourceId.ToLowerInvariant().Contains($clusterNeedle)) {
      $sum += Parse-DoubleInvariant $r.Cost
    }
  }

  return [Math]::Round($sum, 6)
}

function Convert-ForCsvExport {
  param(
    [Parameter(Mandatory = $true)]$Rows,
    [Parameter(Mandatory = $true)][string[]]$DecimalColumns
  )

  $result = New-Object System.Collections.Generic.List[object]
  foreach ($row in $Rows) {
    $ht = [ordered]@{}
    foreach ($prop in $row.PSObject.Properties) {
      if ($DecimalColumns -contains $prop.Name) {
        $ht[$prop.Name] = Format-DecimalForCsv $prop.Value
      }
      else {
        $ht[$prop.Name] = $prop.Value
      }
    }
    $result.Add([PSCustomObject]$ht) | Out-Null
  }

  return @(Convert-ToSafeArray $result)
}

function Set-UnsafeSslIfRequested {
  param([bool]$Enabled)

  if (-not $Enabled) { return }

  Add-Type @"
using System.Net;
using System.Security.Cryptography.X509Certificates;
public class TrustAllCertsPolicy : ICertificatePolicy {
    public bool CheckValidationResult(
        ServicePoint srvPoint, X509Certificate certificate,
        WebRequest request, int certificateProblem) {
        return true;
    }
}
"@ -ErrorAction SilentlyContinue

  [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
}

function Test-OpenCostConnectivity {
  param([Parameter(Mandatory = $true)][string]$BaseUrl)

  $uri = $BaseUrl.TrimEnd("/")
  try { $u = [System.Uri]$uri }
  catch { throw "AllocationUrl inválida: $BaseUrl" }

  Write-Host "🔌 Testando conectividade TCP em $($u.Host):$($u.Port)..."

  if (Get-Command Test-NetConnection -ErrorAction SilentlyContinue) {
    $tcp = Test-NetConnection -ComputerName $u.Host -Port $u.Port -WarningAction SilentlyContinue
    if (-not $tcp.TcpTestSucceeded) {
      throw "Sem conectividade TCP com $($u.Host):$($u.Port)"
    }
  }
  else {
    try {
      $client = New-Object System.Net.Sockets.TcpClient
      $async = $client.BeginConnect($u.Host, $u.Port, $null, $null)
      $ok = $async.AsyncWaitHandle.WaitOne(5000, $false)
      if (-not $ok -or -not $client.Connected) {
        $client.Close()
        throw "Sem conectividade TCP com $($u.Host):$($u.Port)"
      }
      $client.Close()
    }
    catch {
      throw "Sem conectividade TCP com $($u.Host):$($u.Port)"
    }
  }

  Write-Host "✅ TCP OK: $($u.Host):$($u.Port)"
}

function Invoke-OpenCostAllocation {
  param(
    [Parameter(Mandatory = $true)][string]$BaseUrl,
    [Parameter(Mandatory = $true)][string]$UsageDate,
    [Parameter(Mandatory = $false)][string]$BearerToken,
    [Parameter(Mandatory = $false)][int]$TimeoutSec = 60,
    [Parameter(Mandatory = $false)][int]$RetryCount = 2,
    [Parameter(Mandatory = $false)][int]$RetryDelaySeconds = 5
  )

  $base = $BaseUrl.TrimEnd("/")

  try {
    $start = [datetime]::ParseExact($UsageDate, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
  }
  catch {
    throw "Date inválida. Use yyyy-MM-dd. Valor recebido: $UsageDate"
  }

  $end = $start.AddDays(1)
  $window = '{0:yyyy-MM-dd}T00:00:00Z,{1:yyyy-MM-dd}T00:00:00Z' -f $start, $end

  $candidateUrls = @(
    "$base/allocation/compute?window=$window&aggregate=namespace",
    "$base/allocation?window=$window&aggregate=namespace"
  )

  $headers = @{}
  if (-not [string]::IsNullOrWhiteSpace($BearerToken)) {
    $headers["Authorization"] = "Bearer $BearerToken"
  }

  $lastError = $null

  foreach ($url in $candidateUrls) {
    for ($attempt = 1; $attempt -le ($RetryCount + 1); $attempt++) {
      Write-Host ("🌐 OpenCost attempt {0}: {1}" -f $attempt, $url)
      try {
        if ($headers.Count -gt 0) {
          return Invoke-RestMethod -Method Get -Uri $url -Headers $headers -TimeoutSec $TimeoutSec
        }
        else {
          return Invoke-RestMethod -Method Get -Uri $url -TimeoutSec $TimeoutSec
        }
      }
      catch {
        $lastError = $_
        Write-Warning ("Falha na chamada OpenCost. Attempt={0}. Detalhe: {1}" -f $attempt, $_.Exception.Message)
        if ($attempt -lt ($RetryCount + 1)) {
          Start-Sleep -Seconds $RetryDelaySeconds
        }
      }
    }
  }

  throw "Falha ao consultar OpenCost em '$BaseUrl'. Último erro: $($lastError.Exception.Message)"
}

function Extract-OpenCostRows {
  param(
    [Parameter(Mandatory = $true)]$Response,
    [Parameter(Mandatory = $true)][string]$ClusterName,
    [Parameter(Mandatory = $true)][string]$ServiceKey,
    [Parameter(Mandatory = $true)][string]$UsageDate
  )

  $rows = @()

  if ($null -eq $Response -or $null -eq $Response.data) {
    return $rows
  }

  $dataNode = $Response.data
  $items = @()

  if ($dataNode -is [System.Collections.IEnumerable] -and -not ($dataNode -is [string])) {
    foreach ($entry in $dataNode) {
      if ($entry -ne $null) { $items += $entry }
    }
  }
  else {
    $items += $dataNode
  }

  foreach ($container in $items) {
    if ($null -eq $container) { continue }

    foreach ($prop in $container.PSObject.Properties) {
      $key = $prop.Name
      $item = $prop.Value
      if ($null -eq $item) { continue }

      $namespace = ''
      if ($key -and $key -notmatch '^(code|status|message)$') {
        $namespace = Normalize-Text $key
      }

      if ([string]::IsNullOrWhiteSpace($namespace)) {
        $hasNamespace = @($item.PSObject.Properties | Where-Object { $_.Name -eq 'namespace' }).Count -gt 0
        $hasName      = @($item.PSObject.Properties | Where-Object { $_.Name -eq 'name' }).Count -gt 0

        if ($hasNamespace -and -not [string]::IsNullOrWhiteSpace(($item.namespace + ''))) {
          $namespace = Normalize-Text ($item.namespace + '')
        }
        elseif ($hasName -and -not [string]::IsNullOrWhiteSpace(($item.name + ''))) {
          $namespace = Normalize-Text ($item.name + '')
        }
      }

      if ([string]::IsNullOrWhiteSpace($namespace)) { continue }

      $totalCost = 0.0
      $hasTotalCost = @($item.PSObject.Properties | Where-Object { $_.Name -eq 'totalCost' }).Count -gt 0
      $hasTotalMonthlyCost = @($item.PSObject.Properties | Where-Object { $_.Name -eq 'totalMonthlyCost' }).Count -gt 0

      if ($hasTotalCost -and $item.totalCost -ne $null) {
        $totalCost = Parse-DoubleInvariant ($item.totalCost.ToString())
      }
      elseif ($hasTotalMonthlyCost -and $item.totalMonthlyCost -ne $null) {
        $totalCost = Parse-DoubleInvariant ($item.totalMonthlyCost.ToString())
      }
      else {
        $cpuCost = 0.0
        $ramCost = 0.0
        $gpuCost = 0.0
        $pvCost = 0.0
        $networkCost = 0.0
        $lbCost = 0.0

        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'cpuCost' }).Count -gt 0 -and $item.cpuCost -ne $null) { $cpuCost = Parse-DoubleInvariant ($item.cpuCost.ToString()) }
        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'ramCost' }).Count -gt 0 -and $item.ramCost -ne $null) { $ramCost = Parse-DoubleInvariant ($item.ramCost.ToString()) }
        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'gpuCost' }).Count -gt 0 -and $item.gpuCost -ne $null) { $gpuCost = Parse-DoubleInvariant ($item.gpuCost.ToString()) }
        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'pvCost' }).Count -gt 0 -and $item.pvCost -ne $null) { $pvCost = Parse-DoubleInvariant ($item.pvCost.ToString()) }
        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'networkCost' }).Count -gt 0 -and $item.networkCost -ne $null) { $networkCost = Parse-DoubleInvariant ($item.networkCost.ToString()) }
        if (@($item.PSObject.Properties | Where-Object { $_.Name -eq 'loadBalancerCost' }).Count -gt 0 -and $item.loadBalancerCost -ne $null) { $lbCost = Parse-DoubleInvariant ($item.loadBalancerCost.ToString()) }

        $totalCost = $cpuCost + $ramCost + $gpuCost + $pvCost + $networkCost + $lbCost
      }

      $rows += [PSCustomObject]@{
        Date       = $UsageDate
        Cluster    = $ClusterName
        ServiceKey = $ServiceKey
        Namespace  = Normalize-Text $namespace
        Cost       = [Math]::Round($totalCost, 6)
      }
    }
  }

  return $rows
}

function Normalize-NamespaceAlias {
  param([string]$Namespace)

  $ns = Normalize-Comparable $Namespace
  $patternsToRemove = @('^ns-','^app-','^cliente-','-prod$','-prd$','-hml$','-dev$','-qa$','-stg$','-app$')
  foreach ($p in $patternsToRemove) {
    $ns = [regex]::Replace($ns, $p, '')
  }

  return $ns.Trim('-','_',' ')
}

function Get-SystemNamespaceDefaults {
  return @(
    '__unmounted__','app-routing-system','argocd','atlantis','cert-manager','ingress-nginx','kube-system','opencost','opensearch',
    'opentelemetry-operator-system','monitoramento','prometheus','grafana','external-dns','velero','azure-workload-identity-system',
    'cattle-system','gatekeeper-system','gravitee','multi-automatiza-qa','nsdocs','n8n','firewall','roteirizador'
  )
}

function Resolve-SystemNamespace {
  param(
    [string]$Namespace,
    [hashtable]$SystemExact,
    [string[]]$SystemContains
  )

  $ns = Normalize-Comparable $Namespace
  if ([string]::IsNullOrWhiteSpace($ns)) { return $false }
  if ($SystemExact.ContainsKey($ns)) { return $true }

  foreach ($k in $SystemContains) {
    if (-not [string]::IsNullOrWhiteSpace($k) -and $ns.Contains($k)) {
      return $true
    }
  }

  return $false
}

function Resolve-ClienteFromNamespace {
  param(
    [string]$Namespace,
    [hashtable]$AliasExact,
    [System.Collections.ArrayList]$AliasRules
  )

  $candidate = Normalize-NamespaceAlias -Namespace $Namespace
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    return [PSCustomObject]@{ Cliente = ''; Alias = ''; MatchType = 'EMPTY' }
  }

  if ($AliasExact.ContainsKey($candidate)) {
    return [PSCustomObject]@{ Cliente = $AliasExact[$candidate]; Alias = $candidate; MatchType = 'EXACT' }
  }

  $candidateCompressed = Compress-Comparable $candidate
  $best = $null

  foreach ($rule in $AliasRules) {
    $alias = $rule.Alias
    $aliasCompressed = $rule.AliasCompressed
    if ([string]::IsNullOrWhiteSpace($aliasCompressed)) { continue }

    $isMatch = $false
    if ($candidate -eq $alias) { $isMatch = $true }
    elseif ($candidate.StartsWith("$alias-")) { $isMatch = $true }
    elseif ($candidate.EndsWith("-$alias")) { $isMatch = $true }
    elseif ($candidate.Contains("-$alias-")) { $isMatch = $true }
    elseif ($candidateCompressed.Contains($aliasCompressed)) { $isMatch = $true }

    if ($isMatch) {
      if ($null -eq $best -or $rule.Priority -gt $best.Priority) {
        $best = $rule
      }
    }
  }

  if ($null -ne $best) {
    return [PSCustomObject]@{ Cliente = $best.Cliente; Alias = $best.Alias; MatchType = 'FUZZY' }
  }

  return [PSCustomObject]@{ Cliente = ''; Alias = $candidate; MatchType = 'UNKNOWN' }
}


function Normalize-Cliente {
  param([Parameter(Mandatory = $false)][AllowNull()][string]$Cliente)

  $normalized = Normalize-Text $Cliente
  if ([string]::IsNullOrWhiteSpace($normalized)) {
    return "UNKNOWN"
  }

  return $normalized
}

function Parse-AliasLineFallback {
  param([Parameter(Mandatory = $true)][string]$Line)

  $text = ($Line + "").Trim().Trim([char]0xFEFF)
  if ([string]::IsNullOrWhiteSpace($text)) { return $null }
  if ($text -match '^\s*Alias\s*;\s*Cliente\s*$') { return $null }

  if ($text -match '^\s*"?(?<alias>[^;"]+)"?\s*;\s*"?(?<cliente>[^"]+)"?\s*$') {
    return [PSCustomObject]@{ Alias = Normalize-Text $matches['alias']; Cliente = Normalize-Text $matches['cliente'] }
  }

  if ($text -match '^\s*"(?<alias>[^;"]+);""?(?<cliente>[^"]+)""?"\s*$') {
    return [PSCustomObject]@{ Alias = Normalize-Text $matches['alias']; Cliente = Normalize-Text $matches['cliente'] }
  }

  return $null
}

function Load-ClientesAliasRows {
  param([Parameter(Mandatory = $true)][string]$Path)

  if (-not (Test-Path $Path)) {
    throw "Arquivo de aliases não encontrado: $Path"
  }

  $rows = @()
  try { $rows = Import-Csv $Path -Delimiter ";" }
  catch { $rows = @() }

  $parsed = New-Object System.Collections.Generic.List[object]

  foreach ($r in $rows) {
    $rawAlias = ""
    $rawCliente = ""

    if ($null -ne $r) {
      if ($r.PSObject.Properties.Name -contains "Alias")   { $rawAlias = Normalize-Text $r.Alias }
      if ($r.PSObject.Properties.Name -contains "Cliente") { $rawCliente = Normalize-Text $r.Cliente }
      if (-not $rawAlias -and ($r.PSObject.Properties.Name -contains "alias")) { $rawAlias = Normalize-Text $r.alias }
      if (-not $rawCliente -and ($r.PSObject.Properties.Name -contains "cliente")) { $rawCliente = Normalize-Text $r.cliente }
    }

    if (-not [string]::IsNullOrWhiteSpace($rawAlias) -and -not [string]::IsNullOrWhiteSpace($rawCliente)) {
      $parsed.Add([PSCustomObject]@{ Alias = $rawAlias; Cliente = $rawCliente }) | Out-Null
      continue
    }

    if (-not [string]::IsNullOrWhiteSpace($rawAlias) -and [string]::IsNullOrWhiteSpace($rawCliente)) {
      $fallback = Parse-AliasLineFallback -Line $rawAlias
      if ($null -ne $fallback) { $parsed.Add($fallback) | Out-Null }
    }
  }

  if ($parsed.Count -eq 0) {
    $rawLines = Get-Content -Path $Path -Encoding UTF8
    foreach ($line in $rawLines) {
      $fallback = Parse-AliasLineFallback -Line $line
      if ($null -ne $fallback) { $parsed.Add($fallback) | Out-Null }
    }
  }

  $valid = @(
    $parsed | Where-Object {
      -not [string]::IsNullOrWhiteSpace((Normalize-Text $_.Alias)) -and
      -not [string]::IsNullOrWhiteSpace((Normalize-Text $_.Cliente))
    }
  )

  if (-not $valid -or $valid.Count -eq 0) {
    throw "ClientesAliasCsvPath inválido ou mal formatado. Nenhuma linha válida com Alias e Cliente foi carregada."
  }

  return $valid
}

function Get-ClusterConfigValue {
  param(
    [Parameter(Mandatory = $true)]$Row,
    [Parameter(Mandatory = $true)][string[]]$Names,
    [Parameter(Mandatory = $false)][string]$DefaultValue = ""
  )

  foreach ($n in $Names) {
    if ($Row.PSObject.Properties.Name -contains $n) {
      $v = Normalize-Text ($Row.PSObject.Properties[$n].Value + '')
      if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    }
  }

  return $DefaultValue
}

Set-UnsafeSslIfRequested -Enabled:$IgnoreSslErrors.IsPresent
Ensure-Folder -Path $TempFolder
Login-Azure
$ctx = Get-StorageContext -StorageAccountName $StorageAccountName

$preferredPartitionDate = Resolve-PipelinePartitionDate -ExplicitPipelineDate $PipelineDate -FallbackDate $Date
$PipelinePartitionDate = Get-LatestAvailablePartitionDate -Ctx $ctx -Container $FinopsContainer -Prefix $FactCostPrefix -PreferredDate $preferredPartitionDate
$MappingPartitionDate  = Get-LatestAvailablePartitionDate -Ctx $ctx -Container $FinopsContainer -Prefix $ResourceToServicePrefix -PreferredDate $PipelinePartitionDate
Write-Host "✅ Partição fact_cost encontrada            : $PipelinePartitionDate"
Write-Host "✅ Partição resource_to_service encontrada : $MappingPartitionDate"

Write-Host "`n== DOWNLOAD INPUTS =="

$opencostClustersPath = Download-ReferenceBlobFile -Ctx $ctx -Container $FinopsContainer -Prefix $ReferencePrefix -FileName $OpenCostClustersFileName -Label "opencost_clusters" -OutFolder $TempFolder
$clientesAliasPath = Download-ReferenceBlobFile -Ctx $ctx -Container $FinopsContainer -Prefix $ReferencePrefix -FileName $ClientesAliasFileName -Label "clientes_alias" -OutFolder $TempFolder
$systemNamespacesPath = Download-ReferenceBlobFile -Ctx $ctx -Container $FinopsContainer -Prefix $ReferencePrefix -FileName $SystemNamespacesFileName -Label "aks_system_namespaces" -OutFolder $TempFolder
$mappingPath = Download-BlobByExactDatePrefix -Ctx $ctx -Container $FinopsContainer -BasePrefix $ResourceToServicePrefix -Dt $MappingPartitionDate -NameRegex "resource_to_service_shared(_.*)?\.csv$" -Label "resource_to_service_shared" -OutFolder $TempFolder
$factCostPath = Download-BlobByExactDatePrefix -Ctx $ctx -Container $FinopsContainer -BasePrefix $FactCostPrefix -Dt $PipelinePartitionDate -NameRegex "fact_cost_.*\.csv$" -Label "fact_cost" -OutFolder $TempFolder

Write-Host "📥 OpenCost clusters : $opencostClustersPath"
Write-Host "📥 Clientes alias    : $clientesAliasPath"
Write-Host "📥 System namespaces : $systemNamespacesPath"
Write-Host "📥 RTS               : $mappingPath"
Write-Host "📥 Fact cost         : $factCostPath"

$mapping = @(Import-Csv $mappingPath -Delimiter ";")
$clusterConfig = @(Import-Csv $opencostClustersPath -Delimiter ";")
$aliasRows = @(Load-ClientesAliasRows -Path $clientesAliasPath)
$systemRows = @()
if (Test-Path $systemNamespacesPath) { $systemRows = @(Import-Csv $systemNamespacesPath -Delimiter ";") }
$factCostRows = @()
if (Test-Path $factCostPath) { $factCostRows = @(Import-Csv $factCostPath -Delimiter ";") }

if (-not $mapping -or $mapping.Count -eq 0) { throw "Mapping vazio." }
if (-not $clusterConfig -or $clusterConfig.Count -eq 0) { throw "OpenCostClustersCsvPath vazio." }
if (-not $aliasRows -or $aliasRows.Count -eq 0) { throw "ClientesAliasCsvPath vazio ou inválido." }
if (-not $factCostRows -or $factCostRows.Count -eq 0) { throw "fact_cost vazio." }

if ([string]::IsNullOrWhiteSpace($UsageDate)) {
  $UsageDate = Get-FactCostUsageDateFromFileName -Path $factCostPath
  if ([string]::IsNullOrWhiteSpace($UsageDate)) {
    $UsageDate = Get-MaxUsageDateFromFactCost -Rows $factCostRows
  }
}
else {
  if (-not (Test-DateString -Value $UsageDate)) {
    throw "Date inválida. Use yyyy-MM-dd. Valor recebido: $UsageDate"
  }
}

Write-Host "📅 PipelinePartitionDate final: $PipelinePartitionDate"
Write-Host "📅 UsageDate final           : $UsageDate"
$ExpectedPreviousUsageDate = (Get-BrazilNow).AddDays(-1).ToString("yyyy-MM-dd")
Write-Host "📅 Expected previous day     : $ExpectedPreviousUsageDate"
if ($UsageDate -ne $ExpectedPreviousUsageDate) {
  Write-Warning "A UsageDate derivada do fact_cost difere de D-1 do Brasil. Isso pode ocorrer por atraso/publicação acumulada do export. O script seguirá com a UsageDate real encontrada no CSV: $UsageDate"
}

$factCostRows = @(
  $factCostRows |
  Where-Object {
    $rowDate = $null
    foreach ($propName in @('UsageDateTime','UsageDate','Date')) {
      if ($_.PSObject.Properties.Name -contains $propName) {
        $rowDate = Convert-ToUsageDateString -Value $_.$propName
        if (-not [string]::IsNullOrWhiteSpace($rowDate)) { break }
      }
    }
    $rowDate -eq $UsageDate
  }
)

if (-not $factCostRows -or $factCostRows.Count -eq 0) {
  throw "Nenhuma linha do fact_cost corresponde à UsageDate derivada: $UsageDate"
}

$aksServiceKeys = @(
  $mapping |
  Where-Object { (Normalize-Text $_.ServiceKey).StartsWith("AKS:", [System.StringComparison]::OrdinalIgnoreCase) } |
  Select-Object -ExpandProperty ServiceKey -Unique |
  ForEach-Object { Normalize-Text $_ }
)

if (-not $aksServiceKeys -or $aksServiceKeys.Count -eq 0) {
  throw "Nenhum ServiceKey AKS encontrado no mapping."
}

$clusterByServiceKey = @{}
foreach ($c in $clusterConfig) {
  $enabled = Get-ClusterConfigValue -Row $c -Names @('Enabled','enabled') -DefaultValue 'true'
  if ($enabled -match '^(?i:false|0|no|n)$') { continue }

  $serviceKey = Get-ClusterConfigValue -Row $c -Names @('ServiceKey','servicekey','service_key')
  $clusterName = Get-ClusterConfigValue -Row $c -Names @('ClusterName','clustername','cluster_name')
  $allocationUrl = Get-ClusterConfigValue -Row $c -Names @('AllocationUrl','allocationurl','allocation_url','OpenCostUrl','OpenCostURL','Url')

  if (-not $serviceKey) {
    $props = @($c.PSObject.Properties.Name)
    if ($props.Count -ge 1) { $serviceKey = Normalize-Text ($c.PSObject.Properties[$props[0]].Value + '') }
    if (-not $clusterName -and $props.Count -ge 2) { $clusterName = Normalize-Text ($c.PSObject.Properties[$props[1]].Value + '') }
    if (-not $allocationUrl -and $props.Count -ge 3) { $allocationUrl = Normalize-Text ($c.PSObject.Properties[$props[2]].Value + '') }
  }

  if (-not $serviceKey) { continue }

  $clusterByServiceKey[$serviceKey] = [PSCustomObject]@{
    ServiceKey    = $serviceKey
    ClusterName   = $clusterName
    AllocationUrl = $allocationUrl
    Enabled       = $enabled
  }
}

$aliasExact = @{}
$aliasRules = New-Object System.Collections.ArrayList
foreach ($a in $aliasRows) {
  $alias = Normalize-Comparable $a.Alias
  $cliente = Normalize-Text $a.Cliente
  if ([string]::IsNullOrWhiteSpace($alias) -or [string]::IsNullOrWhiteSpace($cliente)) { continue }

  $aliasExact[$alias] = $cliente
  [void]$aliasRules.Add([PSCustomObject]@{
    Alias           = $alias
    AliasCompressed = Compress-Comparable $alias
    Cliente         = $cliente
    Priority        = (Compress-Comparable $alias).Length
  })
}
$aliasRules = $aliasRules | Sort-Object Priority -Descending

if ($aliasExact.Count -eq 0) {
  throw "Nenhum alias válido foi carregado do arquivo de clientes."
}

$systemExact = @{}
foreach ($ns in (Get-SystemNamespaceDefaults)) {
  $n = Normalize-Comparable $ns
  if (-not [string]::IsNullOrWhiteSpace($n)) { $systemExact[$n] = $true }
}

$systemContains = @('ingress','monitor','otel','telemetry','prometheus','grafana','argocd','cert-manager','opencost','opensearch','routing','kube-','cattle','firewall','gravitee','roteirizador','nsdocs','n8n','atlantis','multi-automatiza-qa')
foreach ($r in $systemRows) {
  $nsValue = ''
  if ($r.PSObject.Properties.Name -contains 'Namespace') { $nsValue = $r.Namespace }
  elseif ($r.PSObject.Properties.Name -contains 'namespace') { $nsValue = $r.namespace }
  else {
    $props = @($r.PSObject.Properties.Name)
    if ($props.Count -ge 1) { $nsValue = $r.PSObject.Properties[$props[0]].Value }
  }

  $candidate = Normalize-Comparable $nsValue
  if ([string]::IsNullOrWhiteSpace($candidate)) { continue }

  $matchMode = ''
  if ($r.PSObject.Properties.Name -contains 'MatchMode') { $matchMode = ($r.MatchMode + '').ToLowerInvariant() }
  elseif ($r.PSObject.Properties.Name -contains 'matchmode') { $matchMode = ($r.matchmode + '').ToLowerInvariant() }

  if ($matchMode -eq 'contains') {
    $systemContains += $candidate
  }
  else {
    $systemExact[$candidate] = $true
  }
}

Write-Host "`n== CLUSTERS TO PROCESS =="
$clustersToProcess = @(
  $aksServiceKeys |
  ForEach-Object {
    $sk = $_
    if ($clusterByServiceKey.ContainsKey($sk)) { $clusterByServiceKey[$sk] }
  } |
  Where-Object { $_ -and $_.AllocationUrl }
)
$clustersToProcess | Format-Table -AutoSize

$clusterDailyCostByServiceKey = @{}
foreach ($cfg in $clustersToProcess) {
  $clusterDailyCostByServiceKey[$cfg.ServiceKey] = Get-FactCostClusterDailyCost -Rows $factCostRows -UsageDate $UsageDate -ClusterName $cfg.ClusterName
}

$allDetail = New-Object System.Collections.Generic.List[object]
$allNamespaceDetailAll = New-Object System.Collections.Generic.List[object]
$allUnknownNamespaces = New-Object System.Collections.Generic.List[object]
$allSystemNamespaces = New-Object System.Collections.Generic.List[object]
$clusterDiagnostics = New-Object System.Collections.Generic.List[object]

foreach ($sk in $aksServiceKeys) {
  if (-not $clusterByServiceKey.ContainsKey($sk)) {
    Write-Warning "⚠ ServiceKey sem configuração OpenCost: $sk"
    $clusterDiagnostics.Add([PSCustomObject]@{ Date = $UsageDate; ClusterName = ''; ServiceKey = $sk; Status = 'MISSING_CONFIG'; Notes = 'ServiceKey sem configuração OpenCost'; NamespaceCnt = 0; TotalCost = 0 }) | Out-Null
    continue
  }

  $cfg = $clusterByServiceKey[$sk]
  if ([string]::IsNullOrWhiteSpace($cfg.AllocationUrl)) {
    Write-Warning "⚠ AllocationUrl vazio para ServiceKey: $sk"
    $clusterDiagnostics.Add([PSCustomObject]@{ Date = $UsageDate; ClusterName = $cfg.ClusterName; ServiceKey = $sk; Status = 'MISSING_URL'; Notes = 'AllocationUrl vazio'; NamespaceCnt = 0; TotalCost = 0 }) | Out-Null
    continue
  }

  Write-Host ""
  Write-Host "============================================================"
  Write-Host "Cluster    : $($cfg.ClusterName)"
  Write-Host "ServiceKey : $($cfg.ServiceKey)"
  Write-Host "OpenCost   : $($cfg.AllocationUrl)"
  Write-Host "============================================================"

  try {
    Test-OpenCostConnectivity -BaseUrl $cfg.AllocationUrl

    $resp = Invoke-OpenCostAllocation -BaseUrl $cfg.AllocationUrl -UsageDate $UsageDate -BearerToken $BearerToken -TimeoutSec $OpenCostTimeoutSec -RetryCount $RetryCount -RetryDelaySeconds $RetryDelaySeconds
    $rows = @(Extract-OpenCostRows -Response $resp -ClusterName $cfg.ClusterName -ServiceKey $cfg.ServiceKey -UsageDate $UsageDate)

    Write-Host "📦 Linhas retornadas do OpenCost: $($rows.Count)"

    $validRows = @($rows | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Namespace) -and $_.Cost -gt 0 })
    Write-Host "📦 Linhas válidas após filtro: $($validRows.Count)"

    if (-not $validRows -or $validRows.Count -eq 0) {
      $clusterDiagnostics.Add([PSCustomObject]@{
        Date         = $UsageDate
        ClusterName  = $cfg.ClusterName
        ServiceKey   = $cfg.ServiceKey
        Status       = 'NO_VALID_ROWS'
        Notes        = 'OpenCost retornou 0 linhas válidas com namespace/custo > 0'
        NamespaceCnt = 0
        TotalCost    = 0
      }) | Out-Null
      continue
    }

    foreach ($r in $validRows) {
      $resolved = Resolve-ClienteFromNamespace -Namespace $r.Namespace -AliasExact $aliasExact -AliasRules $aliasRules

      $cliente = ''
      $classification = 'UNKNOWN'
      $alias = $resolved.Alias
      $matchType = $resolved.MatchType

      if (-not [string]::IsNullOrWhiteSpace($resolved.Cliente)) {
        $cliente = Normalize-Text $resolved.Cliente
        $classification = 'CLIENT'
      }
      elseif (Resolve-SystemNamespace -Namespace $r.Namespace -SystemExact $systemExact -SystemContains $systemContains) {
        $cliente = 'INFRA_PSL'
        $classification = 'SYSTEM'
        $matchType = 'SYSTEM'

        $allSystemNamespaces.Add([PSCustomObject]@{
          Date           = $r.Date
          Cluster        = $r.Cluster
          ServiceKey     = $r.ServiceKey
          Namespace      = $r.Namespace
          Alias          = $alias
          MatchType      = $matchType
          Cliente        = $cliente
          Classification = $classification
          Cost           = $r.Cost
        }) | Out-Null
      }
      else {
        $cliente = 'UNKNOWN'
        $classification = 'UNKNOWN'
        $matchType = 'UNKNOWN'

        $allUnknownNamespaces.Add([PSCustomObject]@{
          Date           = $r.Date
          Cluster        = $r.Cluster
          ServiceKey     = $r.ServiceKey
          Namespace      = $r.Namespace
          Alias          = $alias
          MatchType      = $matchType
          Cliente        = $cliente
          Classification = $classification
          Cost           = $r.Cost
        }) | Out-Null
      }

      $allDetail.Add([PSCustomObject]@{
        Date           = $r.Date
        Cluster        = $r.Cluster
        ServiceKey     = $r.ServiceKey
        Namespace      = $r.Namespace
        Alias          = $alias
        Cliente        = $cliente
        Classification = $classification
        MatchType      = $matchType
        Cost           = $r.Cost
      }) | Out-Null
    }

    $clusterDiagnostics.Add([PSCustomObject]@{
      Date         = $UsageDate
      ClusterName  = $cfg.ClusterName
      ServiceKey   = $cfg.ServiceKey
      Status       = 'PROCESSED'
      Notes        = ''
      NamespaceCnt = $validRows.Count
      TotalCost    = [Math]::Round((Safe-Sum -Collection $validRows -Property 'Cost'), 6)
    }) | Out-Null
  }
  catch {
    Write-Warning "Falha no cluster '$($cfg.ClusterName)'. Detalhe: $($_.Exception.Message)"
    $clusterDiagnostics.Add([PSCustomObject]@{
      Date         = $UsageDate
      ClusterName  = $cfg.ClusterName
      ServiceKey   = $cfg.ServiceKey
      Status       = 'FAILED'
      Notes        = $_.Exception.Message
      NamespaceCnt = 0
      TotalCost    = 0
    }) | Out-Null
    continue
  }
}

$allDetailArray = Convert-ToSafeArray -InputObject $allDetail
$allUnknownNamespacesArray = Convert-ToSafeArray -InputObject $allUnknownNamespaces
$allSystemNamespacesArray = Convert-ToSafeArray -InputObject $allSystemNamespaces
$clusterDiagnosticsArray = Convert-ToSafeArray -InputObject $clusterDiagnostics

Write-Host "`n== DIAGNOSTIC SUMMARY =="
if ($clusterDiagnosticsArray.Count -eq 0) { Write-Host '⚠ Nenhum cluster processado.' }
else { $clusterDiagnosticsArray | Format-Table -AutoSize }

if (-not $allDetailArray -or $allDetailArray.Count -eq 0) {
  throw "Nenhum cluster OpenCost gerou linhas válidas."
}

Write-Host "`n== BUILD SHARE =="
$allocation = @()
$byServiceKey = $allDetailArray | Group-Object ServiceKey
foreach ($g in $byServiceKey) {
  $serviceKey = $g.Name
  $rows = @($g.Group)
  if (-not $rows -or $rows.Count -eq 0) { continue }

  $firstRow = $rows[0]
  $clusterName = Normalize-Text $firstRow.Cluster
  $clusterDailyCost = 0.0
  if ($clusterDailyCostByServiceKey.ContainsKey($serviceKey)) {
    $clusterDailyCost = Parse-DoubleInvariant $clusterDailyCostByServiceKey[$serviceKey]
  }

  $clientRows = @($rows | Where-Object { $_.Classification -eq 'CLIENT' })
  $systemRowsCluster = @($rows | Where-Object { $_.Classification -eq 'SYSTEM' })
  $unknownRowsCluster = @($rows | Where-Object { $_.Classification -eq 'UNKNOWN' })

  $clientOpenCost = (Safe-Sum -Collection $clientRows -Property 'Cost')
  $systemOpenCost = (Safe-Sum -Collection $systemRowsCluster -Property 'Cost')
  $unknownOpenCost = (Safe-Sum -Collection $unknownRowsCluster -Property 'Cost')
  $eligibleOpenCost = $clientOpenCost + $systemOpenCost

  foreach ($d in $clusterDiagnosticsArray | Where-Object { $_.ServiceKey -eq $serviceKey }) {
    $openCostTotal = Parse-DoubleInvariant $d.TotalCost
    $d | Add-Member -NotePropertyName OpenCostTotalCost -NotePropertyValue ([Math]::Round($openCostTotal, 6)) -Force
    $d | Add-Member -NotePropertyName ClusterDailyCost -NotePropertyValue ([Math]::Round($clusterDailyCost, 6)) -Force
    $d | Add-Member -NotePropertyName OpenCostClientCost -NotePropertyValue ([Math]::Round($clientOpenCost, 6)) -Force
    $d | Add-Member -NotePropertyName OpenCostSystemCost -NotePropertyValue ([Math]::Round($systemOpenCost, 6)) -Force
    $d | Add-Member -NotePropertyName OpenCostUnknownCost -NotePropertyValue ([Math]::Round($unknownOpenCost, 6)) -Force
    $d | Add-Member -NotePropertyName OpenCostEligibleCost -NotePropertyValue ([Math]::Round($eligibleOpenCost, 6)) -Force
    $d | Add-Member -NotePropertyName CostGapClusterMinusOpenCost -NotePropertyValue ([Math]::Round(($clusterDailyCost - $openCostTotal), 6)) -Force
  }

  if ($clientRows.Count -eq 0 -or $clientOpenCost -le 0) {
    Write-Warning "⚠ ServiceKey $serviceKey sem namespaces CLIENT elegíveis para redistribuir SYSTEM."
    continue
  }

  if ($eligibleOpenCost -le 0) {
    Write-Warning "⚠ ServiceKey $serviceKey sem OpenCost elegível (>0)."
    continue
  }

  $namespaceDetailRows = New-Object System.Collections.Generic.List[object]
  $clientBuckets = @{}

  foreach ($r in $clientRows) {
    $namespaceOpenCost = Parse-DoubleInvariant $r.Cost
    $baseNamespaceShare = if ($clientOpenCost -gt 0) { $namespaceOpenCost / $clientOpenCost } else { 0.0 }
    $systemRedistributedCost = $systemOpenCost * $baseNamespaceShare
    $effectiveOpenCost = $namespaceOpenCost + $systemRedistributedCost
    $finalShare = if ($eligibleOpenCost -gt 0) { $effectiveOpenCost / $eligibleOpenCost } else { 0.0 }
    $allocatedClusterCost = $clusterDailyCost * $finalShare

    $clienteKey = Normalize-Cliente $r.Cliente
    if ([string]::IsNullOrWhiteSpace($clienteKey)) {
      $clienteKey = "UNKNOWN"
    }

    if (-not $clientBuckets.ContainsKey($clienteKey)) {
      $clientBuckets[$clienteKey] = [ordered]@{
        Date                       = $UsageDate
        ClusterName                = $clusterName
        ServiceKey                 = $serviceKey
        Cliente                    = $clienteKey
        NamespaceCount             = 0
        WeightRaw                  = 0.0
        NamespaceOpenCost          = 0.0
        SystemRedistributedCost    = 0.0
        EffectiveOpenCost          = 0.0
        ClusterDailyCost           = $clusterDailyCost
      }
    }

    $bucket = $clientBuckets[$clienteKey]
    $bucket.NamespaceCount = [int]$bucket.NamespaceCount + 1
    $bucket.WeightRaw = [double]$bucket.WeightRaw + [double]$effectiveOpenCost
    $bucket.NamespaceOpenCost = [double]$bucket.NamespaceOpenCost + [double]$namespaceOpenCost
    $bucket.SystemRedistributedCost = [double]$bucket.SystemRedistributedCost + [double]$systemRedistributedCost
    $bucket.EffectiveOpenCost = [double]$bucket.EffectiveOpenCost + [double]$effectiveOpenCost

    $namespaceDetailRows.Add([PSCustomObject]@{
      Date                    = $r.Date
      ClusterName             = $clusterName
      ServiceKey              = $serviceKey
      Namespace               = $r.Namespace
      Cliente                 = $clienteKey
      Alias                   = $r.Alias
      MatchType               = $r.MatchType
      Classification          = $r.Classification
      NamespaceOpenCost       = [Math]::Round($namespaceOpenCost, 6)
      BaseNamespaceShare      = [Math]::Round($baseNamespaceShare, 6)
      ClusterSystemOpenCost   = [Math]::Round($systemOpenCost, 6)
      SystemRedistributedCost = [Math]::Round($systemRedistributedCost, 6)
      EffectiveOpenCost       = [Math]::Round($effectiveOpenCost, 6)
      WeightRaw               = [Math]::Round($effectiveOpenCost, 6)
      WeightNormalizedBase    = [Math]::Round($baseNamespaceShare, 6)
      FinalShare              = [Math]::Round($finalShare, 6)
      ClusterDailyCost        = [Math]::Round($clusterDailyCost, 6)
      AllocatedClusterCost    = [Math]::Round($allocatedClusterCost, 6)
      AllocationMethod        = 'AKS_NAMESPACE_EFFECTIVE_OPENCOST_NORMALIZED'
      Notes                   = 'namespace_weight_normalized_with_system_redistribution'
    }) | Out-Null
  }

  foreach ($nsRow in (Convert-ToSafeArray -InputObject $namespaceDetailRows)) {
    $allNamespaceDetailAll.Add($nsRow) | Out-Null
  }

  $clientAgg = @()
  foreach ($clienteKey in ($clientBuckets.Keys | Sort-Object)) {
    $bucket = $clientBuckets[$clienteKey]
    $weightRaw = Parse-DoubleInvariant $bucket.WeightRaw
    $finalShare = if ($eligibleOpenCost -gt 0) { $weightRaw / $eligibleOpenCost } else { 0.0 }
    $allocatedClusterCost = $clusterDailyCost * $finalShare

    $clientAgg += [PSCustomObject]@{
      Date                 = $UsageDate
      ClusterName          = $clusterName
      ServiceKey           = $serviceKey
      Cliente              = $clienteKey
      NamespaceCount       = [int]$bucket.NamespaceCount
      WeightRaw            = [Math]::Round($weightRaw, 6)
      WeightNormalizedBase = [Math]::Round($finalShare, 6)
      OpenCostClientCost   = [Math]::Round((Parse-DoubleInvariant $bucket.NamespaceOpenCost), 6)
      OpenCostSystemCost   = [Math]::Round((Parse-DoubleInvariant $bucket.SystemRedistributedCost), 6)
      OpenCostEligibleCost = [Math]::Round($eligibleOpenCost, 6)
      ClusterDailyCost     = [Math]::Round($clusterDailyCost, 6)
      AllocatedClusterCost = [Math]::Round($allocatedClusterCost, 6)
      Share                = [Math]::Round($finalShare, 6)
      DriverType           = 'AKS_EFFECTIVE_OPENCOST_NORMALIZED'
      AllocationMethod     = 'AKS_SCRIPT_08_NORMALIZED'
      Notes                = 'share=effective_opencost/sum(effective_opencost) by ServiceKey'
    }
  }

  $shareSumBeforeFix = Safe-Sum -Collection $clientAgg -Property 'Share'
  if (($clientAgg.Count -gt 0) -and ([Math]::Abs($shareSumBeforeFix - 1.0) -gt 0.000001)) {
    foreach ($row in $clientAgg) {
      $normalizedShare = if ($shareSumBeforeFix -gt 0) { (Parse-DoubleInvariant $row.Share) / $shareSumBeforeFix } else { 0.0 }
      $row.Share = [Math]::Round($normalizedShare, 12)
      $row.WeightNormalizedBase = [Math]::Round($normalizedShare, 12)
      $row.AllocatedClusterCost = [Math]::Round(($clusterDailyCost * $normalizedShare), 6)
      $row.Notes = ($row.Notes + '|renormalized_after_aggregation')
    }
  }

  $shareSumAfterFix = Safe-Sum -Collection $clientAgg -Property 'Share'
  if ([Math]::Abs($shareSumAfterFix - 1.0) -gt 0.001) {
    throw "Share inválido para ServiceKey '$serviceKey'. Soma final = $([Math]::Round($shareSumAfterFix, 12))"
  }

  $badShareRows = @($clientAgg | Where-Object { (Parse-DoubleInvariant $_.Share) -gt 1.0 -or (Parse-DoubleInvariant $_.Share) -lt 0.0 })
  if ($badShareRows.Count -gt 0) {
    throw "Share fora do intervalo [0,1] para ServiceKey '$serviceKey'."
  }

  foreach ($r in $clientAgg) {
    $allocation += $r
  }
}

if (-not $allocation -or $allocation.Count -eq 0) {
  throw "Nenhum allocation AKS foi gerado."
}
Write-Host "`n== EXPORT =="
$outDetail    = Join-Path $TempFolder ("aks_opencost_detail_{0}.csv" -f $UsageDate)
$outDetailAll = Join-Path $TempFolder ("aks_namespace_detail_all_{0}.csv" -f $UsageDate)
$outShare     = Join-Path $TempFolder ("aks_allocation_share_all_{0}.csv" -f $UsageDate)
$outUnknown   = Join-Path $TempFolder ("aks_unknown_namespaces_{0}.csv" -f $UsageDate)
$outSystem    = Join-Path $TempFolder ("aks_system_namespaces_{0}.csv" -f $UsageDate)
$outDiag      = Join-Path $TempFolder ("aks_cluster_diagnostics_{0}.csv" -f $UsageDate)

$detailExport = Convert-ForCsvExport -Rows $allDetailArray -DecimalColumns @('Cost')
$detailAllExport = Convert-ForCsvExport -Rows (Convert-ToSafeArray -InputObject $allNamespaceDetailAll) -DecimalColumns @('NamespaceOpenCost','BaseNamespaceShare','ClusterSystemOpenCost','SystemRedistributedCost','EffectiveOpenCost','WeightRaw','WeightNormalizedBase','FinalShare','ClusterDailyCost','AllocatedClusterCost')
$shareExport = Convert-ForCsvExport -Rows $allocation -DecimalColumns @('WeightRaw','WeightNormalizedBase','OpenCostClientCost','OpenCostSystemCost','OpenCostEligibleCost','ClusterDailyCost','AllocatedClusterCost','Share')
$unknownExport = Convert-ForCsvExport -Rows $allUnknownNamespacesArray -DecimalColumns @('Cost')
$systemExport = Convert-ForCsvExport -Rows $allSystemNamespacesArray -DecimalColumns @('Cost')
$diagExport = Convert-ForCsvExport -Rows $clusterDiagnosticsArray -DecimalColumns @('TotalCost','OpenCostTotalCost','ClusterDailyCost','OpenCostClientCost','OpenCostSystemCost','OpenCostUnknownCost','OpenCostEligibleCost','CostGapClusterMinusOpenCost')

$detailExport | Export-Csv -Path $outDetail -Delimiter ';' -NoTypeInformation -Encoding UTF8
$detailAllExport | Export-Csv -Path $outDetailAll -Delimiter ';' -NoTypeInformation -Encoding UTF8
$shareExport | Export-Csv -Path $outShare -Delimiter ';' -NoTypeInformation -Encoding UTF8
$unknownExport | Export-Csv -Path $outUnknown -Delimiter ';' -NoTypeInformation -Encoding UTF8
$systemExport | Export-Csv -Path $outSystem -Delimiter ';' -NoTypeInformation -Encoding UTF8
$diagExport | Export-Csv -Path $outDiag -Delimiter ';' -NoTypeInformation -Encoding UTF8

Write-Host "✅ Detail      : $outDetail"
Write-Host "✅ Detail All  : $outDetailAll"
Write-Host "✅ Share       : $outShare"
Write-Host "✅ Unknown     : $outUnknown"
Write-Host "✅ System      : $outSystem"
Write-Host "✅ Diag        : $outDiag"

# IMPORTANTE:
# A pasta dt= da saída representa a data corrente de publicação/processamento no Brasil.
# A competência real do dado continua no nome do arquivo via UsageDate.
# Isso preserva a convenção esperada para a consolidação do Script 09:
#   dt=YYYY-MM-DD -> dia da publicação
#   *_YYYY-MM-DD.csv -> dia real do custo alocado
# PipelinePartitionDate é usado apenas para localizar os insumos disponíveis.
$OutputPartitionDate = $PublishPartitionDate
Write-Host "📦 OutputPartitionDate final    : $OutputPartitionDate"
Write-Host "📦 UsageDate nos arquivos      : $UsageDate"

$blobDetail    = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outDetail -Leaf)"
$blobDetailAll = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outDetailAll -Leaf)"
$blobShare     = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outShare -Leaf)"
$blobUnknown   = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outUnknown -Leaf)"
$blobSystem    = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outSystem -Leaf)"
$blobDiag      = "$OutPrefix/dt=$OutputPartitionDate/$(Split-Path $outDiag -Leaf)"

Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outDetail -BlobPath $blobDetail
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outDetailAll -BlobPath $blobDetailAll
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outShare -BlobPath $blobShare
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outUnknown -BlobPath $blobUnknown
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outSystem -BlobPath $blobSystem
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $outDiag -BlobPath $blobDiag

Write-Host "`n== VALIDATION =="
$validation = @(
  $allocation |
  Group-Object ServiceKey |
  ForEach-Object {
    $sum = (Safe-SumValues -Collection ($_.Group | ForEach-Object { Parse-DoubleInvariant $_.Share }))
    [PSCustomObject]@{
      ServiceKey = $_.Name
      ShareSum   = [Math]::Round($sum, 6)
      Rows       = $_.Count
    }
  }
)

$bad = @($validation | Where-Object { [Math]::Abs($_.ShareSum - 1.0) -gt 0.01 })
Write-Host ("📌 ServiceKeys AKS processados      : {0}" -f $validation.Count)
Write-Host ("📌 ServiceKeys com soma fora de ~1  : {0}" -f $bad.Count)
Write-Host ("📌 Namespaces system                : {0}" -f @($allSystemNamespacesArray).Count)
Write-Host ("📌 Namespaces não reconhecidos      : {0}" -f @($allUnknownNamespacesArray).Count)

if ($bad.Count -gt 0) {
  Write-Host "`n⚠ Exemplos com soma divergente:"
  $bad | Select-Object -First 20 | Format-Table -AutoSize
}
else {
  Write-Host "✅ Todos os ServiceKeys AKS estão com share somando aproximadamente 1."
}

$sharesAboveOne = @($allocation | Where-Object { (Parse-DoubleInvariant $_.Share) -gt 1.0 })
$sharesAbsurd = @($allocation | Where-Object { (Parse-DoubleInvariant $_.Share) -gt 10.0 })
if ($sharesAboveOne.Count -gt 0) {
  throw ("Existem linhas AKS com Share > 1. Exemplo: " + (($sharesAboveOne | Select-Object -First 5 | ForEach-Object { "{0}/{1}={2}" -f $_.ServiceKey, $_.Cliente, $_.Share }) -join "; "))
}
if ($sharesAbsurd.Count -gt 0) {
  throw ("Existem linhas AKS com Share absurdamente alto (>10). Exemplo: " + (($sharesAbsurd | Select-Object -First 5 | ForEach-Object { "{0}/{1}={2}" -f $_.ServiceKey, $_.Cliente, $_.Share }) -join "; "))
}

Write-Host "`n== SUMMARY =="
$allocation | Sort-Object ServiceKey, Share -Descending | Format-Table -AutoSize
