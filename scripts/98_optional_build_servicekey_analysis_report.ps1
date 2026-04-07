<#
README
------
Script: 04_5_build_servicekey_analysis_report.ps1

Função:
Gerar a camada de análise e observabilidade das ServiceKeys do pipeline de FinOps.

O script lê o resource_to_service da data informada e produz:
1. Detalhamento dos componentes por ServiceKey
2. Relatório analítico consolidado por ServiceKey
3. Candidatos de escopo para rateio genérico
4. Composição financeira por ServiceKey com base no fact_cost
5. Breakdown por ResourceType e por ResourceGroupName

Objetivo:
Ajudar na validação do mapping, na governança do modelo e no troubleshooting de ServiceKeys com custo alto,
cobertura incompleta ou comportamento inesperado.

Observações:
- AKS continua fora do rateio genérico e deve ser tratado pelo script 08
- SQLPOOL continua fora do rateio genérico e deve ser tratado pelo script 07
- Este script não aloca custo; ele apenas analisa e explica a composição das ServiceKeys
- A composição financeira incorporada aqui substitui a necessidade do antigo script 92
#>

param(
  [Parameter(Mandatory = $true)]
  [string]$Date,

  [Parameter(Mandatory = $false)]
  [string]$ResourceToServiceCsvPath = "",

  [Parameter(Mandatory = $false)]
  [bool]$UseLatestResourceToServiceFromLake = $true,

  [Parameter(Mandatory = $false)]
  [string]$FactCostCsvPath = "",

  [Parameter(Mandatory = $false)]
  [bool]$UseLatestFactCostFromLake = $true,

  [Parameter(Mandatory = $false)]
  [string]$ServiceKeys = ""
)

. "$PSScriptRoot\config.ps1"

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Date = ($Date + "").Trim().Trim('"').Trim("'")
$UsageDate = $Date
$PipelinePartitionDate = $Date

$ResourceToServicePrefix = "silver/resource_to_service"
$FactCostPrefix          = "silver/fact_cost"
$OutPrefix               = "gold/servicekey_analysis"

function Normalize-Text($s) {
  if ($null -eq $s) { return "" }
  $v = ($s + "")
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }
  return $v.Trim([char]0xFEFF).Trim().Trim('"')
}

function Normalize-Key([string]$s) {
  $v = Normalize-Text $s
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }
  return $v.TrimEnd("/").ToLowerInvariant()
}

function Normalize-Cliente($s) {
  $v = (Normalize-Text $s).ToUpperInvariant()
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }

  switch ($v) {
    "KMM"           { return "INFRA_PSL" }
    "COMPARTILHADO" { return "COMPARTILHADO" }
    "UNKNOWN"       { return "UNKNOWN" }
    "DEFAULT"       { return "DEFAULT" }
    default         { return $v }
  }
}

function Parse-DoubleInvariant($s) {
  $txt = Normalize-Text $s
  if ([string]::IsNullOrWhiteSpace($txt)) { return 0.0 }

  $inv = [System.Globalization.CultureInfo]::InvariantCulture
  $txt = $txt.Replace(",", ".")
  [double]$v = 0.0
  [void][double]::TryParse($txt, [System.Globalization.NumberStyles]::Any, $inv, [ref]$v)
  return $v
}

function Test-HeaderContainsAll {
  param(
    [string]$CsvPath,
    [string[]]$ExpectedColumns
  )

  if (-not (Test-Path $CsvPath)) { return $false }

  $line = Get-Content -Path $CsvPath -TotalCount 1 -ErrorAction SilentlyContinue
  $line = Normalize-Text $line
  if ([string]::IsNullOrWhiteSpace($line)) { return $false }

  foreach ($col in @($ExpectedColumns)) {
    if ($line -notmatch ('(?i)(^|;|")' + [regex]::Escape($col) + '(;|$|")')) {
      return $false
    }
  }

  return $true
}

function Download-BlobByExactDatePrefix {
  param(
    [Parameter(Mandatory = $true)]$Ctx,
    [Parameter(Mandatory = $true)][string]$Container,
    [Parameter(Mandatory = $true)][string]$BasePrefix,
    [Parameter(Mandatory = $true)][string]$Dt,
    [Parameter(Mandatory = $true)][string]$NameRegex,
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $false)][string[]]$ExpectedColumns = @()
  )

  $Dt = Normalize-Text $Dt
  $prefix = "$BasePrefix/dt=$Dt/"
  Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

  $candidateBlobs = @(
    Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefix |
    Where-Object {
      $leaf = Split-Path $_.Name -Leaf
      $leaf -match $NameRegex
    } |
    Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending
  )

  if (@($candidateBlobs).Count -eq 0) {
    throw "Nenhum CSV encontrado para '$Label' em '$Container/$prefix'."
  }

  foreach ($candidate in @($candidateBlobs)) {
    $downloaded = Download-Blob -Ctx $Ctx -Container $Container -BlobName $candidate.Name -OutFolder $FinopsTempFolder

    if (@($ExpectedColumns).Count -eq 0) {
      Write-Host "📌 Blob encontrado ($Label): $($candidate.Name)"
      return $downloaded
    }

    if (Test-HeaderContainsAll -CsvPath $downloaded -ExpectedColumns $ExpectedColumns) {
      Write-Host "📌 Blob encontrado ($Label): $($candidate.Name)"
      return $downloaded
    }

    Write-Host "⚠️ Ignorando blob sem colunas esperadas: $($candidate.Name)"
  }

  throw "Foram encontrados blobs em '$Container/$prefix', mas nenhum parece ser um CSV válido para '$Label'."
}

function Get-FirstFilledPropertyValue {
  param(
    $Row,
    [string[]]$Names
  )

  foreach ($name in @($Names)) {
    if ($Row.PSObject.Properties.Name -contains $name) {
      $value = Normalize-Text $Row.$name
      if (-not [string]::IsNullOrWhiteSpace($value)) {
        return $value
      }
    }
  }

  return ""
}

function Get-RowCostValue {
  param($Row)

  $candidates = @("Cost","AmortizedCost","PretaxCost","NetCost","EffectiveCost","FinalCost","AllocatedCost")

  foreach ($name in $candidates) {
    if ($Row.PSObject.Properties.Name -contains $name) {
      return (Parse-DoubleInvariant $Row.$name)
    }
  }

  return 0.0
}

function Get-ServiceKeyPrefix($serviceKey) {
  $v = Normalize-Text $serviceKey
  if ([string]::IsNullOrWhiteSpace($v)) { return "" }
  $parts = $v.Split(':')
  if (@($parts).Count -ge 1) { return ($parts[0].ToUpperInvariant()) }
  return ""
}

function Join-TopValues {
  param(
    $Rows,
    [string]$PropertyName,
    [int]$Top = 10
  )

  $vals = @(
    $Rows |
    Where-Object {
      $tmp = ""
      if ($_.PSObject.Properties.Name -contains $PropertyName) {
        $tmp = Normalize-Text $_.$PropertyName
      }
      -not [string]::IsNullOrWhiteSpace($tmp)
    } |
    Group-Object $PropertyName |
    Sort-Object Count -Descending |
    Select-Object -First $Top |
    ForEach-Object { Normalize-Text $_.Name }
  )

  return ((@($vals)) -join " | ")
}

function Get-Recommendation {
  param(
    [string]$ServiceKey,
    [string]$ServiceType,
    [string[]]$DirectClients,
    [bool]$HasSharedMarkers,
    [bool]$HasInfraPslOnly,
    [string]$TopResourceTypes,
    [int]$DistinctDirectClientCount
  )

  $prefix = Normalize-Text (Get-ServiceKeyPrefix $ServiceKey)
  $svcTypeNorm = (Normalize-Text $ServiceType).ToUpperInvariant()
  $topTypes = (Normalize-Text $TopResourceTypes).ToUpperInvariant()

  if ($prefix -eq "AKS" -or $svcTypeNorm -eq "AKS") {
    return [PSCustomObject]@{
      SuggestedAllocationMode = "AKS_SCRIPT_08"
      NeedsScopeEntry         = "NO"
      RecommendationReason    = "AKS deve ser rateado no script 08 por namespace/OpenCost."
    }
  }

  if ($prefix -eq "SQLPOOL" -or $svcTypeNorm -eq "SQLPOOL") {
    return [PSCustomObject]@{
      SuggestedAllocationMode = "SQLPOOL_SCRIPT_07"
      NeedsScopeEntry         = "NO"
      RecommendationReason    = "SQL Elastic Pool deve ser rateado no script 07, não pelo rateio genérico."
    }
  }

  if ($DistinctDirectClientCount -eq 1 -and -not $HasSharedMarkers) {
    return [PSCustomObject]@{
      SuggestedAllocationMode = "DEDICATED"
      NeedsScopeEntry         = "NO"
      RecommendationReason    = "Há apenas um cliente direto detectado sem marcador de compartilhado. Ajuste tag/recurso e deixe Dedicated."
    }
  }

  if ($DistinctDirectClientCount -ge 2) {
    return [PSCustomObject]@{
      SuggestedAllocationMode = "ELIGIBLE_CLIENTS"
      NeedsScopeEntry         = "YES"
      RecommendationReason    = "Há múltiplos clientes diretos detectados no mesmo ServiceKey. Recomenda-se restringir rateio ao subconjunto elegível."
    }
  }

  if ($HasInfraPslOnly -or $HasSharedMarkers) {
    return [PSCustomObject]@{
      SuggestedAllocationMode = "GLOBAL_WEIGHT"
      NeedsScopeEntry         = "OPTIONAL"
      RecommendationReason    = "ServiceKey sem cliente direto único confiável; tratar como compartilhado global até existir evidência de subconjunto elegível."
    }
  }

  return [PSCustomObject]@{
    SuggestedAllocationMode = "GLOBAL_WEIGHT"
    NeedsScopeEntry         = "OPTIONAL"
    RecommendationReason    = "Fallback padrão para ServiceKey compartilhado sem regra específica."
  }
}

Ensure-AzLogin
$ctx = Get-StorageContextByName -saName $StorageAccountName
Ensure-Folder -Path $FinopsTempFolder

Write-Host "`n== LOAD RESOURCE_TO_SERVICE =="

if ($UseLatestResourceToServiceFromLake) {
  $mappingPath = Download-BlobByExactDatePrefix `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -BasePrefix $ResourceToServicePrefix `
    -Dt $UsageDate `
    -NameRegex '^resource_to_service(_shared)?_.*\.csv$' `
    -Label 'resource_to_service' `
    -ExpectedColumns @("ResourceId", "ServiceKey")
}
else {
  if ([string]::IsNullOrWhiteSpace($ResourceToServiceCsvPath)) {
    throw "Se UseLatestResourceToServiceFromLake=`$false, informe -ResourceToServiceCsvPath."
  }
  if (-not (Test-Path $ResourceToServiceCsvPath)) {
    throw "Arquivo resource_to_service não encontrado: $ResourceToServiceCsvPath"
  }
  $mappingPath = $ResourceToServiceCsvPath
}

Write-Host "`n== LOAD FACT_COST =="

if ($UseLatestFactCostFromLake) {
  $factCostPath = Download-BlobByExactDatePrefix `
    -Ctx $ctx `
    -Container $FinopsContainer `
    -BasePrefix $FactCostPrefix `
    -Dt $UsageDate `
    -NameRegex '^fact_cost_.*\.csv$' `
    -Label 'fact_cost' `
    -ExpectedColumns @("ResourceId")
}
else {
  if ([string]::IsNullOrWhiteSpace($FactCostCsvPath)) {
    throw "Se UseLatestFactCostFromLake=`$false, informe -FactCostCsvPath."
  }
  if (-not (Test-Path $FactCostCsvPath)) {
    throw "Arquivo fact_cost não encontrado: $FactCostCsvPath"
  }
  $factCostPath = $FactCostCsvPath
}

Write-Host "📥 resource_to_service: $mappingPath"
Write-Host "📥 fact_cost         : $factCostPath"

$mapping = @(Import-Csv -Path $mappingPath -Delimiter ";")
if (@($mapping).Count -eq 0) {
  throw "resource_to_service vazio/não lido: $mappingPath"
}

$factCost = @(Import-Csv -Path $factCostPath -Delimiter ";")
if (@($factCost).Count -eq 0) {
  throw "fact_cost vazio/não lido: $factCostPath"
}

$serviceKeyFilter = @{}
if (-not [string]::IsNullOrWhiteSpace($ServiceKeys)) {
  foreach ($p in ($ServiceKeys -split '[,;\r\n]+')) {
    $sk = Normalize-Text $p
    if (-not [string]::IsNullOrWhiteSpace($sk)) {
      $serviceKeyFilter[$sk.ToUpperInvariant()] = $true
    }
  }
}

$detailRows = New-Object System.Collections.ArrayList
$rowIndex = 0

foreach ($row in @($mapping)) {
  $rowIndex++

  try {
    $serviceKey = Get-FirstFilledPropertyValue -Row $row -Names @("ServiceKey", "SuggestedServiceKey")
    if ([string]::IsNullOrWhiteSpace($serviceKey)) { continue }

    if ($serviceKeyFilter.Count -gt 0 -and -not $serviceKeyFilter.ContainsKey($serviceKey.ToUpperInvariant())) { continue }

    $resourceId    = Get-FirstFilledPropertyValue -Row $row -Names @("ResourceId", "Id")
    $resourceGroup = Get-FirstFilledPropertyValue -Row $row -Names @("ResourceGroupName", "ResourceGroup")
    $resourceName  = Get-FirstFilledPropertyValue -Row $row -Names @("ResourceName", "Name")
    $resourceType  = Get-FirstFilledPropertyValue -Row $row -Names @("ResourceType", "Type")
    $serviceType   = Get-FirstFilledPropertyValue -Row $row -Names @("ServiceType")

    if ([string]::IsNullOrWhiteSpace($serviceType)) {
      $serviceType = Get-ServiceKeyPrefix $serviceKey
    }

    $clienteOriginal = Get-FirstFilledPropertyValue -Row $row -Names @(
      "ClienteOriginal",
      "ClienteNormalizado",
      "Cliente",
      "FinopsCliente",
      "FINOPS-CLIENTE"
    )
    $clienteNormalizado = Normalize-Cliente $clienteOriginal

    [void]$detailRows.Add([PSCustomObject]@{
      Date               = $UsageDate
      ServiceKey         = $serviceKey
      ServiceType        = $serviceType
      ResourceGroupName  = $resourceGroup
      ResourceName       = $resourceName
      ResourceType       = $resourceType
      ResourceId         = $resourceId
      ClienteOriginal    = $clienteOriginal
      ClienteNormalizado = $clienteNormalizado
      SharedMarker       = "YES"
    })
  }
  catch {
    Write-Warning ("Falha ao processar linha #{0}: {1}" -f $rowIndex, $_.Exception.Message)
  }
}

if (@($detailRows).Count -eq 0) {
  throw "Nenhuma linha encontrada para análise de ServiceKey."
}

Write-Host ("📌 Linhas para análise: {0}" -f @($detailRows).Count)

Write-Host "`n== BUILD SERVICEKEY ANALYSIS =="

$summaryRows = New-Object System.Collections.ArrayList
$scopeCandidateRows = New-Object System.Collections.ArrayList

$grouped = @($detailRows | Group-Object ServiceKey | Sort-Object Name)

foreach ($g in @($grouped)) {
  $serviceKey = Normalize-Text $g.Name
  $rows = @($g.Group)
  if (@($rows).Count -eq 0) { continue }

  $serviceType = Normalize-Text $rows[0].ServiceType
  if ([string]::IsNullOrWhiteSpace($serviceType)) {
    $serviceType = Normalize-Text (Get-ServiceKeyPrefix $serviceKey)
  }

  $resourceCount = @($rows).Count

  $distinctResourceGroups = @(
    $rows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceGroupName) } |
    Select-Object -ExpandProperty ResourceGroupName -Unique
  )
  $distinctResources = @(
    $rows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceId) } |
    Select-Object -ExpandProperty ResourceId -Unique
  )
  $resourceTypes = @(
    $rows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceType) } |
    Select-Object -ExpandProperty ResourceType -Unique |
    Sort-Object
  )
  $resourceGroups = @(
    $rows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceGroupName) } |
    Select-Object -ExpandProperty ResourceGroupName -Unique |
    Sort-Object
  )

  $directClients = @(
    foreach ($rowItem in @($rows)) {
      $clienteValue = Get-FirstFilledPropertyValue -Row $rowItem -Names @("ClienteNormalizado","ClienteOriginal")
      $c = Normalize-Cliente $clienteValue
      if (-not [string]::IsNullOrWhiteSpace($c) -and $c -notin @("COMPARTILHADO","UNKNOWN","INFRA_PSL","DEFAULT")) {
        $c
      }
    }
  ) | Sort-Object -Unique

  $allClients = @(
    foreach ($rowItem in @($rows)) {
      $clienteValue = Get-FirstFilledPropertyValue -Row $rowItem -Names @("ClienteNormalizado","ClienteOriginal")
      $c = Normalize-Cliente $clienteValue
      if (-not [string]::IsNullOrWhiteSpace($c)) {
        $c
      }
    }
  ) | Sort-Object -Unique

  $sharedClients = @($allClients | Where-Object { $_ -in @("COMPARTILHADO","UNKNOWN","INFRA_PSL","DEFAULT") })
  $hasSharedMarkers = @($sharedClients).Count -gt 0
  $nonInfraPslClients = @($allClients | Where-Object { $_ -ne "INFRA_PSL" })
  $hasInfraPslOnly = (@($directClients).Count -eq 0 -and @($allClients).Count -gt 0 -and @($nonInfraPslClients).Count -eq 0)

  $topResourceTypes  = Join-TopValues -Rows $rows -PropertyName "ResourceType" -Top 8
  $topResourceGroups = Join-TopValues -Rows $rows -PropertyName "ResourceGroupName" -Top 8
  $topResources      = Join-TopValues -Rows $rows -PropertyName "ResourceName" -Top 12

  $rec = Get-Recommendation `
    -ServiceKey $serviceKey `
    -ServiceType $serviceType `
    -DirectClients @($directClients) `
    -HasSharedMarkers $hasSharedMarkers `
    -HasInfraPslOnly $hasInfraPslOnly `
    -TopResourceTypes $topResourceTypes `
    -DistinctDirectClientCount @($directClients).Count

  [void]$summaryRows.Add([PSCustomObject]@{
    Date                    = $UsageDate
    ServiceKey              = $serviceKey
    ServiceType             = $serviceType
    ResourceRows            = $resourceCount
    DistinctResources       = @($distinctResources).Count
    DistinctResourceGroups  = @($distinctResourceGroups).Count
    DistinctResourceTypes   = @($resourceTypes).Count
    DirectClientsDetected   = @($directClients).Count
    AllClientsDetected      = @($allClients).Count
    DirectClients           = (@($directClients) -join "|")
    AllClients              = (@($allClients) -join "|")
    SharedMarkersDetected   = (@($sharedClients) -join "|")
    TopResourceTypes        = $topResourceTypes
    TopResourceGroups       = (@($resourceGroups) -join "|")
    TopResources            = $topResources
    SuggestedAllocationMode = $rec.SuggestedAllocationMode
    NeedsScopeEntry         = $rec.NeedsScopeEntry
    RecommendationReason    = $rec.RecommendationReason
  })

  if ($rec.SuggestedAllocationMode -eq "ELIGIBLE_CLIENTS" -and @($directClients).Count -gt 0) {
    foreach ($client in @($directClients)) {
      [void]$scopeCandidateRows.Add([PSCustomObject]@{
        ServiceKey      = $serviceKey
        AllocationMode  = "ELIGIBLE_CLIENTS"
        Cliente         = $client
        Notes           = "AUTO_CANDIDATE_FROM_SERVICEKEY_ANALYSIS"
      })
    }
  }
  elseif ($rec.SuggestedAllocationMode -eq "GLOBAL_WEIGHT") {
    [void]$scopeCandidateRows.Add([PSCustomObject]@{
      ServiceKey      = $serviceKey
      AllocationMode  = "GLOBAL_WEIGHT"
      Cliente         = ""
      Notes           = "AUTO_CANDIDATE_FROM_SERVICEKEY_ANALYSIS"
    })
  }
}

if (@($summaryRows).Count -eq 0) {
  throw "Nenhum resumo de ServiceKey foi gerado."
}

$scopeCandidateRows = @($scopeCandidateRows | Sort-Object ServiceKey, AllocationMode, Cliente -Unique)

Write-Host ("📌 ServiceKeys analisadas: {0}" -f @($summaryRows).Count)

Write-Host "`n== BUILD SERVICEKEY COST COMPOSITION =="

$mapIndex = @{}

foreach ($m in @($mapping)) {
  $serviceKey = Get-FirstFilledPropertyValue -Row $m -Names @("ServiceKey", "SuggestedServiceKey")
  if ([string]::IsNullOrWhiteSpace($serviceKey)) { continue }

  if ($serviceKeyFilter.Count -gt 0 -and -not $serviceKeyFilter.ContainsKey($serviceKey.ToUpperInvariant())) { continue }

  $rid = Get-FirstFilledPropertyValue -Row $m -Names @("ResourceId", "Id")
  $ridKey = Normalize-Key $rid
  if ([string]::IsNullOrWhiteSpace($ridKey)) { continue }

  if (-not $mapIndex.ContainsKey($ridKey)) {
    $mapIndex[$ridKey] = [PSCustomObject]@{
      ServiceKey        = $serviceKey
      ResourceId        = $rid
      ResourceType      = Get-FirstFilledPropertyValue -Row $m -Names @("ResourceType", "Type")
      ResourceGroupName = Get-FirstFilledPropertyValue -Row $m -Names @("ResourceGroupName", "ResourceGroup")
      ResourceName      = Get-FirstFilledPropertyValue -Row $m -Names @("ResourceName", "Name")
    }
  }
}

$compositionDetailRows = New-Object System.Collections.ArrayList

foreach ($c in @($factCost)) {
  $rid = Get-FirstFilledPropertyValue -Row $c -Names @("ResourceId", "resourceId", "Id")
  $ridKey = Normalize-Key $rid
  if ([string]::IsNullOrWhiteSpace($ridKey)) { continue }

  if (-not $mapIndex.ContainsKey($ridKey)) { continue }

  $m = $mapIndex[$ridKey]
  $serviceKey = Normalize-Text $m.ServiceKey
  if ([string]::IsNullOrWhiteSpace($serviceKey)) { continue }

  $cost = Get-RowCostValue $c
  if ($cost -eq 0) { continue }

  [void]$compositionDetailRows.Add([PSCustomObject]@{
    Date              = $UsageDate
    ServiceKey        = $serviceKey
    ResourceId        = Normalize-Text $m.ResourceId
    ResourceType      = Normalize-Text $m.ResourceType
    ResourceGroupName = Normalize-Text $m.ResourceGroupName
    ResourceName      = Normalize-Text $m.ResourceName
    Cost              = [Math]::Round($cost, 6)
  })
}

Write-Host ("📌 Linhas de composição financeira: {0}" -f @($compositionDetailRows).Count)

$compositionByTypeRows = New-Object System.Collections.ArrayList
$compositionByRgRows   = New-Object System.Collections.ArrayList

$compositionGroupedBySk = @($compositionDetailRows | Group-Object ServiceKey | Sort-Object Name)

foreach ($g in @($compositionGroupedBySk)) {
  $serviceKey = Normalize-Text $g.Name
  $rows = @($g.Group)
  if (@($rows).Count -eq 0) { continue }

  $totalCost = [double](($rows | Measure-Object Cost -Sum).Sum)
  if ($totalCost -eq 0) { continue }

  $byType = @(
    $rows |
    Group-Object ResourceType |
    ForEach-Object {
      $groupCost = [double](($_.Group | Measure-Object Cost -Sum).Sum)
      [PSCustomObject]@{
        Date          = $UsageDate
        ServiceKey    = $serviceKey
        ResourceType  = Normalize-Text $_.Name
        ResourceCount = @($_.Group | Select-Object -ExpandProperty ResourceId -Unique).Count
        Cost          = [Math]::Round($groupCost, 6)
        CostPct       = [Math]::Round(($groupCost / $totalCost), 6)
        CostPct_100   = [Math]::Round((($groupCost / $totalCost) * 100), 4)
      }
    } |
    Sort-Object Cost -Descending
  )

  foreach ($row in @($byType)) {
    [void]$compositionByTypeRows.Add($row)
  }

  $byRg = @(
    $rows |
    Group-Object ResourceGroupName |
    ForEach-Object {
      $groupCost = [double](($_.Group | Measure-Object Cost -Sum).Sum)
      [PSCustomObject]@{
        Date              = $UsageDate
        ServiceKey        = $serviceKey
        ResourceGroupName = Normalize-Text $_.Name
        ResourceCount     = @($_.Group | Select-Object -ExpandProperty ResourceId -Unique).Count
        Cost              = [Math]::Round($groupCost, 6)
        CostPct           = [Math]::Round(($groupCost / $totalCost), 6)
        CostPct_100       = [Math]::Round((($groupCost / $totalCost) * 100), 4)
      }
    } |
    Sort-Object Cost -Descending
  )

  foreach ($row in @($byRg)) {
    [void]$compositionByRgRows.Add($row)
  }
}

Write-Host "`n== EXPORT =="

$detailFile            = Join-Path $FinopsTempFolder ("servicekey_components_detail_{0}.csv" -f $UsageDate)
$summaryFile           = Join-Path $FinopsTempFolder ("servicekey_analysis_report_{0}.csv" -f $UsageDate)
$scopeFile             = Join-Path $FinopsTempFolder ("servicekey_allocation_scope_candidates_{0}.csv" -f $UsageDate)
$compositionDetailFile = Join-Path $FinopsTempFolder ("servicekey_composition_detail_{0}.csv" -f $UsageDate)
$compositionTypeFile   = Join-Path $FinopsTempFolder ("servicekey_composition_by_type_{0}.csv" -f $UsageDate)
$compositionRgFile     = Join-Path $FinopsTempFolder ("servicekey_composition_by_rg_{0}.csv" -f $UsageDate)

$detailRows |
  Sort-Object ServiceKey, ResourceGroupName, ResourceType, ResourceName |
  Export-Csv -Path $detailFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$summaryRows |
  Sort-Object ServiceKey |
  Export-Csv -Path $summaryFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$scopeCandidateRows |
  Sort-Object ServiceKey, AllocationMode, Cliente |
  Export-Csv -Path $scopeFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$compositionDetailRows |
  Sort-Object ServiceKey, ResourceGroupName, ResourceType, ResourceName |
  Export-Csv -Path $compositionDetailFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$compositionByTypeRows |
  Sort-Object ServiceKey, @{Expression="Cost";Descending=$true}, ResourceType |
  Export-Csv -Path $compositionTypeFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

$compositionByRgRows |
  Sort-Object ServiceKey, @{Expression="Cost";Descending=$true}, ResourceGroupName |
  Export-Csv -Path $compositionRgFile -Delimiter ";" -NoTypeInformation -Encoding UTF8

Write-Host "✅ Detail                 : $detailFile"
Write-Host "✅ Summary                : $summaryFile"
Write-Host "✅ Scope                  : $scopeFile"
Write-Host "✅ Composition Detail     : $compositionDetailFile"
Write-Host "✅ Composition By Type    : $compositionTypeFile"
Write-Host "✅ Composition By RG      : $compositionRgFile"

$blobDetail            = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $detailFile -Leaf)"
$blobSummary           = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $summaryFile -Leaf)"
$blobScope             = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $scopeFile -Leaf)"
$blobCompositionDetail = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $compositionDetailFile -Leaf)"
$blobCompositionType   = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $compositionTypeFile -Leaf)"
$blobCompositionRg     = "$OutPrefix/dt=$PipelinePartitionDate/$(Split-Path $compositionRgFile -Leaf)"

Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $detailFile -BlobPath $blobDetail
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $summaryFile -BlobPath $blobSummary
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $scopeFile -BlobPath $blobScope
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $compositionDetailFile -BlobPath $blobCompositionDetail
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $compositionTypeFile -BlobPath $blobCompositionType
Upload-ToBlob -Ctx $ctx -Container $FinopsContainer -LocalPath $compositionRgFile -BlobPath $blobCompositionRg

Write-Host "⬆️ Upload:"
Write-Host " - $FinopsContainer/$blobDetail"
Write-Host " - $FinopsContainer/$blobSummary"
Write-Host " - $FinopsContainer/$blobScope"
Write-Host " - $FinopsContainer/$blobCompositionDetail"
Write-Host " - $FinopsContainer/$blobCompositionType"
Write-Host " - $FinopsContainer/$blobCompositionRg"

Write-Host "`n== SUMMARY =="
$summaryRows | Sort-Object ServiceKey | Select-Object -First 50 | Format-Table -AutoSize

Write-Host "`n== TOP SERVICEKEY COST COMPOSITION BY TYPE =="
$compositionByTypeRows | Sort-Object Cost -Descending | Select-Object -First 30 | Format-Table -AutoSize

Write-Host "`n== TOP SERVICEKEY COST COMPOSITION BY RG =="
$compositionByRgRows | Sort-Object Cost -Descending | Select-Object -First 30 | Format-Table -AutoSize