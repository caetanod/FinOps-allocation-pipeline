<#
Objetivo:
Mapear recursos compartilhados do inventário para uma ServiceKey de rateio.

Função no pipeline:
É a etapa central de classificação ResourceId -> ServiceKey, identificando a qual serviço/pool cada recurso compartilhado pertence.

Entrada:
- Inventário da data
- Overrides de RG (quando aplicável)

Saída:
- resource_to_service
- override_candidates_shared

Regras típicas:
- AKS / MC_* / managedClusters
- SQL Pool / SQL Database
- Backup / snapshots / restore structures
- Infra compartilhada
- Fallbacks por Resource Group e padrões de ResourceId

Observação:
É um dos scripts mais críticos do pipeline, pois define a base de ServiceKey usada nas etapas de share e alocação final.

############
  .\04_build_resource_to_service.ps1 -Date "2026-03-11" -UseLatestInventoryFromLake $true
############
#>

param(
    [Parameter(Mandatory = $false)]
    [string]$InventoryCsvPath = "",

    [Parameter(Mandatory = $false)]
    [bool]$UseLatestInventoryFromLake = $true,

    [Parameter(Mandatory = $false)]
    [string]$Date = "",

    [Parameter(Mandatory = $false)]
    [string]$PipelineDate = "",

    [Parameter(Mandatory = $false)]
    [string]$OverridesRgPath = "",

    [Parameter(Mandatory = $false)]
    [bool]$ExcludeNetworkWatcherRG = $true,

    [Parameter(Mandatory = $false)]
    [string]$StorageAccountName = "stpslkmmfinopseusprd",

    [Parameter(Mandatory = $false)]
    [string]$FinopsContainer = "finops",

    [Parameter(Mandatory = $false)]
    [string]$InventoryPrefix = "bronze/inventory_daily",

    [Parameter(Mandatory = $false)]
    [string]$OverridesPrefix = "silver/overrides_rg",

    [Parameter(Mandatory = $false)]
    [string]$ResourceToServicePrefix = "silver/resource_to_service",

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

$script:ResolvedProcessingDate = $null
$script:BrazilTimeZoneId = "E. South America Standard Time"

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

function Get-BrazilNow {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($script:BrazilTimeZoneId)
    return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

function Resolve-PreferredProcessingDate {
    param(
        [string]$Date,
        [string]$PipelineDate
    )

    if (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
        return ([datetime]::ParseExact($PipelineDate, 'yyyy-MM-dd', $null)).ToString('yyyy-MM-dd')
    }

    if (-not [string]::IsNullOrWhiteSpace($Date)) {
        return ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null)).ToString('yyyy-MM-dd')
    }

    return (Get-BrazilNow).ToString('yyyy-MM-dd')
}

function Get-DateCandidates {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PreferredDate,

        [int]$FallbackDays = 2
    )

    $base = [datetime]::ParseExact($PreferredDate, 'yyyy-MM-dd', $null)
    $list = New-Object System.Collections.Generic.List[string]

    for ($i = 0; $i -le $FallbackDays; $i++) {
        $candidate = $base.AddDays(-$i).ToString('yyyy-MM-dd')
        if (-not $list.Contains($candidate)) {
            $list.Add($candidate) | Out-Null
        }
    }

    return @($list.ToArray())
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

function Get-LatestCsvBlobFromPrefix {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix
    )

    $blobs = @(
        Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $Prefix -ErrorAction Stop |
        Where-Object { $_.Name -like '*.csv' }
    )

    if ($blobs.Count -eq 0) {
        return $null
    }

    return (
        $blobs |
        Sort-Object { $_.ICloudBlob.Properties.LastModified } -Descending |
        Select-Object -First 1
    )
}

function Download-BlobByResolvedDatePrefix {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$BasePrefix,
        [Parameter(Mandatory = $true)][string]$PreferredDate,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$OutFolder,
        [int]$FallbackDays = 2,
        [bool]$UseFallback = $true
    )

    $datesToTry = if ($UseFallback) {
        Get-DateCandidates -PreferredDate $PreferredDate -FallbackDays $FallbackDays
    }
    else {
        @($PreferredDate)
    }

    foreach ($dt in $datesToTry) {
        $prefix = "$BasePrefix/dt=$dt/"
        Write-Host "🔎 Procurando '$Label' em '$Container/$prefix'..."

        $blob = Get-LatestCsvBlobFromPrefix -Ctx $Ctx -Container $Container -Prefix $prefix
        if ($null -ne $blob) {
            Write-Host "📌 Blob encontrado ($Label): $($blob.Name)"
            $localPath = Download-Blob -Ctx $Ctx -Container $Container -BlobName $blob.Name -OutFolder $OutFolder
            return [PSCustomObject]@{
                Date      = $dt
                BlobName  = $blob.Name
                LocalPath = $localPath
            }
        }
    }

    $msg = if ($UseFallback) {
        "Nenhum CSV encontrado para '$Label' em '$BasePrefix' usando as partições: $($datesToTry -join ', ')."
    }
    else {
        "Nenhum CSV encontrado para '$Label' em '$BasePrefix/dt=$PreferredDate/'."
    }

    throw $msg
}

# ==========================================
# FUNÇÕES DE NEGÓCIO
# ==========================================
function Normalize-Text {
    param([string]$s)

    if ([string]::IsNullOrWhiteSpace($s)) { return "" }
    return ($s + "").Trim([char]0xFEFF).Trim()
}

function Get-SubscriptionIdFromResourceId {
    param([string]$ResourceId)

    $rid = Normalize-Text $ResourceId
    if ([string]::IsNullOrWhiteSpace($rid)) { return "" }

    if ($rid -match '(?i)/subscriptions/([^/]+)/') {
        return ($matches[1] + "").Trim()
    }

    return ""
}

function Get-McRgFinopsClienteMap {
    param(
        [Parameter(Mandatory = $true)]
        [array]$InventoryRows
    )

    $map = @{}
    $pairs = New-Object System.Collections.Generic.List[object]

    foreach ($row in $InventoryRows) {
        $rg = (Normalize-Text $row.ResourceGroupName).ToLowerInvariant()
        if ([string]::IsNullOrWhiteSpace($rg)) { continue }
        if (-not $rg.StartsWith("mc_", [System.StringComparison]::OrdinalIgnoreCase)) { continue }

        $subId = ""
        $props = @($row.PSObject.Properties.Name)
        if ($props -contains "SubscriptionId") {
            $subId = Normalize-Text $row.SubscriptionId
        }

        if ([string]::IsNullOrWhiteSpace($subId)) {
            $subId = Get-SubscriptionIdFromResourceId -ResourceId $row.ResourceId
        }

        if ([string]::IsNullOrWhiteSpace($subId)) { continue }

        $key = ("{0}|{1}" -f $subId.ToLowerInvariant(), $rg)
        if (-not $map.ContainsKey($key)) {
            $map[$key] = $null
            $pairs.Add([PSCustomObject]@{
                SubscriptionId    = $subId
                ResourceGroupName = $rg
            }) | Out-Null
        }
    }

    if ($pairs.Count -eq 0) {
        return $map
    }

    foreach ($pair in @($pairs.ToArray())) {
        $subId = Normalize-Text $pair.SubscriptionId
        $rg = (Normalize-Text $pair.ResourceGroupName).ToLowerInvariant()
        $key = ("{0}|{1}" -f $subId.ToLowerInvariant(), $rg)

        try {
            Select-AzSubscription -SubscriptionId $subId | Out-Null
            $rgObj = Get-AzResourceGroup -Name $rg -ErrorAction Stop

            $rgClient = ""
            if ($rgObj -and $rgObj.Tags) {
                foreach ($k in @($rgObj.Tags.Keys)) {
                    if ((Normalize-Text $k).ToUpperInvariant() -eq "FINOPS-CLIENTE") {
                        $rgClient = Normalize-Text $rgObj.Tags[$k]
                        break
                    }
                }
            }

            $map[$key] = $rgClient
        }
        catch {
            Write-Warning ("Não foi possível obter FINOPS-CLIENTE do RG '{0}' na subscription '{1}'. Detalhe: {2}" -f $rg, $subId, $_.Exception.Message)
            $map[$key] = ""
        }
    }

    return $map
}

function Get-EffectiveFinopsClienteInfo {
    param(
        [Parameter(Mandatory = $true)]
        $Row,
        [Parameter(Mandatory = $true)]
        $McRgFinopsClienteMap
    )

    $resourceClient = Normalize-Text $Row.'FINOPS-CLIENTE'
    $rg = (Normalize-Text $Row.ResourceGroupName).ToLowerInvariant()

    if (-not [string]::IsNullOrWhiteSpace($rg) -and $rg.StartsWith("mc_", [System.StringComparison]::OrdinalIgnoreCase)) {
        $subId = ""
        $props = @($Row.PSObject.Properties.Name)
        if ($props -contains "SubscriptionId") {
            $subId = Normalize-Text $Row.SubscriptionId
        }

        if ([string]::IsNullOrWhiteSpace($subId)) {
            $subId = Get-SubscriptionIdFromResourceId -ResourceId $Row.ResourceId
        }

        if (-not [string]::IsNullOrWhiteSpace($subId)) {
            $key = ("{0}|{1}" -f $subId.ToLowerInvariant(), $rg)
            if ($McRgFinopsClienteMap.ContainsKey($key)) {
                $rgClient = Normalize-Text $McRgFinopsClienteMap[$key]
                if (-not [string]::IsNullOrWhiteSpace($rgClient)) {
                    return [PSCustomObject]@{
                        EffectiveFinopsCliente = $rgClient
                        FinopsClienteSource    = "MC_RG"
                    }
                }
            }
        }
    }

    return [PSCustomObject]@{
        EffectiveFinopsCliente = $resourceClient
        FinopsClienteSource    = "RESOURCE"
    }
}

function Resolve-AksClusterFromMcRg {
    param([string]$rgLower)

    if ([string]::IsNullOrWhiteSpace($rgLower)) { return $null }
    if (-not $rgLower.StartsWith("mc_", [System.StringComparison]::OrdinalIgnoreCase)) { return $null }

    $rest  = $rgLower.Substring(3)
    $parts = $rest.Split("_")

    if ($parts.Count -ge 3) {
        return $parts[$parts.Count - 2]
    }

    return $null
}

function Extract-FromResourceId {
    param(
        [string]$rid,
        [string]$marker
    )

    if ([string]::IsNullOrWhiteSpace($rid)) { return $null }

    $idx = $rid.IndexOf($marker, [System.StringComparison]::OrdinalIgnoreCase)
    if ($idx -lt 0) { return $null }

    $start = $idx + $marker.Length
    $rest = $rid.Substring($start)
    $name = ($rest.Split("/"))[0]

    if ([string]::IsNullOrWhiteSpace($name)) { return $null }

    return $name
}

function Guess-ParentVmFromDiskName {
    param([string]$diskName)

    if ([string]::IsNullOrWhiteSpace($diskName)) { return $null }

    $m = [regex]::Match($diskName, "^(?<vm>.+?)_(OsDisk|DataDisk)_", "IgnoreCase")
    if ($m.Success) { return $m.Groups["vm"].Value }

    return $null
}

function Guess-ParentFromNicName {
    param([string]$nicName)

    if ([string]::IsNullOrWhiteSpace($nicName)) { return $null }

    $m = [regex]::Match($nicName, "^(?<vm>.+?)(-nic|_nic|nic-)", "IgnoreCase")
    if ($m.Success) { return $m.Groups["vm"].Value }

    return $null
}

function Test-IsAksInfraType {
    param([string]$type)

    $t = (Normalize-Text $type).ToLowerInvariant()

    return @(
        "microsoft.compute/virtualmachinescalesets",
        "microsoft.compute/disks",
        "microsoft.compute/snapshots",
        "microsoft.network/loadbalancers",
        "microsoft.network/publicipaddresses",
        "microsoft.network/networkinterfaces",
        "microsoft.managedidentity/userassignedidentities",
        "microsoft.compute/availabilitysets",
        "microsoft.compute/virtualmachines"
    ) -contains $t
}

function Test-IsSharedBackupRg {
    param([string]$rg)

    $x = (Normalize-Text $rg).ToLowerInvariant()
    return $x -eq "rg-mg-kmm-backup-datadisk-eu-prd"
}

function New-Row {
    param(
        $rid,$type,$name,$rg,$svcKey,$svcType,$rule,$confidence,$notes,$effectiveFinopsCliente,$finopsClienteSource
    )

    [PSCustomObject]@{
        Date                   = $Date
        ResourceId             = $rid
        ResourceType           = $type
        ResourceName           = $name
        ResourceGroupName      = $rg
        ServiceKey             = $svcKey
        ServiceType            = $svcType
        Rule                   = $rule
        Confidence             = $confidence
        EffectiveFinopsCliente = $effectiveFinopsCliente
        FinopsClienteSource    = $finopsClienteSource
        Notes                  = $notes
    }
}

function Detect-Delimiter {
    param([string]$filePath)

    $first = Get-Content -Path $filePath -TotalCount 1
    if (($first.Split(';').Count -gt $first.Split(',').Count)) { return ';' }
    return ','
}

function Load-RgOverrides {
    param([string]$path)

    $list = New-Object System.Collections.Generic.List[object]
    if (-not (Test-Path $path)) { return @() }

    $delim = Detect-Delimiter -filePath $path
    $raw = @(Import-Csv -Path $path -Delimiter $delim)

    foreach ($r in $raw) {
        $props = @($r.PSObject.Properties.Name)

        $pattern = ""
        $serviceKey = ""
        $serviceType = ""
        $notes = ""

        if (($props -contains "Pattern") -and ($props -contains "ServiceKey")) {
            $pattern     = Normalize-Text $r.Pattern
            $serviceKey  = Normalize-Text $r.ServiceKey
            $serviceType = Normalize-Text $r.ServiceType
            $notes       = Normalize-Text $r.Notes
        }
        else {
            $singleProp = $props | Select-Object -First 1
            $line = Normalize-Text $r.$singleProp
            if ([string]::IsNullOrWhiteSpace($line)) { continue }

            $parts = $line.Split($delim)
            if ($parts.Count -ge 2) {
                $pattern     = Normalize-Text $parts[0]
                $serviceKey  = Normalize-Text $parts[1]
                if ($parts.Count -ge 3) { $serviceType = Normalize-Text $parts[2] }
                if ($parts.Count -ge 4) { $notes = Normalize-Text ($parts[3..($parts.Count-1)] -join $delim) }
            }
        }

        if ([string]::IsNullOrWhiteSpace($pattern)) { continue }

        $obj = [PSCustomObject]@{
            Pattern     = $pattern
            ServiceKey  = $serviceKey
            ServiceType = $serviceType
            Notes       = $notes
        }
        $list.Add($obj) | Out-Null
    }

    return @($list.ToArray())
}

function Try-ApplyRgOverride {
    param(
        [string]$rgLower,
        $rgOverrides
    )

    $items = @($rgOverrides)
    if ($items.Count -eq 0) { return $null }

    foreach ($o in $items) {
        $pattern = Normalize-Text $o.Pattern
        if ([string]::IsNullOrWhiteSpace($pattern)) { continue }

        if ([regex]::IsMatch($rgLower, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
            return @{
                ServiceKey  = Normalize-Text $o.ServiceKey
                ServiceType = Normalize-Text $o.ServiceType
                Notes       = Normalize-Text $o.Notes
                Pattern     = $pattern
            }
        }
    }

    return $null
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

$preferredDate = Resolve-PreferredProcessingDate -Date $Date -PipelineDate $PipelineDate
Write-Host "🕒 Brasil now...............: $((Get-BrazilNow).ToString('yyyy-MM-dd HH:mm:ss'))"
Write-Host "📅 Preferred processing date: $preferredDate"
Write-Host "📌 UseLatestInventoryFromLake: $UseLatestInventoryFromLake"
Write-Host "📌 ExcludeNetworkWatcherRG: $ExcludeNetworkWatcherRG"

if ($UseLatestInventoryFromLake) {
    $inventoryBlobInfo = Download-BlobByResolvedDatePrefix `
        -Ctx $ctx `
        -Container $FinopsContainer `
        -BasePrefix $InventoryPrefix `
        -PreferredDate $preferredDate `
        -Label 'inventory' `
        -OutFolder $TempFolder `
        -FallbackDays 2 `
        -UseFallback $true

    $InventoryCsvPath = $inventoryBlobInfo.LocalPath
    $script:ResolvedProcessingDate = $inventoryBlobInfo.Date
}
else {
    if ([string]::IsNullOrWhiteSpace($InventoryCsvPath) -or -not (Test-Path $InventoryCsvPath)) {
        throw 'Inventário não encontrado. Passe -InventoryCsvPath ou use -UseLatestInventoryFromLake.'
    }

    if (-not [string]::IsNullOrWhiteSpace($Date)) {
        $script:ResolvedProcessingDate = $Date
    }
    elseif (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
        $script:ResolvedProcessingDate = $PipelineDate
    }
    else {
        $script:ResolvedProcessingDate = $preferredDate
    }
}

if ([string]::IsNullOrWhiteSpace($script:ResolvedProcessingDate)) {
    throw 'Não foi possível resolver a data de processamento do Script 04.'
}

$Date = $script:ResolvedProcessingDate
Write-Host "✅ Data efetiva do processamento (partição resolvida): $Date"
Write-Host "📥 Inventário selecionado: $InventoryCsvPath"

if ([string]::IsNullOrWhiteSpace($OverridesRgPath)) {
    $overridesBlobInfo = Download-BlobByResolvedDatePrefix `
        -Ctx $ctx `
        -Container $FinopsContainer `
        -BasePrefix $OverridesPrefix `
        -PreferredDate $Date `
        -Label 'overrides_rg' `
        -OutFolder $TempFolder `
        -FallbackDays 2 `
        -UseFallback $true

    $OverridesRgPath = $overridesBlobInfo.LocalPath
    Write-Host "📥 Overrides selecionado: $OverridesRgPath (dt=$($overridesBlobInfo.Date))"
}

Write-Host "📥 Lendo inventário: $InventoryCsvPath"
$inv = @(Import-Csv -Path $InventoryCsvPath -Delimiter ';')
if ($inv.Count -eq 0) {
    throw "Inventário vazio/não lido: $InventoryCsvPath"
}

$invUnique = @(
    $inv |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ResourceId) } |
    Sort-Object ResourceId -Unique
)

$invUnique | ForEach-Object {
    $_.ResourceGroupName = (Normalize-Text $_.ResourceGroupName).ToLowerInvariant()
    $_.Type              = Normalize-Text $_.Type
    $_.Name              = Normalize-Text $_.Name
    $_.ResourceId        = Normalize-Text $_.ResourceId
}

$mcRgFinopsClienteMap = Get-McRgFinopsClienteMap -InventoryRows $invUnique

$invUnique | ForEach-Object {
    $finopsInfo = Get-EffectiveFinopsClienteInfo -Row $_ -McRgFinopsClienteMap $mcRgFinopsClienteMap

    if ($_.PSObject.Properties.Name -contains 'EffectiveFinopsCliente') {
        $_.EffectiveFinopsCliente = Normalize-Text $finopsInfo.EffectiveFinopsCliente
    }
    else {
        $_ | Add-Member -NotePropertyName EffectiveFinopsCliente -NotePropertyValue (Normalize-Text $finopsInfo.EffectiveFinopsCliente)
    }

    if ($_.PSObject.Properties.Name -contains 'FinopsClienteSource') {
        $_.FinopsClienteSource = Normalize-Text $finopsInfo.FinopsClienteSource
    }
    else {
        $_ | Add-Member -NotePropertyName FinopsClienteSource -NotePropertyValue (Normalize-Text $finopsInfo.FinopsClienteSource)
    }
}

$shared = @(
    $invUnique | Where-Object { (Normalize-Text $_.EffectiveFinopsCliente) -eq 'COMPARTILHADO' }
)

if ($ExcludeNetworkWatcherRG) {
    $shared = @(
        $shared | Where-Object { $_.ResourceGroupName -ne 'networkwatcherrg' }
    )
}

Write-Host "🔎 Shared (EffectiveFinopsCliente=COMPARTILHADO): $($shared.Count) recursos"

$vmNames = @{}
$vmssNames = @{}

@(
    $shared | Where-Object { $_.Type -eq 'Microsoft.Compute/virtualMachines' }
) | ForEach-Object {
    $vmNames[($_.Name + '').ToLowerInvariant()] = $true
}

@(
    $shared | Where-Object { $_.Type -eq 'Microsoft.Compute/virtualMachineScaleSets' }
) | ForEach-Object {
    $vmssNames[($_.Name + '').ToLowerInvariant()] = $true
}

function Exists-VM([string]$name)   { return $vmNames.ContainsKey(($name + '').ToLowerInvariant()) }
function Exists-VMSS([string]$name) { return $vmssNames.ContainsKey(($name + '').ToLowerInvariant()) }

$rgOverrides = @(Load-RgOverrides -path $OverridesRgPath)
Write-Host "📌 Overrides RG carregados: $($rgOverrides.Count) ($OverridesRgPath)"

$rows = New-Object System.Collections.Generic.List[object]

foreach ($r in $shared) {
    $rid  = $r.ResourceId
    $type = $r.Type
    $name = $r.Name
    $rg   = $r.ResourceGroupName

    $svcKey = $null
    $svcType = $null
    $rule = $null
    $conf = 0
    $notes = ''

    $rgOv = Try-ApplyRgOverride -rgLower $rg -rgOverrides $rgOverrides

    # 1) AKS / MC_* / managedClusters
    $aks = Resolve-AksClusterFromMcRg -rgLower $rg
    if ($aks -and (Test-IsAksInfraType -type $type)) {
        $svcKey = "AKS:$aks"
        $svcType = 'AKS'
        $rule = 'AKS_MC_RG_INFRA'
        $conf = 100
        $notes = 'MC_RG infra mapped to AKS cluster'
    }
    elseif ($aks) {
        $svcKey = "AKS:$aks"
        $svcType = 'AKS'
        $rule = 'AKS_MC_RG'
        $conf = 95
        $notes = 'MC_RG generic mapped to AKS cluster'
    }
    elseif ($type -eq 'Microsoft.ContainerService/managedClusters') {
        $svcKey = "AKS:$name"
        $svcType = 'AKS'
        $rule = 'AKS_MANAGEDCLUSTER'
        $conf = 100
    }

    # 2) SQL family
    if (-not $svcKey -and $type -like 'Microsoft.Sql/servers/*') {
        if ($type -eq 'Microsoft.Sql/servers/elasticpools') {
            $svcKey = "SQLPOOL:$name"
            $svcType = 'SQLPOOL'
            $rule = 'SQL_ELASTICPOOL'
            $conf = 98
        }
        elseif ($type -eq 'Microsoft.Sql/servers/databases') {
            $pool = Extract-FromResourceId -rid $rid -marker '/elasticPools/'
            if ($pool) {
                $svcKey = "SQLPOOL:$pool"
                $svcType = 'SQLPOOL'
                $rule = 'SQLDB_IN_POOL'
                $conf = 96
            }
            else {
                $svcKey = "SQLDB:$name"
                $svcType = 'SQLDB'
                $rule = 'SQLDB_STANDALONE'
                $conf = 92
            }
        }
        else {
            $server = ($name.Split('/'))[0]
            if ($server) {
                $svcKey = "SQL:$server"
                $svcType = 'SQL'
                $rule = 'SQL_FAMILY_FALLBACK'
                $conf = 80
                $notes = 'Faltou regra específica para este sub-type SQL.'
            }
        }
    }

    # 3) Shared backup RG
    if (-not $svcKey -and (Test-IsSharedBackupRg -rg $rg)) {
        $svcKey = 'BACKUP:shared-datadisk-eu-prd'
        $svcType = 'BACKUP'
        $rule = 'SHARED_BACKUP_RG'
        $conf = 100
        $notes = 'Shared backup RG mapped to global allocation'
    }

    # 4) Direct shared service types
    if (-not $svcKey) {
        switch ($type) {
            'Microsoft.Cache/Redis'                     { $svcKey="REDIS:$name"; $svcType='REDIS'; $rule='REDIS'; $conf=95; break }
            'Microsoft.OperationalInsights/workspaces' { $svcKey="LAW:$name";   $svcType='LAW';   $rule='LAW';   $conf=95; break }
            'Microsoft.Network/applicationGateways'    { $svcKey="APPGW:$name"; $svcType='APPGW'; $rule='APPGW'; $conf=95; break }
            'Microsoft.Network/loadBalancers'          { $svcKey="LB:$name";    $svcType='LB';    $rule='LB';    $conf=95; break }
            'Microsoft.Storage/storageAccounts'        { $svcKey="STG:$name";   $svcType='STORAGE'; $rule='STORAGE'; $conf=95; break }
            'Microsoft.KeyVault/vaults'                { $svcKey="KV:$name";    $svcType='KEYVAULT'; $rule='KEYVAULT'; $conf=95; break }
            'Microsoft.ServiceBus/namespaces'          { $svcKey="SB:$name";    $svcType='SERVICEBUS'; $rule='SERVICEBUS'; $conf=95; break }
            'Microsoft.RecoveryServices/vaults'        { $svcKey="RSV:$name";   $svcType='RECOVERYVAULT'; $rule='RECOVERYVAULT'; $conf=90; break }
            default { }
        }
    }

    # 5) VM / VMSS heuristics
    if (-not $svcKey) {
        if ($type -eq 'Microsoft.Compute/virtualMachines') {
            $svcKey="VM:$name"; $svcType='VM'; $rule='VM_DIRECT'; $conf=95
        }
        elseif ($type -eq 'Microsoft.Compute/virtualMachineScaleSets') {
            $svcKey="VMSS:$name"; $svcType='VMSS'; $rule='VMSS_DIRECT'; $conf=95
        }
        elseif ($type -eq 'Microsoft.Compute/virtualMachines/extensions') {
            $vm = Extract-FromResourceId -rid $rid -marker '/virtualMachines/'
            if ($vm -and (Exists-VM $vm)) {
                $svcKey="VM:$vm"; $svcType='VM'; $rule='VM_EXTENSION_PARENT'; $conf=92
            }
        }
        elseif ($type -eq 'Microsoft.Network/networkInterfaces') {
            $vmGuess = Guess-ParentFromNicName -nicName $name
            if ($vmGuess -and (Exists-VM $vmGuess)) {
                $svcKey="VM:$vmGuess"; $svcType='VM'; $rule='NIC_TO_VM_NAME_VALIDATED'; $conf=80
            }
            elseif ($vmGuess -and (Exists-VMSS $vmGuess)) {
                $svcKey="VMSS:$vmGuess"; $svcType='VMSS'; $rule='NIC_TO_VMSS_NAME_VALIDATED'; $conf=75
            }
        }
        elseif ($type -eq 'Microsoft.Compute/disks') {
            $vmFromDisk = Guess-ParentVmFromDiskName -diskName $name
            if ($vmFromDisk -and (Exists-VM $vmFromDisk)) {
                $svcKey="VM:$vmFromDisk"; $svcType='VM'; $rule='DISK_TO_VM_NAME_VALIDATED'; $conf=82
            }
            elseif ($vmFromDisk -and (Exists-VMSS $vmFromDisk)) {
                $svcKey="VMSS:$vmFromDisk"; $svcType='VMSS'; $rule='DISK_TO_VMSS_NAME_VALIDATED'; $conf=75
            }
        }
    }

    # 6) RG override or generic infra fallback
    if (-not $svcKey) {
        if ($rgOv -and (($rgOv.ServiceKey + '').Trim() -eq 'EXCLUDE')) { continue }

        if ($rgOv -and -not [string]::IsNullOrWhiteSpace((Normalize-Text $rgOv.ServiceKey))) {
            $svcKey  = Normalize-Text $rgOv.ServiceKey
            $svcType = if ([string]::IsNullOrWhiteSpace((Normalize-Text $rgOv.ServiceType))) { 'INFRA' } else { (Normalize-Text $rgOv.ServiceType) }
            $rule    = 'OVERRIDE_RG'
            $conf    = 99
            $notes   = "RG override (pattern=$($rgOv.Pattern)). $(Normalize-Text $rgOv.Notes)"
        }
        else {
            if ($type -like 'Microsoft.Network/*' -or $type -eq 'Microsoft.Network/privateDnsZones') {
                $svcKey='INFRA:SHARED-NETWORK'; $svcType='INFRA'; $rule='INFRA_NETWORK'; $conf=70
            }
            elseif ($type -like 'Microsoft.ManagedIdentity/*') {
                $svcKey='INFRA:SHARED-IDENTITY'; $svcType='INFRA'; $rule='INFRA_IDENTITY'; $conf=70
            }
            else {
                $svcKey='INFRA:SHARED'; $svcType='INFRA'; $rule='INFRA_SHARED_FALLBACK'; $conf=60
            }
        }
    }

    $rows.Add((New-Row $rid $type $name $rg $svcKey $svcType $rule $conf $notes $r.EffectiveFinopsCliente $r.FinopsClienteSource)) | Out-Null
}

$outLocal = Join-Path $TempFolder ("resource_to_service_shared_{0}.csv" -f $Date)
@($rows.ToArray()) | Export-Csv -Path $outLocal -NoTypeInformation -Delimiter ';' -Encoding UTF8
Write-Host "✅ resource_to_service_shared gerado: $outLocal"

$candidates = @(
    @($rows.ToArray()) |
    Where-Object { $_.Confidence -lt 75 -or $_.ServiceKey -like 'INFRA:*' } |
    Group-Object ResourceGroupName, ResourceType |
    Sort-Object Count -Descending |
    Select-Object -First 300 |
    ForEach-Object {
        $parts = $_.Name.Split(',')
        [PSCustomObject]@{
            ResourceGroupName   = $parts[0].Trim()
            ResourceType        = $parts[1].Trim()
            Count               = $_.Count
            SuggestedServiceKey = ''
            Notes               = 'Se precisar granularidade, defina SuggestedServiceKey'
        }
    }
)

$outCandLocal = Join-Path $TempFolder ("override_candidates_shared_{0}.csv" -f $Date)
$candidates | Export-Csv -Path $outCandLocal -NoTypeInformation -Delimiter ';' -Encoding UTF8
Write-Host "✅ override_candidates_shared gerado: $outCandLocal"

$blobMap  = "$ResourceToServicePrefix/dt=$Date/$(Split-Path $outLocal -Leaf)"
$blobCand = "$ResourceToServicePrefix/dt=$Date/$(Split-Path $outCandLocal -Leaf)"

Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $outLocal -Blob $blobMap
Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $outCandLocal -Blob $blobCand

Write-Host "✅ OK - resource_to_service_shared enviado para: $FinopsContainer/$blobMap"
Write-Host "✅ OK - override_candidates_shared enviado para: $FinopsContainer/$blobCand"

$rowsArray = @($rows.ToArray())

$stats = @(
    $rowsArray | Group-Object ServiceType | Sort-Object Count -Descending
)

Write-Host "`n📊 ServiceTypes (shared):"
$stats | ForEach-Object { Write-Host ("- {0}: {1}" -f $_.Name, $_.Count) }

$low = @(
    $rowsArray | Where-Object { $_.Confidence -lt 75 }
).Count
Write-Host "`n⚠️ Low-confidence (shared): $low de $($rowsArray.Count)"
