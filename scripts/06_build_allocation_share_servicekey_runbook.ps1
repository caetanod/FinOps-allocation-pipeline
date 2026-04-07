<#
Objetivo:
Gerar a share de alocação genérica por ServiceKey usando os pesos globais dos clientes.

Função no pipeline:
Aplicar rateio padrão para ServiceKeys compartilhadas que não exigem tratamento especializado.

Entrada:
- resource_to_service
- client_weights_by_dedicated_cost

Saída:
- allocation_share_servicekey

Exclusões:
- AKS
- SQLPOOL
- SQLDB
Essas seguem para scripts especializados.

Observação:
É o motor de rateio genérico do pipeline. Tudo que não precisa de lógica específica deve ser resolvido aqui.
#>

<#
Objetivo:
Gerar a share de alocação genérica por ServiceKey usando os pesos globais/dedicados dos clientes.

Ajustes aplicados:
- Não usa D-1 fixo
- Separa PipelinePartitionDate de UsageDate
- Resolve a partição pelo timezone do Brasil + fallback (hoje, D-1, D-2) e, se necessário,
  pela maior dt= comum entre os insumos
- Deriva UsageDate do conteúdo do fact_cost (Date / UsageDate / UsageDateTime)
- Gera saída particionada por UsageDate
- Exclui AKS, SQLPOOL e SQLDB do rateio genérico
#>

<#
Objetivo:
Gerar a share de alocação genérica por ServiceKey usando os pesos globais dos clientes.

Ajustes aplicados nesta versão:
- Não exige dt= em comum entre fact_cost e client_weights.
- fact_cost define a UsageDate real pelo conteúdo.
- client_weights é localizado pelo nome do arquivo client_weights_<UsageDate>.csv, independentemente da dt= de publicação.
- resource_to_service continua sendo consumido por ResourceId -> ServiceKey e é tratado como lookup, usando a partição mais recente disponível <= UsageDate quando possível.
- Mantém a lógica original de rateio do Script 06.
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
    [string]$ResourceToServicePrefix = "silver/resource_to_service",

    [Parameter(Mandatory = $false)]
    [string]$FactCostPrefix = "silver/fact_cost",

    [Parameter(Mandatory = $false)]
    [string]$WeightsPrefix = "silver/client_weights_by_dedicated_cost",

    [Parameter(Mandatory = $false)]
    [string]$OutputPrefix = "silver/allocation_share_servicekey",

    [Parameter(Mandatory = $false)]
    [string]$TempFolder = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$env:SuppressAzurePowerShellBreakingChangeWarnings = "true"

trap {
    Write-Error ("❌ Step 06 falhou. Linha: {0}. Comando: {1}. Erro: {2}" -f $_.InvocationInfo.ScriptLineNumber, $_.InvocationInfo.Line.Trim(), $_.Exception.Message)
    throw
}

$TargetSubscriptions = @(
    "52d4423b-7ed9-4673-b8e2-fa21cdb83176",
    "3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
)

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
        if ($IsLinux -or $IsMacOS) {
        $tzId = 'America/Sao_Paulo'
    }
    else {
        $tzId = 'E. South America Standard Time'
    }

    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($tzId)
    return [System.TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $tz)
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
                Write-Host ("✅ Storage encontrado na subscription: {0}" -f $subId)
                return $sa.Context
            }
        }
        catch {
            Write-Warning ("Erro ao buscar storage em {0}. Detalhe: {1}" -f $subId, $_.Exception.Message)
        }
    }

    throw "Storage Account '$StorageAccountName' não encontrado nas subscriptions informadas."
}

function Get-BlobLastModifiedUtc {
    param([Parameter(Mandatory = $true)]$Blob)

    if ($Blob.ICloudBlob -and $Blob.ICloudBlob.Properties -and $null -ne $Blob.ICloudBlob.Properties.LastModified) {
        try { return $Blob.ICloudBlob.Properties.LastModified.UtcDateTime } catch {}
        try { return $Blob.ICloudBlob.Properties.LastModified.DateTime.ToUniversalTime() } catch {}
    }

    if ($Blob.LastModified) {
        try { return $Blob.LastModified.UtcDateTime } catch {}
        try { return $Blob.LastModified.DateTime.ToUniversalTime() } catch {}
    }

    return [datetime]::MinValue
}

function Download-Blob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$BlobName,
        [Parameter(Mandatory = $true)][string]$OutFolder
    )

    Ensure-Folder -Path $OutFolder
    $local = Join-Path $OutFolder (Split-Path $BlobName -Leaf)
    Write-Host ("⬇️ Download: {0}/{1} -> {2}" -f $Container, $BlobName, $local)

    Get-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -Blob $BlobName `
        -Destination $local `
        -Force `
        -ErrorAction Stop | Out-Null

    return $local
}

function Upload-Blob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$File,
        [Parameter(Mandatory = $true)][string]$Blob
    )

    if (-not (Test-Path -LiteralPath $File)) {
        throw "Arquivo local não encontrado para upload: $File"
    }

    Write-Host ("⬆️ Upload: {0}/{1}" -f $Container, $Blob)
    Set-AzStorageBlobContent `
        -Context $Ctx `
        -Container $Container `
        -File $File `
        -Blob $Blob `
        -Force `
        -ErrorAction Stop | Out-Null
}

function Assert-Columns {
    param(
        [Parameter(Mandatory = $true)]$Rows,
        [Parameter(Mandatory = $true)][string[]]$RequiredColumns,
        [Parameter(Mandatory = $true)][string]$Label
    )

    if (-not $Rows -or @($Rows).Count -eq 0) {
        throw "$Label vazio."
    }

    $headers = @($Rows[0].PSObject.Properties.Name)
    foreach ($col in $RequiredColumns) {
        if ($headers -notcontains $col) {
            throw "$Label sem coluna obrigatória '$col'. Colunas encontradas: $($headers -join ', ')"
        }
    }
}

function Normalize-Text {
    param([object]$Text)
    if ($null -eq $Text) { return "" }
    $value = [string]$Text
    if ([string]::IsNullOrWhiteSpace($value)) { return "" }
    return $value.Trim([char[]]@([char]0xFEFF)).Trim()
}

function Parse-DoubleInvariant {
    param([object]$Text)
    if ($null -eq $Text) { return [double]0 }

    $raw = Normalize-Text $Text
    if ([string]::IsNullOrWhiteSpace($raw)) { return [double]0 }

    $parsed = 0.0
    $styles = [System.Globalization.NumberStyles]::AllowLeadingSign `
        -bor [System.Globalization.NumberStyles]::AllowDecimalPoint `
        -bor [System.Globalization.NumberStyles]::AllowThousands `
        -bor [System.Globalization.NumberStyles]::AllowLeadingWhite `
        -bor [System.Globalization.NumberStyles]::AllowTrailingWhite

    if ([double]::TryParse($raw, $styles, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return [double]$parsed
    }

    if ([double]::TryParse($raw, $styles, [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR'), [ref]$parsed)) {
        return [double]$parsed
    }

    $normalized = $raw.Replace('.', '').Replace(',', '.')
    if ([double]::TryParse($normalized, $styles, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return [double]$parsed
    }

    return [double]0
}

function Try-ParseDateInvariant {
    param([object]$Value)
    $text = Normalize-Text $Value
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    try {
        return [datetime]::ParseExact($text, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        try { return [datetime]::Parse($text, [System.Globalization.CultureInfo]::InvariantCulture) }
        catch { return $null }
    }
}

function Resolve-MaxUsageDateFromRows {
    param(
        [Parameter(Mandatory = $true)]$Rows,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $maxDate = $null

    foreach ($row in $Rows) {
        $candidate = $null
        foreach ($propName in @('UsageDate', 'Date')) {
            if ($row.PSObject.Properties[$propName]) {
                $candidate = Try-ParseDateInvariant $row.$propName
                if ($candidate) { break }
            }
        }

        if ($candidate -and ($null -eq $maxDate -or $candidate -gt $maxDate)) {
            $maxDate = $candidate
        }
    }

    if ($null -eq $maxDate) {
        throw "Não foi possível derivar UsageDate do conteúdo de $Label."
    }

    return $maxDate.ToString('yyyy-MM-dd')
}

function Get-DtValuesFromPrefix {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix
    )

    $blobs = @(Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix ($Prefix.TrimEnd('/') + '/') -ErrorAction SilentlyContinue)
    $dates = New-Object System.Collections.Generic.List[string]

    foreach ($blob in $blobs) {
        $name = [string]$blob.Name
        if ($name -match '/dt=(\d{4}-\d{2}-\d{2})/') {
            $dt = $Matches[1]
            if (-not $dates.Contains($dt)) {
                [void]$dates.Add($dt)
            }
        }
    }

    return @([string[]]$dates | Sort-Object)
}

function Select-FactBlob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [string]$PreferredDate = ''
    )

    $datesToTry = New-Object System.Collections.Generic.List[string]
    if (-not [string]::IsNullOrWhiteSpace($PreferredDate)) {
        [void]$datesToTry.Add($PreferredDate)
    }

    $nowLocal = Get-BrazilNow
    foreach ($offset in 0..2) {
        $candidate = $nowLocal.AddDays(-$offset).ToString('yyyy-MM-dd')
        if (-not $datesToTry.Contains($candidate)) {
            [void]$datesToTry.Add($candidate)
        }
    }

    $availableDates = @(Get-DtValuesFromPrefix -Ctx $Ctx -Container $Container -Prefix $Prefix)
    foreach ($candidate in $availableDates | Sort-Object -Descending) {
        if (-not $datesToTry.Contains($candidate)) {
            [void]$datesToTry.Add($candidate)
        }
    }

    foreach ($dt in $datesToTry) {
        $prefixToCheck = "$Prefix/dt=$dt/"
        $blobs = @(
            Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefixToCheck -ErrorAction SilentlyContinue |
            Where-Object { (Split-Path $_.Name -Leaf) -like 'fact_cost_*.csv' }
        )

        if ($blobs.Count -gt 0) {
            $blob = $blobs |
                Sort-Object @{ Expression = { Get-BlobLastModifiedUtc $_ }; Descending = $true } |
                Select-Object -First 1

            Write-Host ("📌 Blob fact_cost selecionado em dt={0}: {1}" -f $dt, $blob.Name)
            return $blob
        }
    }

    throw "Nenhum blob fact_cost foi encontrado em '$Prefix'."
}

function Select-WeightsBlobByUsageDate {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [Parameter(Mandatory = $true)][string]$UsageDate
    )

    $expectedLeaf = "client_weights_{0}.csv" -f $UsageDate
    $prefixToCheck = $Prefix.TrimEnd('/') + '/'

    $blobs = @(
        Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefixToCheck -ErrorAction SilentlyContinue |
        Where-Object { (Split-Path $_.Name -Leaf) -eq $expectedLeaf }
    )

    if ($blobs.Count -eq 0) {
        throw "Arquivo '$expectedLeaf' não encontrado em '$Prefix' (independentemente da dt= de publicação)."
    }

    $blob = $blobs |
        Sort-Object @{ Expression = { Get-BlobLastModifiedUtc $_ }; Descending = $true } |
        Select-Object -First 1

    Write-Host ("📌 Blob client_weights selecionado para UsageDate={0}: {1}" -f $UsageDate, $blob.Name)
    return $blob
}

function Select-ResourceToServiceBlob {
    param(
        [Parameter(Mandatory = $true)]$Ctx,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Prefix,
        [Parameter(Mandatory = $true)][string]$TargetDate
    )

    $availableDates = @(Get-DtValuesFromPrefix -Ctx $Ctx -Container $Container -Prefix $Prefix)
    if ($availableDates.Count -eq 0) {
        throw "Nenhuma partição dt= encontrada em '$Prefix'."
    }

    $eligible = @(
        foreach ($dt in $availableDates) {
            if ($dt -le $TargetDate) { $dt }
        }
    )

    if (@($eligible).Count -gt 0) {
        $selectedDt = (@($eligible) | Sort-Object -Descending | Select-Object -First 1)
    }
    else {
        $selectedDt = (@($availableDates) | Sort-Object -Descending | Select-Object -First 1)
    }

    $expectedLeaf = "resource_to_service_shared_{0}.csv" -f $selectedDt
    $prefixToCheck = "$Prefix/dt=$selectedDt/"
    $blobs = @(
        Get-AzStorageBlob -Context $Ctx -Container $Container -Prefix $prefixToCheck -ErrorAction SilentlyContinue |
        Where-Object { ((Split-Path $_.Name -Leaf) -eq $expectedLeaf) -or ((Split-Path $_.Name -Leaf) -like 'resource_to_service*.csv') }
    )

    if ($blobs.Count -eq 0) {
        throw "Nenhum CSV de resource_to_service encontrado em '$prefixToCheck'."
    }

    $blob = $blobs |
        Sort-Object @{ Expression = { 
            if ((Split-Path $_.Name -Leaf) -eq $expectedLeaf) { 0 } else { 1 }
        } }, @{ Expression = { Get-BlobLastModifiedUtc $_ }; Descending = $true } |
        Select-Object -First 1

    Write-Host ("📌 Blob resource_to_service selecionado em dt={0}: {1}" -f $selectedDt, $blob.Name)
    return $blob
}

if ($PipelineDate) { $Date = $PipelineDate }
$Date = Normalize-Text $Date

$PipelinePartitionDate = ''
if (-not [string]::IsNullOrWhiteSpace($Date)) {
    $PipelinePartitionDate = $Date
}
else {
    $PipelinePartitionDate = (Get-BrazilNow).ToString('yyyy-MM-dd')
}

Write-Host ("📅 PipelinePartitionDate: {0}" -f $PipelinePartitionDate)

if ([string]::IsNullOrWhiteSpace($TempFolder)) {
    $baseTemp = [System.IO.Path]::GetTempPath()
    if ([string]::IsNullOrWhiteSpace($baseTemp)) {
        if ($IsLinux -or $IsMacOS) {
            $baseTemp = '/tmp'
        }
        else {
            $baseTemp = 'C:\Temp'
        }
    }
    $TempFolder = Join-Path $baseTemp 'finops'
}
$TempFolder = $TempFolder.Trim()

$preferredDateLabel = '<auto>'
if (-not [string]::IsNullOrWhiteSpace($Date)) { $preferredDateLabel = $Date }
Write-Host ("📅 PreferredDate: {0}" -f $preferredDateLabel)
Write-Host ("📂 TempFolder: {0}" -f $TempFolder)

Ensure-Folder -Path $TempFolder
Login-Azure
$ctx = Get-StorageContext -StorageAccountName $StorageAccountName
Write-Host "✅ Storage context carregado com sucesso."

# 1) fact_cost define a UsageDate real
$factBlob = Select-FactBlob -Ctx $ctx -Container $FinopsContainer -Prefix $FactCostPrefix -PreferredDate $Date
$costPath = Download-Blob -Ctx $ctx -Container $FinopsContainer -BlobName $factBlob.Name -OutFolder $TempFolder
$cost = @(Import-Csv -Path $costPath -Delimiter ';')
Assert-Columns -Rows $cost -RequiredColumns @('Date','ResourceId','Cost') -Label 'fact_cost'

$UsageDate = Resolve-MaxUsageDateFromRows -Rows $cost -Label 'fact_cost'
Write-Host ("📅 UsageDate resolvida do fact_cost: {0}" -f $UsageDate)

$costFiltered = @(
    foreach ($row in $cost) {
        $rowDate = ''
        if ($row.PSObject.Properties['Date']) { $rowDate = Normalize-Text $row.Date }
        if ($rowDate -eq $UsageDate) { $row }
    }
)
if (@($costFiltered).Count -eq 0) {
    throw "Nenhuma linha de fact_cost encontrada para UsageDate=$UsageDate."
}

# 2) client_weights alinhado por UsageDate, independentemente da dt= da pasta
$weightsBlob = Select-WeightsBlobByUsageDate -Ctx $ctx -Container $FinopsContainer -Prefix $WeightsPrefix -UsageDate $UsageDate
$weightPath = Download-Blob -Ctx $ctx -Container $FinopsContainer -BlobName $weightsBlob.Name -OutFolder $TempFolder
$weights = @(Import-Csv -Path $weightPath -Delimiter ';')
Assert-Columns -Rows $weights -RequiredColumns @('UsageDate','Cliente','Weight') -Label 'client_weights'

$weightsFiltered = @(
    foreach ($row in $weights) {
        $rowUsageDate = ''
        if ($row.PSObject.Properties['UsageDate']) { $rowUsageDate = Normalize-Text $row.UsageDate }
        if ($rowUsageDate -eq $UsageDate) { $row }
    }
)
if (@($weightsFiltered).Count -eq 0) {
    throw "Nenhuma linha de client_weights encontrada para UsageDate=$UsageDate."
}

# 3) resource_to_service como lookup pela partição mais recente disponível <= UsageDate
$rtsBlob = Select-ResourceToServiceBlob -Ctx $ctx -Container $FinopsContainer -Prefix $ResourceToServicePrefix -TargetDate $UsageDate
$rtsPath = Download-Blob -Ctx $ctx -Container $FinopsContainer -BlobName $rtsBlob.Name -OutFolder $TempFolder
$rts = @(Import-Csv -Path $rtsPath -Delimiter ';')
Assert-Columns -Rows $rts -RequiredColumns @('ResourceId','ServiceKey') -Label 'resource_to_service'

Write-Host ("📥 RTS    : {0}" -f $rtsPath)
Write-Host ("📥 COST   : {0}" -f $costPath)
Write-Host ("📥 WEIGHT : {0}" -f $weightPath)
Write-Host ("📌 Linhas fact_cost (UsageDate): {0}" -f @($costFiltered).Count)
Write-Host ("📌 Linhas client_weights      : {0}" -f @($weightsFiltered).Count)
Write-Host ("📌 Linhas resource_to_service : {0}" -f @($rts).Count)

# MAP RESOURCE -> SERVICEKEY
$map = @{}
foreach ($r in $rts) {
    $rid = Normalize-Text $r.ResourceId
    $svc = Normalize-Text $r.ServiceKey
    if ([string]::IsNullOrWhiteSpace($rid)) { continue }
    if ([string]::IsNullOrWhiteSpace($svc)) { continue }
    $map[$rid] = $svc
}
Write-Host ("📌 Recursos mapeados para ServiceKey: {0}" -f $map.Count)

# SOMAR CUSTO POR SERVICEKEY
$costByService = @{}
foreach ($c in $costFiltered) {
    $rid = Normalize-Text $c.ResourceId
    if ([string]::IsNullOrWhiteSpace($rid)) { continue }
    if (-not $map.ContainsKey($rid)) { continue }

    $svc = Normalize-Text $map[$rid]
    if ([string]::IsNullOrWhiteSpace($svc)) { continue }

    $costValue = Parse-DoubleInvariant $c.Cost
    if (-not $costByService.ContainsKey($svc)) {
        $costByService[$svc] = [double]0
    }
    $costByService[$svc] = [double]$costByService[$svc] + [double]$costValue
}
Write-Host ("📌 ServiceKeys com custo encontrado: {0}" -f $costByService.Count)

# EXPANDIR PARA CLIENTES COM PESO
$result = New-Object System.Collections.Generic.List[object]
foreach ($svc in $costByService.Keys) {
    $totalCost = [double]$costByService[$svc]

    foreach ($w in $weightsFiltered) {
        $cliente = Normalize-Text $w.Cliente
        if ([string]::IsNullOrWhiteSpace($cliente)) { continue }

        $weightValue = Parse-DoubleInvariant $w.Weight
        if ($weightValue -le 0) { continue }

        $allocated = [double]$totalCost * [double]$weightValue
        [void]$result.Add([PSCustomObject]@{
            Date       = $UsageDate
            ServiceKey = $svc
            Cliente    = $cliente
            Weight     = $weightValue
            Cost       = $allocated
            Notes      = 'step06_base_allocation'
        })
    }
}

$resultArray = @(foreach ($item in $result) { $item })
if (@($resultArray).Count -eq 0) {
    throw 'Nenhuma linha de allocation_share_servicekey foi gerada.'
}
Write-Host ("📌 Linhas intermediárias geradas: {0}" -f @($resultArray).Count)

# NORMALIZE WEIGHT -> SHARE
$normalizedRows = @(
    $resultArray |
    Group-Object -Property Date, ServiceKey |
    ForEach-Object {
        $group = $_.Group
        $totalWeight = [double](($group | Measure-Object -Property Weight -Sum).Sum)
        if ($null -eq $totalWeight -or $totalWeight -le 0) {
            Write-Warning ("ServiceKey sem weight válido: {0}" -f $_.Name)
            return
        }

        foreach ($r in $group) {
            [PSCustomObject]@{
                Date           = $r.Date
                ServiceKey     = $r.ServiceKey
                Cliente        = $r.Cliente
                Share          = [Math]::Round(([double]$r.Weight / [double]$totalWeight), 6)
                DriverType     = 'GLOBAL_WEIGHT'
                AllocationMode = 'BASE_SHARED'
                Notes          = $r.Notes
                AllocatedCost  = [Math]::Round([double]$r.Cost, 6)
            }
        }
    }
)
if (@($normalizedRows).Count -eq 0) {
    throw 'Nenhuma linha normalizada de share foi gerada.'
}
Write-Host ("📌 Linhas normalizadas: {0}" -f @($normalizedRows).Count)

# VALIDATION
$validation = @(
    $normalizedRows |
    Group-Object -Property Date, ServiceKey |
    ForEach-Object {
        $first = $_.Group[0]
        $sumShare = [double](($_.Group | Measure-Object -Property Share -Sum).Sum)
        [PSCustomObject]@{
            Date       = $first.Date
            ServiceKey = $first.ServiceKey
            ShareSum   = [Math]::Round($sumShare, 6)
            Rows       = $_.Count
        }
    }
)
$bad = @($validation | Where-Object { [Math]::Abs([double]$_.ShareSum - 1.0) -gt 0.01 })
Write-Host ("📌 ServiceKeys validados   : {0}" -f @($validation).Count)
Write-Host ("📌 ServiceKeys fora de ~1 : {0}" -f @($bad).Count)
if (@($bad).Count -gt 0) {
    $bad | Select-Object -First 10 | Format-Table | Out-String | Write-Host
}

# EXPORT
$outFile = Join-Path $TempFolder ("allocation_share_servicekey_{0}.csv" -f $UsageDate)
$exportRows = @(
    $normalizedRows |
    Where-Object { ([double]$_.Share -gt 0) } |
    Sort-Object ServiceKey, Cliente |
    Select-Object `
        @{n='Date';e={$_.Date}},
        @{n='ServiceKey';e={$_.ServiceKey}},
        @{n='Cliente';e={$_.Cliente}},
        @{n='Share';e={$_.Share.ToString('F6',[System.Globalization.CultureInfo]::InvariantCulture)}},
        @{n='DriverType';e={$_.DriverType}},
        @{n='AllocationMode';e={$_.AllocationMode}},
        @{n='Notes';e={$_.Notes}}
)
if (@($exportRows).Count -eq 0) {
    throw 'Nenhuma linha pronta para exportação foi gerada.'
}
$exportRows | Export-Csv -Path $outFile -NoTypeInformation -Delimiter ';' -Encoding UTF8
Write-Host ("✅ allocation_share_servicekey gerado: {0}" -f $outFile)

# UPLOAD
$blob = "{0}/dt={1}/allocation_share_servicekey_{2}.csv" -f $OutputPrefix, $PipelinePartitionDate, $UsageDate
Upload-Blob -Ctx $ctx -Container $FinopsContainer -File $outFile -Blob $blob
Write-Host ("🚀 allocation_share_servicekey publicado: {0}/{1}" -f $FinopsContainer, $blob)

# STATS
Write-Host "`n📊 Top ServiceKeys por custo base alocado:"
@(
    $normalizedRows |
    Group-Object ServiceKey |
    ForEach-Object {
        [PSCustomObject]@{
            ServiceKey = $_.Name
            Cost       = ($_.Group | Measure-Object AllocatedCost -Sum).Sum
        }
    } |
    Sort-Object Cost -Descending |
    Select-Object -First 10
) | ForEach-Object {
    Write-Host (" - {0}: {1}" -f $_.ServiceKey, ([double]$_.Cost).ToString('F6',[System.Globalization.CultureInfo]::InvariantCulture))
}

Write-Host "✅ Script 06 finalizado com sucesso"
