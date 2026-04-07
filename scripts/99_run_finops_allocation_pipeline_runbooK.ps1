param(
    [Parameter(Mandatory = $false)]
    [string]$PipelineDate = "",

    [Parameter(Mandatory = $false)]
    [string]$StorageAccountName = "stpslkmmfinopseusprd",

    [Parameter(Mandatory = $false)]
    [string]$StorageContainerName = "finops",

    [Parameter(Mandatory = $false)]
    [string]$StorageSubscriptionId = "52d4423b-7ed9-4673-b8e2-fa21cdb83176",

    [Parameter(Mandatory = $false)]
    [string]$StorageResourceGroupName = "rg-psl-kmm-finops-eus-prd",

    [Parameter(Mandatory = $false)]
    [string]$LogFolderPrefix = "log-execucao",

    [Parameter(Mandatory = $false)]
    [int]$ChildRunbookTimeoutMinutes = 180
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Off

$AutomationSubscriptionId = "0535039d-8b8e-4ac1-9fce-9f727e8ba2b1"
$AutomationAccountName    = "nstech-prd-ccoe-aa"
$AutomationResourceGroup  = "nstech-prd-ccoe-rg"
$HybridWorkerGroup        = "hwmgkmmfinopseusprd"

$RunId = [guid]::NewGuid().ToString("N")
$ExecutionDate = (Get-Date).ToString("yyyy-MM-dd")
$ExecutionTime = (Get-Date).ToString("HH-mm-ss")

$TempRoot = [System.IO.Path]::GetTempPath()
$LogDir   = Join-Path $TempRoot ("finops/{0}/{1}" -f $ExecutionDate, $RunId)
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$MasterLogFile   = Join-Path $LogDir ("master_{0}_{1}.log" -f $ExecutionDate, $ExecutionTime)
$SummaryJsonFile = Join-Path $LogDir ("summary_{0}_{1}.json" -f $ExecutionDate, $ExecutionTime)

$Global:RunSummary = New-Object System.Collections.ArrayList

function Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )

    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Write-Host $line
    try {
        Add-Content -Path $MasterLogFile -Value $line -Encoding UTF8
    }
    catch {
        Write-Host ("[WARN] Falha ao gravar master log: {0}" -f $_.Exception.Message)
    }
}

function Publish-RunbookFailure {
    param(
        [string]$Message
    )

    Write-Host $Message
    Write-Output $Message
    Write-Error $Message
}

function Add-RunSummary {
    param(
        [string]$RunbookName,
        [string]$JobId,
        [string]$Status,
        [datetime]$StartedAt,
        [datetime]$EndedAt,
        [string]$RunOn = "",
        [string]$ChildLogFile = "",
        [string]$Message = ""
    )

    try {
        $durationSec = [math]::Round((New-TimeSpan -Start $StartedAt -End $EndedAt).TotalSeconds, 2)
    }
    catch {
        $durationSec = 0
    }

    $obj = [pscustomobject]@{
        RunbookName = $RunbookName
        JobId       = $JobId
        Status      = $Status
        StartedAt   = $StartedAt.ToString("s")
        EndedAt     = $EndedAt.ToString("s")
        DurationSec = $durationSec
        RunOn       = $RunOn
        ChildLog    = $ChildLogFile
        Message     = $Message
    }

    [void]$Global:RunSummary.Add($obj)
}

function Save-RunSummaryToDisk {
    $payload = [pscustomobject]@{
        MasterRunId             = $RunId
        ExecutionDate           = $ExecutionDate
        ExecutionTime           = $ExecutionTime
        PipelineDate            = $PipelineDate
        AutomationSubscription  = $AutomationSubscriptionId
        AutomationAccountName   = $AutomationAccountName
        AutomationResourceGroup = $AutomationResourceGroup
        StorageAccountName      = $StorageAccountName
        StorageContainerName    = $StorageContainerName
        StorageSubscriptionId   = $StorageSubscriptionId
        StorageResourceGroup    = $StorageResourceGroupName
        LogFolderPrefix         = $LogFolderPrefix
        Items                   = @($Global:RunSummary)
    }

    $json = $payload | ConvertTo-Json -Depth 10
    Set-Content -Path $SummaryJsonFile -Value $json -Encoding UTF8
}

Log -Message "Iniciando pipeline FinOps master."
if (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
    Log -Message ("PipelineDate informado ao master: {0}" -f $PipelineDate)
}
else {
    Log -Message "PipelineDate não informado. Os runbooks filhos irão resolver a data conforme sua própria lógica."
}

Log -Message "Conectando com Managed Identity..."
Disable-AzContextAutosave -Scope Process | Out-Null
Connect-AzAccount -Identity -ErrorAction Stop | Out-Null

Log -Message ("Fixando subscription do Automation: {0}" -f $AutomationSubscriptionId)
Set-AzContext -SubscriptionId $AutomationSubscriptionId -ErrorAction Stop | Out-Null

$ctx = Get-AzContext -ErrorAction Stop
Log -Message ("Contexto Azure ativo: SubscriptionId={0} | TenantId={1}" -f $ctx.Subscription.Id, $ctx.Tenant.Id) -Level "SUCCESS"

function Validate-Runbook {
    param([Parameter(Mandatory = $true)][string]$Name)

    $aa = Get-AzAutomationAccount -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -ErrorAction Stop
    if ($null -eq $aa) {
        throw ("Automation Account '{0}' não encontrado no RG '{1}'." -f $AutomationAccountName, $AutomationResourceGroup)
    }

    $rb = Get-AzAutomationRunbook -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Name $Name -ErrorAction Stop
    if ($null -eq $rb) {
        throw ("Runbook '{0}' não encontrado." -f $Name)
    }

    Log -Message ("Runbook validado: {0} | State={1} | Type={2}" -f $Name, $rb.State, $rb.RunbookType) -Level "DEBUG"
}

function Resolve-JobId {
    param($JobObject)

    $jobId = ""
    try { $jobId = [string]$JobObject.JobId } catch {}
    if ([string]::IsNullOrWhiteSpace($jobId)) {
        try { $jobId = [string]$JobObject.Id } catch {}
    }

    if ([string]::IsNullOrWhiteSpace($jobId)) {
        throw "Falha ao obter JobId/Id do retorno do Start-AzAutomationRunbook."
    }

    return $jobId
}

function Get-ChildLogFilePath {
    param([string]$RunbookName,[string]$JobId)
    $safeName = ($RunbookName -replace '[^a-zA-Z0-9\-_]', '_')
    return (Join-Path $LogDir ("child_{0}_{1}.log" -f $safeName, $JobId))
}

function Write-ChildStreams {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$RunbookName
    )

    $childLogFile = Get-ChildLogFilePath -RunbookName $RunbookName -JobId $JobId
    $lines = New-Object System.Collections.ArrayList
    [void]$lines.Add(("===== INICIO CHILD STREAMS | Runbook={0} | JobId={1} =====" -f $RunbookName, $JobId))

    foreach ($streamType in @("Output", "Error", "Warning", "Verbose", "Progress", "Debug")) {
        try {
            $items = Get-AzAutomationJobOutput -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Id $JobId -Stream $streamType -ErrorAction SilentlyContinue

            foreach ($item in @($items)) {
                if ($null -eq $item) { continue }

                $text = ""
                try {
                    $record = Get-AzAutomationJobOutputRecord -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -JobId $JobId -Id $item.Id -ErrorAction SilentlyContinue
                    if ($null -ne $record -and $null -ne $record.Value) {
                        $text = ($record.Value | Out-String).Trim()
                    }
                }
                catch {}

                if ([string]::IsNullOrWhiteSpace($text)) {
                    try { $text = ($item.Summary | Out-String).Trim() } catch {}
                }

                if ([string]::IsNullOrWhiteSpace($text)) {
                    try { $text = ($item.Value | Out-String).Trim() } catch {}
                }

                if (-not [string]::IsNullOrWhiteSpace($text)) {
                    [void]$lines.Add(("[{0}] {1}" -f $streamType, $text))
                }
            }
        }
        catch {
            [void]$lines.Add(("[WARN] Falha ao ler stream {0}: {1}" -f $streamType, $_.Exception.Message))
        }
    }

    [void]$lines.Add(("===== FIM CHILD STREAMS | Runbook={0} | JobId={1} =====" -f $RunbookName, $JobId))
    Set-Content -Path $childLogFile -Value @($lines) -Encoding UTF8

    foreach ($line in @($lines)) {
        try { Add-Content -Path $MasterLogFile -Value $line -Encoding UTF8 } catch {}
    }

    return $childLogFile
}

function Get-ChildFailureDetail {
    param([Parameter(Mandatory = $true)][string]$ChildLogFile)

    if ([string]::IsNullOrWhiteSpace($ChildLogFile) -or -not (Test-Path -Path $ChildLogFile)) {
        return ""
    }

    try {
        $errorLines = Get-Content -Path $ChildLogFile -ErrorAction Stop | Where-Object {
            $_ -match '^\[Error\]' -or $_ -match '^\[Warning\]' -or $_ -match '^\[Output\]'
        }

        if (@($errorLines).Count -gt 0) {
            return ((@($errorLines | Select-Object -First 8)) -join " || ")
        }
    }
    catch {}

    return ""
}

function Test-JobHasErrorStream {
    param([string]$JobId)

    try {
        $errors = Get-AzAutomationJobOutput -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Id $JobId -Stream Error -ErrorAction SilentlyContinue
        return (@($errors).Count -gt 0)
    }
    catch {
        return $false
    }
}

function Start-ChildRunbook {
    param([string]$Name,[string]$RunOn = "")

    if (-not [string]::IsNullOrWhiteSpace($PipelineDate)) {
        if (-not [string]::IsNullOrWhiteSpace($RunOn)) {
            return Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Name $Name -Parameters @{ PipelineDate = $PipelineDate } -RunOn $RunOn -ErrorAction Stop
        }
        else {
            return Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Name $Name -Parameters @{ PipelineDate = $PipelineDate } -ErrorAction Stop
        }
    }
    else {
        if (-not [string]::IsNullOrWhiteSpace($RunOn)) {
            return Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Name $Name -RunOn $RunOn -ErrorAction Stop
        }
        else {
            return Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Name $Name -ErrorAction Stop
        }
    }
}

function Get-StorageContext {
    if ([string]::IsNullOrWhiteSpace($StorageAccountName) -or [string]::IsNullOrWhiteSpace($StorageContainerName)) {
        return $null
    }

    if (-not [string]::IsNullOrWhiteSpace($StorageSubscriptionId)) {
        Log -Message ("Ajustando contexto para subscription do storage: {0}" -f $StorageSubscriptionId)
        Set-AzContext -SubscriptionId $StorageSubscriptionId -ErrorAction Stop | Out-Null
    }

    $storageAccount = $null
    if (-not [string]::IsNullOrWhiteSpace($StorageResourceGroupName)) {
        $storageAccount = Get-AzStorageAccount -Name $StorageAccountName -ResourceGroupName $StorageResourceGroupName -ErrorAction Stop
    }
    else {
        $all = Get-AzStorageAccount -ErrorAction Stop
        $storageAccount = @($all | Where-Object { $_.StorageAccountName -eq $StorageAccountName -or $_.Name -eq $StorageAccountName }) | Select-Object -First 1
    }

    if ($null -eq $storageAccount) {
        throw ("Storage Account '{0}' não encontrado." -f $StorageAccountName)
    }

    return $storageAccount.Context
}

function Upload-LogsToBlob {
    if ([string]::IsNullOrWhiteSpace($StorageAccountName) -or [string]::IsNullOrWhiteSpace($StorageContainerName)) {
        Log -Message "StorageAccountName/StorageContainerName não informados. Upload para blob será ignorado." -Level "WARN"
        return
    }

    $storageCtx = Get-StorageContext
    if ($null -eq $storageCtx) {
        throw "Não foi possível obter o contexto do storage."
    }

    $blobBase = "{0}/master/dt={1}/run={2}" -f $LogFolderPrefix.Trim('/'), $ExecutionDate, $RunId
    Log -Message ("Enviando logs para blob em: {0}" -f $blobBase)

    $files = Get-ChildItem -Path $LogDir -File -ErrorAction Stop
    foreach ($file in @($files)) {
        Set-AzStorageBlobContent -Context $storageCtx -Container $StorageContainerName -File $file.FullName -Blob ("{0}/{1}" -f $blobBase, $file.Name) -Force -ErrorAction Stop | Out-Null
        Log -Message ("Upload concluído: {0}/{1}" -f $blobBase, $file.Name) -Level "SUCCESS"
    }
}

function Run-Child {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)][string]$RunOn = ""
    )

    $startedAt = Get-Date
    $jobId = ""
    $status = ""
    $childLogFile = ""
    $failureDetail = ""

    Log -Message ("================ INÍCIO [{0}] ================" -f $Name)
    if (-not [string]::IsNullOrWhiteSpace($RunOn)) {
        Log -Message ("RunOn configurado para {0}: {1}" -f $Name, $RunOn)
    }

    try {
        Validate-Runbook -Name $Name

        Log -Message ("Disparando runbook {0}..." -f $Name)
        $job = Start-ChildRunbook -Name $Name -RunOn $RunOn
        if ($null -eq $job) {
            throw "Start-AzAutomationRunbook retornou nulo."
        }

        $jobId = Resolve-JobId -JobObject $job
        Log -Message ("JobId retornado para {0}: {1}" -f $Name, $jobId) -Level "SUCCESS"

        Log -Message ("Aguardando conclusão do runbook {0}..." -f $Name)
        $watch = [System.Diagnostics.Stopwatch]::StartNew()

        while ($true) {
            Start-Sleep -Seconds 10

            $jobStatus = Get-AzAutomationJob -AutomationAccountName $AutomationAccountName -ResourceGroupName $AutomationResourceGroup -Id $jobId -ErrorAction Stop
            if ($null -eq $jobStatus) {
                throw ("Status do job {0} não retornado." -f $jobId)
            }

            $status = [string]$jobStatus.Status
            Log -Message ("Status atual [{0}] = {1}" -f $Name, $status)

            if ($status -in @("Completed", "Failed", "Stopped", "Suspended")) {
                break
            }

            if ($watch.Elapsed.TotalMinutes -gt $ChildRunbookTimeoutMinutes) {
                throw ("Timeout aguardando o runbook {0} após {1} minutos." -f $Name, $ChildRunbookTimeoutMinutes)
            }
        }

        Log -Message ("Runbook {0} finalizado com status {1}. Coletando streams..." -f $Name, $status)
        $childLogFile = Write-ChildStreams -JobId $jobId -RunbookName $Name
        $failureDetail = Get-ChildFailureDetail -ChildLogFile $childLogFile

        if ($status -ne "Completed") {
            $msg = "Runbook falhou. Nome={0} | JobId={1} | Status={2}" -f $Name, $jobId, $status
            if (-not [string]::IsNullOrWhiteSpace($failureDetail)) {
                $msg = "{0} | Detalhe={1}" -f $msg, $failureDetail
            }
            $msg = "{0} | ChildLog={1}" -f $msg, $childLogFile

            Add-RunSummary -RunbookName $Name -JobId $jobId -Status $status -StartedAt $startedAt -EndedAt (Get-Date) -RunOn $RunOn -ChildLogFile $childLogFile -Message $msg
            Log -Message $msg -Level "ERROR"
            Publish-RunbookFailure -Message $msg
            throw $msg
        }

        if (Test-JobHasErrorStream -JobId $jobId) {
            $warnMsg = "Runbook {0} concluiu como Completed, porém há eventos no stream Error. JobId={1} | ChildLog={2}" -f $Name, $jobId, $childLogFile
            if (-not [string]::IsNullOrWhiteSpace($failureDetail)) {
                $warnMsg = "{0} | Detalhe={1}" -f $warnMsg, $failureDetail
            }

            Log -Message $warnMsg -Level "WARN"
            Add-RunSummary -RunbookName $Name -JobId $jobId -Status "CompletedWithErrorSignals" -StartedAt $startedAt -EndedAt (Get-Date) -RunOn $RunOn -ChildLogFile $childLogFile -Message $warnMsg
        }
        else {
            Add-RunSummary -RunbookName $Name -JobId $jobId -Status "Completed" -StartedAt $startedAt -EndedAt (Get-Date) -RunOn $RunOn -ChildLogFile $childLogFile -Message "Execução concluída com sucesso."
        }

        Log -Message ("Runbook {0} concluído com sucesso. Liberando próximo da sequência." -f $Name) -Level "SUCCESS"
        Log -Message ("================ FIM [{0}] ================" -f $Name)
    }
    catch {
        if (-not [string]::IsNullOrWhiteSpace($jobId) -and [string]::IsNullOrWhiteSpace($childLogFile)) {
            try { $childLogFile = Write-ChildStreams -JobId $jobId -RunbookName $Name } catch {}
        }

        if ([string]::IsNullOrWhiteSpace($failureDetail) -and -not [string]::IsNullOrWhiteSpace($childLogFile)) {
            try { $failureDetail = Get-ChildFailureDetail -ChildLogFile $childLogFile } catch {}
        }

        $finalMsg = "Falha no child. Nome={0} | JobId={1} | Status={2} | Motivo={3}" -f $Name, $(if ([string]::IsNullOrWhiteSpace($jobId)) { "N/A" } else { $jobId }), $(if ([string]::IsNullOrWhiteSpace($status)) { "Exception" } else { $status }), $_.Exception.Message
        if (-not [string]::IsNullOrWhiteSpace($failureDetail)) {
            $finalMsg = "{0} | Detalhe={1}" -f $finalMsg, $failureDetail
        }
        if (-not [string]::IsNullOrWhiteSpace($childLogFile)) {
            $finalMsg = "{0} | ChildLog={1}" -f $finalMsg, $childLogFile
        }

        Add-RunSummary -RunbookName $Name -JobId $(if ([string]::IsNullOrWhiteSpace($jobId)) { "N/A" } else { $jobId }) -Status $(if ([string]::IsNullOrWhiteSpace($status)) { "Exception" } else { $status }) -StartedAt $startedAt -EndedAt (Get-Date) -RunOn $RunOn -ChildLogFile $childLogFile -Message $finalMsg

        Log -Message $finalMsg -Level "ERROR"
        Publish-RunbookFailure -Message $finalMsg
        Log -Message ("================ FIM [{0}] ================" -f $Name)
        throw $finalMsg
    }
}

$Pipeline = @(
    @{ Name = "finops-rateio-01-build-inventory";                                 RunOn = "" },
    @{ Name = "finops-rateio-02-build-fact-cost";                                 RunOn = "" },
    @{ Name = "finops-rateio-03-build-overrides-rg-auto";                         RunOn = "" },
    @{ Name = "finops-rateio-04-build-resource-to-service";                       RunOn = "" },
    @{ Name = "finops-rateio-05-build-client-weights";                            RunOn = "" },
    @{ Name = "finops-rateio-06-build-allocation-share-servicekey";               RunOn = "" },
    @{ Name = "finops-rateio-07-build-sqlpool-allocation-share-from-metrics";     RunOn = "" },
    @{ Name = "finops-rateio-08-build-aks-allocation-from-opencost";              RunOn = $HybridWorkerGroup },
    @{ Name = "finops-rateio-09_build_allocation_share_servicekey_final_runbook"; RunOn = "" },
    @{ Name = "finops-rateio-10_build_fact_allocated_cost_runbook";               RunOn = "" }
)

$masterFailed = $false
try {
    foreach ($p in @($Pipeline)) {
        Run-Child -Name ([string]$p.Name) -RunOn ([string]$p.RunOn)
    }

    Log -Message "PIPELINE FINALIZADO COM SUCESSO." -Level "SUCCESS"
}
catch {
    $masterFailed = $true
    $masterMsg = "PIPELINE FALHOU. Runbook interrompido. Motivo={0}" -f $_.Exception.Message
    Log -Message $masterMsg -Level "ERROR"
    Publish-RunbookFailure -Message $masterMsg
    throw $masterMsg
}
finally {
    try {
        Save-RunSummaryToDisk
        Log -Message ("Resumo salvo em: {0}" -f $SummaryJsonFile) -Level "DEBUG"
    }
    catch {
        Log -Message ("Falha ao salvar summary JSON: {0}" -f $_.Exception.Message) -Level "WARN"
    }

    try {
        Upload-LogsToBlob
    }
    catch {
        Log -Message ("Falha no upload dos logs para blob: {0}" -f $_.Exception.Message) -Level "ERROR"
    }

    if ($masterFailed) {
        Log -Message "Master finalizado com erro." -Level "ERROR"
    }
    else {
        Log -Message "Master finalizado com sucesso." -Level "SUCCESS"
    }
}
