param(
    [string]$ProjectId = "nyc-taxi-driver-analytics",
    [string]$Region = "us-central1",
    [string]$JobName = "taxi-driver-ingest",
    [string]$SourceMonths = "",
    [string]$SourceDate = "",
    [string]$SourceStartDate = "",
    [string]$SourceEndDate = "",
    [switch]$ForceReload,
    [int]$TaskTimeoutSeconds = 3600,
    [int]$WaitTimeoutSeconds = 7200,
    [switch]$NoWait
)

$ErrorActionPreference = "Stop"

function Test-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Get-AccessToken {
    $token = gcloud auth print-access-token
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
        throw "Unable to obtain a gcloud access token. Run 'gcloud auth login' first."
    }
    return $token.Trim()
}

function Invoke-RunApi {
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body = $null
    )

    $headers = @{
        Authorization = "Bearer $script:AccessToken"
    }

    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers
    }

    $jsonBody = $Body | ConvertTo-Json -Depth 20
    return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -ContentType "application/json" -Body $jsonBody
}

function Assert-Selection {
    $selectors = @(@(
            -not [string]::IsNullOrWhiteSpace($SourceMonths),
            -not [string]::IsNullOrWhiteSpace($SourceDate),
            -not [string]::IsNullOrWhiteSpace($SourceStartDate)
        ) | Where-Object { $_ })

    if ($selectors.Count -ne 1) {
        throw "Pass exactly one selector: -SourceMonths, -SourceDate, or -SourceStartDate."
    }

    if (-not [string]::IsNullOrWhiteSpace($SourceEndDate) -and [string]::IsNullOrWhiteSpace($SourceStartDate)) {
        throw "-SourceEndDate can only be used together with -SourceStartDate."
    }
}

function Get-CompletedCondition {
    param([object]$Execution)

    if ($null -eq $Execution.conditions) {
        return $null
    }

    return $Execution.conditions | Where-Object { $_.type -eq "Completed" } | Select-Object -First 1
}

function Wait-Operation {
    param([string]$OperationName)

    $operationUri = "https://run.googleapis.com/v2/$OperationName"
    $startedAt = Get-Date

    while ($true) {
        $operation = Invoke-RunApi -Method "GET" -Uri $operationUri
        if ($operation.done) {
            if ($operation.error) {
                $message = $operation.error.message
                if ([string]::IsNullOrWhiteSpace($message)) {
                    $message = $operation.error | ConvertTo-Json -Depth 10
                }
                throw "Cloud Run operation failed: $message"
            }

            return $operation
        }

        if (((Get-Date) - $startedAt).TotalSeconds -gt $WaitTimeoutSeconds) {
            throw "Timed out waiting for operation $OperationName after $WaitTimeoutSeconds seconds."
        }

        Write-Host "Waiting for Cloud Run operation..."
        Start-Sleep -Seconds 10
    }
}

function Wait-Execution {
    param([string]$ExecutionName)

    $executionUri = "https://run.googleapis.com/v2/$ExecutionName"
    $startedAt = Get-Date

    while ($true) {
        $execution = Invoke-RunApi -Method "GET" -Uri $executionUri
        $completed = Get-CompletedCondition -Execution $execution

        if ($completed) {
            if ($completed.status -eq "True" -or $completed.state -eq "CONDITION_SUCCEEDED") {
                return $execution
            }

            if ($completed.status -eq "False" -or $completed.state -eq "CONDITION_FAILED") {
                $message = $completed.message
                if ([string]::IsNullOrWhiteSpace($message)) {
                    $message = $execution | ConvertTo-Json -Depth 10
                }
                throw "Cloud Run execution failed: $message"
            }
        }

        if (((Get-Date) - $startedAt).TotalSeconds -gt $WaitTimeoutSeconds) {
            throw "Timed out waiting for execution $ExecutionName after $WaitTimeoutSeconds seconds."
        }

        Write-Host "Waiting for Cloud Run execution..."
        Start-Sleep -Seconds 15
    }
}

Test-Command -Name "gcloud"
Assert-Selection

$envOverrides = @(
    @{ name = "SOURCE_MONTHS"; value = $SourceMonths },
    @{ name = "SOURCE_DATE"; value = $SourceDate },
    @{ name = "SOURCE_START_DATE"; value = $SourceStartDate },
    @{ name = "SOURCE_END_DATE"; value = $SourceEndDate },
    @{ name = "FORCE_RELOAD"; value = $ForceReload.IsPresent.ToString().ToLowerInvariant() }
)

$body = @{
    overrides = @{
        containerOverrides = @(
            @{
                env = $envOverrides
            }
        )
        timeout = "${TaskTimeoutSeconds}s"
    }
}

$script:AccessToken = Get-AccessToken
$runUri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/$JobName`:run"

Write-Host "Starting Cloud Run Job execution with overrides"
Write-Host "Project: $ProjectId"
Write-Host "Region:  $Region"
Write-Host "Job:     $JobName"
if (-not [string]::IsNullOrWhiteSpace($SourceMonths)) { Write-Host "Months:  $SourceMonths" }
if (-not [string]::IsNullOrWhiteSpace($SourceDate)) { Write-Host "Date:    $SourceDate" }
if (-not [string]::IsNullOrWhiteSpace($SourceStartDate)) { Write-Host "Range:   $SourceStartDate to $SourceEndDate" }
Write-Host "Force:   $($ForceReload.IsPresent)"

$operation = Invoke-RunApi -Method "POST" -Uri $runUri -Body $body

if ($NoWait) {
    Write-Host "Operation started:"
    Write-Host $operation.name
    exit 0
}

$completedOperation = Wait-Operation -OperationName $operation.name

if ($completedOperation.response -and $completedOperation.response.name) {
    $execution = Wait-Execution -ExecutionName $completedOperation.response.name
    Write-Host "Execution completed:"
    Write-Host $execution.name
}
else {
    Write-Host "Operation completed:"
    Write-Host $completedOperation.name
}
