param(
    [Parameter(Mandatory = $true)]
    [string]$DatasetId,
    [string]$ProjectId,
    [string]$Region,
    [string]$BqLocation,
    [string]$Environment,
    [switch]$SkipTerraform,
    [switch]$SkipValidate,
    [switch]$AutoApprove
)

$ErrorActionPreference = "Stop"

if ($DatasetId -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    throw "Dataset id must match ^[A-Za-z_][A-Za-z0-9_]*$"
}

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EnvFile = Join-Path $ProjectRoot ".env"
$EnvExample = Join-Path $ProjectRoot ".env.example"
$TfvarsFile = Join-Path $ProjectRoot "infra\terraform.tfvars"
$TfvarsExample = Join-Path $ProjectRoot "infra\terraform.tfvars.example"

function Upsert-EnvVar {
    param(
        [string]$Key,
        [string]$Value,
        [string]$FilePath
    )

    $line = "$Key=$Value"
    if (-not (Test-Path $FilePath)) {
        Set-Content -Path $FilePath -Value $line
        return
    }

    $content = Get-Content -Path $FilePath
    $updated = $false
    $newContent = foreach ($entry in $content) {
        if ($entry -match "^$([regex]::Escape($Key))=") {
            $updated = $true
            $line
        }
        else {
            $entry
        }
    }

    if (-not $updated) {
        $newContent += $line
    }

    Set-Content -Path $FilePath -Value $newContent
}

function Remove-EnvVar {
    param(
        [string]$Key,
        [string]$FilePath
    )

    if (-not (Test-Path $FilePath)) {
        return
    }

    $content = Get-Content -Path $FilePath | Where-Object {
        $_ -notmatch "^$([regex]::Escape($Key))="
    }

    Set-Content -Path $FilePath -Value $content
}

function Upsert-TfvarsString {
    param(
        [string]$Key,
        [string]$Value,
        [string]$FilePath
    )

    $line = "$Key  = ""$Value"""
    if (-not (Test-Path $FilePath)) {
        Set-Content -Path $FilePath -Value $line
        return
    }

    $content = Get-Content -Path $FilePath
    $updated = $false
    $newContent = foreach ($entry in $content) {
        if ($entry -match "^$([regex]::Escape($Key))\s*=") {
            $updated = $true
            $line
        }
        else {
            $entry
        }
    }

    if (-not $updated) {
        $newContent += $line
    }

    Set-Content -Path $FilePath -Value $newContent
}

function Remove-TfvarsKey {
    param(
        [string]$Key,
        [string]$FilePath
    )

    if (-not (Test-Path $FilePath)) {
        return
    }

    $content = Get-Content -Path $FilePath | Where-Object {
        $_ -notmatch "^$([regex]::Escape($Key))\s*="
    }

    Set-Content -Path $FilePath -Value $content
}

function Get-TfvarsValue {
    param(
        [string]$Key,
        [string]$FilePath
    )

    if (-not (Test-Path $FilePath)) {
        return ""
    }

    foreach ($entry in Get-Content -Path $FilePath) {
        if ($entry -match "^$([regex]::Escape($Key))\s*=\s*""([^""]*)""") {
            return $Matches[1]
        }
    }

    return ""
}

Write-Host "Configuring dataset '$DatasetId'..."

if (-not (Test-Path $EnvFile)) {
    Copy-Item $EnvExample $EnvFile
    Write-Host "Created $EnvFile from .env.example"
}

Upsert-EnvVar -Key "THALASSA_BQ_DATASET" -Value $DatasetId -FilePath $EnvFile
Remove-EnvVar -Key "THALASSA_INTELLIGENCE_TABLE" -FilePath $EnvFile
if ($ProjectId) {
    Upsert-EnvVar -Key "THALASSA_BQ_PROJECT" -Value $ProjectId -FilePath $EnvFile
}
if ($BqLocation) {
    Upsert-EnvVar -Key "THALASSA_BQ_LOCATION" -Value $BqLocation -FilePath $EnvFile
}

Push-Location $ProjectRoot
try {
    Write-Host "Syncing Bruin asset prefixes..."
    uv run --no-project python .\scripts\sync_bruin_dataset.py

    if (-not $SkipValidate) {
        Write-Host "Validating pipeline..."
        bruin validate .\pipeline --fast
    }

    if ($SkipTerraform) {
        Write-Host "Skipping Terraform."
        exit 0
    }

    if (-not (Test-Path $TfvarsFile)) {
        Copy-Item $TfvarsExample $TfvarsFile
        Write-Host "Created $TfvarsFile from the example."
    }

    Remove-TfvarsKey -Key "dataset_id" -FilePath $TfvarsFile
    if ($ProjectId) {
        Upsert-TfvarsString -Key "project_id" -Value $ProjectId -FilePath $TfvarsFile
    }
    if ($Region) {
        Upsert-TfvarsString -Key "region" -Value $Region -FilePath $TfvarsFile
    }
    if ($BqLocation) {
        Upsert-TfvarsString -Key "bq_location" -Value $BqLocation -FilePath $TfvarsFile
    }
    if ($Environment) {
        Upsert-TfvarsString -Key "environment" -Value $Environment -FilePath $TfvarsFile
    }

    $TfProjectId = Get-TfvarsValue -Key "project_id" -FilePath $TfvarsFile
    $TfBqLocation = Get-TfvarsValue -Key "bq_location" -FilePath $TfvarsFile
    $TfEnvironment = Get-TfvarsValue -Key "environment" -FilePath $TfvarsFile

    if (-not $TfProjectId -or $TfProjectId -eq "your-gcp-project-id") {
        throw "Missing real project_id. If you only want to switch the dataset locally, rerun with -SkipTerraform. Otherwise rerun with -ProjectId or edit infra/terraform.tfvars."
    }
    if (-not $TfBqLocation) {
        throw "Missing bq_location. If you only want to switch the dataset locally, rerun with -SkipTerraform. Otherwise rerun with -BqLocation or edit infra/terraform.tfvars."
    }
    if (-not $TfEnvironment) {
        throw "Missing environment. If you only want to switch the dataset locally, rerun with -SkipTerraform. Otherwise rerun with -Environment or edit infra/terraform.tfvars."
    }

    Write-Host "Running Terraform..."
    terraform -chdir=infra init
    terraform -chdir=infra plan -var "dataset_id=$DatasetId"
    if ($AutoApprove) {
        terraform -chdir=infra apply -auto-approve -var "dataset_id=$DatasetId"
    }
    else {
        terraform -chdir=infra apply -var "dataset_id=$DatasetId"
    }

    Write-Host "Done. Dataset is set to '$DatasetId'."
}
finally {
    Pop-Location
}
