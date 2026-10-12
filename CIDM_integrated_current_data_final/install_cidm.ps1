param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [string]$DataRoot = "",
    [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path $RepoRoot).Path
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ReplacementRoot = Join-Path $PackageRoot "src\company_platform"
$TargetApi = Join-Path $RepoRoot "src\company_platform\api"
$TargetWeb = Join-Path $RepoRoot "src\company_platform\web"
$TargetRunScript = Join-Path $RepoRoot "run_cidm.ps1"
$SourceRunScript = Join-Path $PackageRoot "run_cidm.ps1"
$PythonConfig = Join-Path $RepoRoot ".cidm_python_path"
$DataConfig = Join-Path $RepoRoot ".cidm_data_root"

if (-not (Test-Path (Join-Path $ReplacementRoot "api\app.py"))) {
    throw "Replacement API source is missing from the package. No changes made."
}
if (-not (Test-Path (Join-Path $ReplacementRoot "web\assets\app.js"))) {
    throw "Replacement website assets are missing from the package. No changes made."
}
if (-not (Test-Path $SourceRunScript)) {
    throw "run_cidm.ps1 is missing from the package. No changes made."
}
if (-not (Test-Path (Join-Path $PackageRoot "requirements-api.txt"))) {
    throw "requirements-api.txt is missing from the package. No changes made."
}
if (-not (Test-Path $TargetApi) -or -not (Test-Path $TargetWeb)) {
    throw "Expected src\company_platform\api and src\company_platform\web in the target repository. No changes made."
}

if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    $DataRoot = Join-Path $RepoRoot "data"
}
$DataRoot = (Resolve-Path $DataRoot).Path
$RequiredData = @(
    (Join-Path $DataRoot "silver\platform\company_registry.parquet"),
    (Join-Path $DataRoot "silver\platform\structured_events.db"),
    (Join-Path $DataRoot "silver\platform\control.db"),
    (Join-Path $DataRoot "gold\unified_company_intelligence\unified_company_view\unified_company_view.parquet")
)
$Missing = @($RequiredData | Where-Object { -not (Test-Path $_) })
if ($Missing.Count -gt 0) {
    throw ("The current data root is incomplete; no changes made. Missing: " + ($Missing -join ", "))
}

if ([string]::IsNullOrWhiteSpace($PythonPath)) {
    $PythonPath = (Get-Command python -ErrorAction Stop).Source
} elseif (-not (Test-Path $PythonPath)) {
    throw "PythonPath does not exist: $PythonPath. No source changes made."
} else {
    $PythonPath = (Resolve-Path $PythonPath).Path
}
& $PythonPath -c "import fastapi, uvicorn, pydantic, pandas, pyarrow" *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing required API runtime packages into $PythonPath ..."
    & $PythonPath -m pip install -r (Join-Path $PackageRoot "requirements-api.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "API dependencies could not be installed. No project source files were changed. Activate the project's Python environment or pass -PythonPath explicitly, then rerun."
    }
}

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupRoot = Join-Path $RepoRoot "backups\cidm_presentation_$Stamp"
$StageRoot = Join-Path $RepoRoot ".cidm_rebuild_staging_$Stamp"
New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null
New-Item -ItemType Directory -Path $StageRoot -Force | Out-Null

try {
    $BackupApi = Join-Path $BackupRoot "api"
    $BackupWeb = Join-Path $BackupRoot "web"
    New-Item -ItemType Directory -Path $BackupApi -Force | Out-Null
    New-Item -ItemType Directory -Path $BackupWeb -Force | Out-Null
    Copy-Item -Path (Join-Path $TargetApi "*") -Destination $BackupApi -Recurse -Force
    Copy-Item -Path (Join-Path $TargetWeb "*") -Destination $BackupWeb -Recurse -Force
    if (Test-Path $TargetRunScript) { Copy-Item -Path $TargetRunScript -Destination (Join-Path $BackupRoot "run_cidm.ps1") }
    if (Test-Path $PythonConfig) { Copy-Item -Path $PythonConfig -Destination (Join-Path $BackupRoot ".cidm_python_path") }
    if (Test-Path $DataConfig) { Copy-Item -Path $DataConfig -Destination (Join-Path $BackupRoot ".cidm_data_root") }
    $TargetTests = Join-Path $RepoRoot "tests\integration"
    if (Test-Path $TargetTests) {
        New-Item -ItemType Directory -Path (Join-Path $BackupRoot "tests\integration") -Force | Out-Null
        foreach ($TestName in @("test_step32_backend_api.py", "test_step33_website_foundation.py")) {
            $ExistingTest = Join-Path $TargetTests $TestName
            if (Test-Path $ExistingTest) { Copy-Item $ExistingTest (Join-Path $BackupRoot "tests\integration\$TestName") }
        }
    }
    $StageApi = Join-Path $StageRoot "api"
    $StageWeb = Join-Path $StageRoot "web"
    New-Item -ItemType Directory -Path $StageApi -Force | Out-Null
    New-Item -ItemType Directory -Path $StageWeb -Force | Out-Null
    Copy-Item -Path (Join-Path $ReplacementRoot "api\*") -Destination $StageApi -Recurse -Force
    Copy-Item -Path (Join-Path $ReplacementRoot "web\*") -Destination $StageWeb -Recurse -Force

    Remove-Item -Path $TargetApi -Recurse -Force
    Remove-Item -Path $TargetWeb -Recurse -Force
    New-Item -ItemType Directory -Path (Split-Path -Parent $TargetApi) -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $TargetWeb) -Force | Out-Null
    Move-Item -Path (Join-Path $StageRoot "api") -Destination $TargetApi
    Move-Item -Path (Join-Path $StageRoot "web") -Destination $TargetWeb
    Copy-Item -Path $SourceRunScript -Destination $TargetRunScript -Force
    Set-Content -Path $PythonConfig -Value $PythonPath -Encoding UTF8
    Set-Content -Path $DataConfig -Value $DataRoot -Encoding UTF8
    $TargetTests = Join-Path $RepoRoot "tests\integration"
    if (Test-Path $TargetTests) {
        foreach ($TestName in @("test_step32_backend_api.py", "test_step33_website_foundation.py")) {
            $SourceTest = Join-Path $PackageRoot "tests\integration\$TestName"
            if (Test-Path $SourceTest) { Copy-Item $SourceTest (Join-Path $TargetTests $TestName) -Force }
        }
    }

    if (-not (Test-Path (Join-Path $TargetWeb "assets\app.js")) -or -not (Test-Path (Join-Path $TargetApi "service.py")) -or -not (Test-Path $TargetRunScript)) {
        throw "Post-install validation failed."
    }
} catch {
    Write-Error "Install failed; restoring the backed-up API and website."
    if (Test-Path $TargetApi) { Remove-Item $TargetApi -Recurse -Force }
    if (Test-Path $TargetWeb) { Remove-Item $TargetWeb -Recurse -Force }
    if (Test-Path (Join-Path $BackupRoot "api")) {
        New-Item -ItemType Directory -Path $TargetApi -Force | Out-Null
        Copy-Item -Path (Join-Path $BackupRoot "api\*") -Destination $TargetApi -Recurse -Force
    }
    if (Test-Path (Join-Path $BackupRoot "web")) {
        New-Item -ItemType Directory -Path $TargetWeb -Force | Out-Null
        Copy-Item -Path (Join-Path $BackupRoot "web\*") -Destination $TargetWeb -Recurse -Force
    }
    if (Test-Path (Join-Path $BackupRoot "run_cidm.ps1")) { Copy-Item (Join-Path $BackupRoot "run_cidm.ps1") $TargetRunScript -Force }
    elseif (Test-Path $TargetRunScript) { Remove-Item $TargetRunScript -Force }
    if (Test-Path (Join-Path $BackupRoot ".cidm_python_path")) { Copy-Item (Join-Path $BackupRoot ".cidm_python_path") $PythonConfig -Force }
    elseif (Test-Path $PythonConfig) { Remove-Item $PythonConfig -Force }
    if (Test-Path (Join-Path $BackupRoot ".cidm_data_root")) { Copy-Item (Join-Path $BackupRoot ".cidm_data_root") $DataConfig -Force }
    elseif (Test-Path $DataConfig) { Remove-Item $DataConfig -Force }
    $TargetTests = Join-Path $RepoRoot "tests\integration"
    if (Test-Path $TargetTests) {
        foreach ($TestName in @("test_step32_backend_api.py", "test_step33_website_foundation.py")) {
            $BackupTest = Join-Path $BackupRoot "tests\integration\$TestName"
            if (Test-Path $BackupTest) { Copy-Item $BackupTest (Join-Path $TargetTests $TestName) -Force }
        }
    }
    throw
} finally {
    if (Test-Path $StageRoot) { Remove-Item $StageRoot -Recurse -Force -ErrorAction SilentlyContinue }
}

Write-Host "CIDM presentation layer installed successfully." -ForegroundColor Green
Write-Host "Existing data and ingestion/intelligence pipeline files were not modified."
Write-Host "Python runtime: $PythonPath"
Write-Host "Backup: $BackupRoot"
Write-Host "Data root: $DataRoot"
Write-Host "Next: run .\run_cidm.ps1 -Verify, then .\run_cidm.ps1"
