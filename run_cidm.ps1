param(
    [string]$DataRoot = "",
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$Verify
)
$ErrorActionPreference = "Stop"
$env:PYTHONPATH = (Join-Path $PSScriptRoot "src") + [IO.Path]::PathSeparator + $env:PYTHONPATH
if (-not [string]::IsNullOrWhiteSpace($DataRoot)) {
    $env:CIDM_DATA_DIR = (Resolve-Path $DataRoot).Path
} else {
    $DataConfig = Join-Path $PSScriptRoot ".cidm_data_root"
    if (Test-Path $DataConfig) {
        $ConfiguredDataRoot = (Get-Content $DataConfig -Raw).Trim()
        if ([string]::IsNullOrWhiteSpace($ConfiguredDataRoot) -or -not (Test-Path $ConfiguredDataRoot)) {
            throw "Configured CIDM data root is missing: $ConfiguredDataRoot"
        }
        $env:CIDM_DATA_DIR = (Resolve-Path $ConfiguredDataRoot).Path
    }
}
$Python = ""
$PythonConfig = Join-Path $PSScriptRoot ".cidm_python_path"
if (Test-Path $PythonConfig) {
    $ConfiguredPython = (Get-Content $PythonConfig -Raw).Trim()
    if (-not [string]::IsNullOrWhiteSpace($ConfiguredPython) -and (Test-Path $ConfiguredPython)) { $Python = $ConfiguredPython }
}
if ([string]::IsNullOrWhiteSpace($Python)) { $Python = (Get-Command python -ErrorAction Stop).Source }
Push-Location $PSScriptRoot
$ExitCode = 0
try {
    if ($Verify) {
        & $Python -m company_platform.api.verify_integration
        $ExitCode = $LASTEXITCODE
    } else {
        & $Python -m uvicorn company_platform.api.app:app --host $HostAddress --port $Port
        $ExitCode = $LASTEXITCODE
    }
} finally {
    Pop-Location
}
exit $ExitCode
