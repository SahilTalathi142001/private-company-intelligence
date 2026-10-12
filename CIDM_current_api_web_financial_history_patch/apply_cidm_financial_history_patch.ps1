param(
  [Parameter(Mandatory=$true)]
  [string]$RepoRoot
)
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path $RepoRoot).Path
$targets = @(
  @{ Rel = 'company_platform\api\service.py'; Marker = 'def get_company(cik: str) -> dict[str, Any] | None:' },
  @{ Rel = 'company_platform\api\app.py'; Marker = '@app.get("/api/v1/companies/{cik}/domains/{domain}"' },
  @{ Rel = 'company_platform\web\assets\app.js'; Marker = 'async function renderCompany(cik)' },
  @{ Rel = 'company_platform\web\assets\styles.css'; Marker = ':root{' }
)
foreach ($t in $targets) {
  $path = Join-Path $RepoRoot $t.Rel
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required target not found: $path" }
  $content = Get-Content -LiteralPath $path -Raw
  if (-not $content.Contains($t.Marker)) { throw "Target does not match the expected CIDM source version; refusing to overwrite: $path" }
}
$patchRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$backupRoot = Join-Path $RepoRoot "backups\cidm_financial_history_$stamp"
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null
foreach ($t in $targets) {
  $target = Join-Path $RepoRoot $t.Rel
  $backup = Join-Path $backupRoot $t.Rel
  New-Item -ItemType Directory -Path (Split-Path -Parent $backup) -Force | Out-Null
  Copy-Item -LiteralPath $target -Destination $backup
}
$files = @(
  @{ Source = 'company_platform\api\service.py'; Dest = 'company_platform\api\service.py' },
  @{ Source = 'company_platform\api\app.py'; Dest = 'company_platform\api\app.py' },
  @{ Source = 'company_platform\web\assets\app.js'; Dest = 'company_platform\web\assets\app.js' },
  @{ Source = 'company_platform\web\assets\styles.css'; Dest = 'company_platform\web\assets\styles.css' }
)
foreach ($f in $files) {
  Copy-Item -LiteralPath (Join-Path $patchRoot $f.Source) -Destination (Join-Path $RepoRoot $f.Dest) -Force
}
Write-Host "CIDM financial-history patch applied." -ForegroundColor Green
Write-Host "Backups: $backupRoot"
Write-Host "Next: run the tests in tests\test_financial_history_api.py and restart the API."
