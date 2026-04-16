$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Get-PythonCommand {
    $pyList = & py -0p 2>$null
    if ($LASTEXITCODE -eq 0 -and $pyList -match "-V:3.10") {
        return @("py", "-3.10")
    }
    return @("python")
}

function Invoke-Python {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    if ($pythonCmd.Length -gt 1) {
        & $pythonCmd[0] $pythonCmd[1] @Args
        return
    }

    & $pythonCmd[0] @Args
}

$pythonCmd = Get-PythonCommand
$pythonExe = $pythonCmd -join " "

Write-Host ""
Write-Host "========================================================="
Write-Host " Supply Chain AI - Local Startup"
Write-Host "========================================================="
Write-Host "Python: $pythonExe"
Write-Host ""

New-Item -ItemType Directory -Force -Path "data\uploads" | Out-Null
New-Item -ItemType Directory -Force -Path "data\csv" | Out-Null

Write-Host "Checking required Python packages..."
Invoke-Python -B -c "import flask, flask_socketio, flask_cors, psycopg2, sklearn, pandas, numpy; print('ok')" 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Missing dependencies detected."
    Write-Host "Install them with:"
    Write-Host "  $pythonExe -m pip install -r requirements.txt"
    exit 1
}

Write-Host "Starting app on http://localhost:5000/dashboard"
Invoke-Python backend/app.py
