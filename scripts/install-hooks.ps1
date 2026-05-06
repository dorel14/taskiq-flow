#!/usr/bin/env pwsh
# Install Git pre-commit hooks for Taskiq-Flow

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$hooksDir = Join-Path $repoRoot ".githooks"
$venvDir = Join-Path $repoRoot ".venv"

Write-Host "🔧 Installing Git pre-commit hooks..." -ForegroundColor Cyan

# Check if .githooks directory exists
if (-not (Test-Path $hooksDir)) {
    New-Item -ItemType Directory -Path $hooksDir -Force | Out-Null
    Write-Host "   Created .githooks/"
}

# Make pre-commit scripts executable (for Unix/WSL)
$preCommitBat = Join-Path $hooksDir "pre-commit.bat"
$preCommitSh = Join-Path $hooksDir "pre-commit"

if (Test-Path $preCommitBat) {
    Write-Host "   pre-commit.bat found"
}

if (Test-Path $preCommitSh) {
    # Set executable bit (works on WSL/Git Bash)
    $wsl = Get-Command wsl -ErrorAction SilentlyContinue
    if ($wsl) {
        wsl chmod +x $preCommitSh
        Write-Host "   Made pre-commit executable (via WSL)"
    } else {
        Write-Host "   pre-commit script (Unix) present"
    }
}

# Configure Git to use the hooks directory
$gitConfig = Join-Path $repoRoot ".git" "config"
if (Test-Path $gitConfig) {
    $currentHooksPath = git config --get core.hooksPath 2>$null
    $expectedHooksPath = $hooksDir
    if ($currentHooksPath -ne $expectedHooksPath) {
        git config core.hooksPath $expectedHooksPath
        Write-Host "   ✅ Set core.hooksPath to $expectedHooksPath"
    } else {
        Write-Host "   ✅ core.hooksPath already set correctly"
    }
} else {
    Write-Warning ".git/config not found. Are you in a git repository?"
}

# Check Python environment
$pythonCmd = $null
$venvPython = Join-Path $venvDir "Scripts\python.exe"
if (Test-Path $venvPython) {
    $pythonCmd = $venvPython
    Write-Host "   ✅ Found Python in .venv"
} else {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        $pythonCmd = "python"
        Write-Host "   Using system Python"
    } else {
        Write-Warning "Python not found. Install Python or create a virtual environment."
    }
}

if ($pythonCmd) {
    Write-Host "   Testing version_updater.py..."
    & $pythonCmd "$repoRoot/scripts/version_updater.py" --check
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ version_updater.py works correctly"
    } else {
        Write-Warning "version_updater.py check failed"
    }
}

Write-Host ""
Write-Host "✅ Pre-commit hooks installed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "What's been set up:"
Write-Host "  • Git will run scripts/version_updater.py --check before each commit"
Write-Host "  • Version consistency is enforced (pyproject.toml, uv.lock, docs)"
Write-Host ""
Write-Host "To bump version manually:"
Write-Host "  python scripts/version_updater.py --bump patch   # 0.4.0 → 0.4.1"
Write-Host "  python scripts/version_updater.py --bump minor   # 0.4.0 → 0.5.0"
Write-Host "  python scripts/version_updater.py --bump major   # 0.4.0 → 1.0.0"
Write-Host ""
Write-Host "To skip version check (temporary):"
Write-Host "  set SKIP_PRE_COMMIT=1   (Windows PowerShell)"
Write-Host "  export SKIP_PRE_COMMIT=1   (Unix/WSL)"
Write-Host ""
Write-Host "Next step: git add, git commit (hook will run automatically)"
