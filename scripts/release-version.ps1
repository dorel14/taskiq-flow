#!/usr/bin/env pwsh
# Automate version bump, commit, tagging, and push for Taskiq-Flow documentation
# Usage: .\scripts\release-version.ps1 -Version 0.4.0 [-Message "Release message"]

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [string]$Message = "Release v$Version - Documentation update",

    [switch]$Push,

    [string]$Branch = "develop"
)

$ErrorActionPreference = "Stop"

# Validate version format (semver)
if ($Version -notmatch '^\d+\.\d+\.\d+$') {
    Write-Error "Version must be in format X.Y.Z (e.g., 0.4.0)"
    exit 1
}

Write-Host "🚀 Starting release process for version $Version" -ForegroundColor Cyan
Write-Host "Branch: $Branch" -ForegroundColor Yellow
Write-Host "Message: $Message" -ForegroundColor Yellow
Write-Host ""

# 1. Bump version in all files
Write-Host "📝 Step 1: Bumping version in documentation files..." -ForegroundColor Green
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_config\.yml$'
}

$oldVersion = "0.3.2"  # We assume current version; could detect from README
$updatedCount = 0

foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    if ($content -match [regex]::Escape($oldVersion)) {
        $new = $content -replace [regex]::Escape($oldVersion), $Version
        Set-Content $file.FullName -Value $new -NoNewline
        $updatedCount++
    }
}

# Update _config.yml footer
$configPath = "$base\_config.yml"
$config = Get-Content $configPath -Raw
if ($config -match [regex]::Escape($oldVersion)) {
    $config = $config -replace [regex]::Escape($oldVersion), $Version
    Set-Content $configPath -Value $config -NoNewline
    $updatedCount++
}

Write-Host "   Updated $updatedCount files to version $Version"

# 2. Stage changes
Write-Host "`n📦 Step 2: Staging changes..." -ForegroundColor Green
git add -A docs/
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to stage changes"
    exit 1
}
Write-Host "   Changes staged"

# 3. Commit
Write-Host "`n💾 Step 3: Creating commit..." -ForegroundColor Green
$commitMsg = "docs: bump version to $Version"
git commit -m $commitMsg
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create commit"
    exit 1
}
Write-Host "   Commit created: $commitMsg"

# 4. Tag
Write-Host "`n🏷️  Step 4: Creating tag v$Version..." -ForegroundColor Green
$tagName = "v$Version"
$tagMsg = "Release v$Version - $Message"
git tag -a $tagName -m $tagMsg
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create tag"
    exit 1
}
Write-Host "   Tag created: $tagName"

# 5. Push (if requested)
if ($Push) {
    Write-Host "`n🚀 Step 5: Pushing to remote..." -ForegroundColor Green

    # Push branch
    Write-Host "   Pushing branch $Branch..."
    git push origin $Branch
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to push branch $Branch"
        exit 1
    }

    # Push tag
    Write-Host "   Pushing tag $tagName..."
    git push origin $tagName
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to push tag"
        exit 1
    }

    Write-Host "   ✅ Pushed branch and tag to remote"
} else {
    Write-Host "`n📍 Step 5: Skipping push (use -Push to push to remote)" -ForegroundColor Yellow
}

Write-Host "`n✅ Release v$Version prepared successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:"
if (-not $Push) {
    Write-Host "  git push origin $Branch"
    Write-Host "  git push origin $tagName"
}
Write-Host "  Verify at: https://dorel14.github.io/taskiq-flow/"
