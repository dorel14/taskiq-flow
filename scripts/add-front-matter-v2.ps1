# Robust script: add/fix title and nav_order in front matter of all docs markdown files
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

# Navigation order mapping by relative path within collection
$navOrderMap = @{
    'index.md' = 5
    'quickstart.md' = 10
    'guides/pipelines.md' = 20
    'guides/tasks.md' = 21
    'guides/execution.md' = 22
    'guides/tracking.md' = 23
    'guides/websocket.md' = 24
    'guides/scheduling.md' = 25
    'guides/retry.md' = 26
    'guides/performance.md' = 27
    'guides/api.md' = 28
    'api/core.md' = 30
    'api/decorators.md' = 31
    'api/execution.md' = 32
    'api/tracking.md' = 33
    'api/websocket.md' = 34
    'examples/index.md' = 40
    'examples/quickstart.md' = 41
    'examples/dataflow-audio-pipeline.md' = 42
    'examples/registry-discovery.md' = 43
    'examples/scheduled-pipeline.md' = 44
    'examples/tracking-demo.md' = 45
    'examples/websocket-demo.md' = 46
    'examples/api-example.md' = 47
}

function Get-NavOrder([string]$relativePath) {
    $rel = $relativePath -replace '\\', '/'
    if ($navOrderMap.ContainsKey($rel)) {
        return $navOrderMap[$rel]
    }
    return 999
}

function Extract-FirstHeading([string[]]$lines) {
    foreach ($line in $lines) {
        if ($line -match '^#\s+(.+)$') {
            return $matches[1].Trim()
        }
    }
    return $null
}

# Get all markdown files (excluding configs, gemfile, layouts)
$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_layouts\\' -and
    $_.Name -ne '_config.yml' -and
    $_.Name -ne 'Gemfile'
}

foreach ($file in $files) {
    Write-Host "Processing: $($file.FullName)"
    # Read all lines (preserve original newline style later)
    $contentRaw = Get-Content $file.FullName -Raw
    # Split into lines (preserving empty lines)
    $allLines = $contentRaw -split "\r?\n"

    $hasFront = $false
    $frontLines = @()
    $restLines = $allLines
    if ($allLines[0] -eq '---') {
        # Find closing delimiter (the next '---')
        $closeIdx = 1
        while ($closeIdx -lt $allLines.Count -and $allLines[$closeIdx] -ne '---') {
            $closeIdx++
        }
        if ($closeIdx -lt $allLines.Count) {
            $hasFront = $true
            $frontLines = $allLines[1..($closeIdx-1)]
            $restLines = $allLines[($closeIdx+1)..($allLines.Count-1)]
        }
    }

    # Extract existing front keys
    $existingLines = @()  # lines that are not title/nav_order, preserve order
    $hasTitle = $false
    $existingTitle = ''
    foreach ($line in $frontLines) {
        if ($line -match '^\s*title\s*:\s*(.*)$') {
            $hasTitle = $true
            $existingTitle = $matches[1].Trim()
            # skip adding this line; we'll re-add controlled
        } elseif ($line -match '^\s*nav_order\s*:') {
            # skip existing nav_order
        } else {
            $existingLines += $line
        }
    }

    # Determine final title
    if ($hasTitle) {
        $title = $existingTitle
    } else {
        $title = Extract-FirstHeading $restLines
        if (-not $title) {
            $title = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
            Write-Warning "No heading found, using filename: $title"
        }
    }

    # Determine nav_order based on relative path within collection folder
    # Compute relative path after _en/ or _fr/
    $fullPath = $file.FullName
    if ($fullPath -match '[_]{1}(en|fr)[/\\](.*)') {
        $inner = $matches[2]
        $navOrder = Get-NavOrder $inner
    } else {
        # Root files: README.md etc.
        if ($file.Name -eq 'README.md') {
            $navOrder = 1
        } else {
            $navOrder = 999
        }
    }

    # Build new front
    $newFront = @()
    $newFront += '---'
    $newFront += "title: $title"
    $newFront += "nav_order: $navOrder"
    $newFront += $existingLines
    $newFront += '---'

    # Combine
    $newContentLines = @()
    $newContentLines += $newFront
    $newContentLines += $restLines
    $newContent = ($newContentLines -join "`r`n")

    Set-Content $file.FullName -Value $newContent -NoNewline
}

Write-Host "`nFront matter updated successfully."
