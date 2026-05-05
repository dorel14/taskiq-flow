# Add title (from first heading) and nav_order (based on path) to all markdown files in docs/
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

# Mapping of relative paths (from collection root) to nav_order values for English and French
$navOrderMap = @{
    # English + French (same order)
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
    # Normalize path separators to forward slash
    $rel = $relativePath -replace '\\', '/'
    if ($navOrderMap.ContainsKey($rel)) {
        return $navOrderMap[$rel]
    }
    # Unknown file, assign a high number to appear at end
    return 999
}

function Extract-FirstHeading([string]$content) {
    if ($content -match '^(#+)\s*(.*)$' -ne $null) {
        # Use first heading match (first line starting with #)
        # Multi-line: match from start after any leading blank lines?
        $lines = $content -split "`r?`n"
        foreach ($line in $lines) {
            if ($line -match '^#\s+(.+)$') {
                return $matches[1].Trim()
            }
        }
    }
    return $null
}

# Get all markdown files
$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_layouts\\' -and
    $_.Name -ne '_config.yml' -and
    $_.Name -ne 'Gemfile'
}

foreach ($file in $files) {
    Write-Host "Updating: $($file.FullName)"
    $content = Get-Content $file.FullName -Raw

    # Check for existing front matter
    $hasFront = $content.StartsWith('---')
    $frontContent = ''
    $rest = $content

    if ($hasFront) {
        # Split into front and rest
        $parts = $content -split "`r?`n---\r?\n", 3  # splits at most 3 parts: before first ---, front, rest
        # Actually the pattern: first '---\n', then front, then '\n---\n', then rest.
        # Using split: we expect 3 parts if delimiters are correct: empty before first, front, rest.
        if ($parts.Count -ge 3) {
            $frontContent = $parts[1]
            $rest = $parts[2]
        } else {
            # fallback: maybe single line? We'll treat whole as front? skip
            Write-Warning "Unexpected front format in $($file.FullName)"
            continue
        }
    }

    # Parse existing front keys into ordered list and dictionary
    $existingLines = @()
    $dict = @{}
    if ($hasFront) {
        $frontLines = $frontContent -split "`r?`n"
        foreach ($line in $frontLines) {
            if ($line -match '^\s*([^:]+)\s*:\s*(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                $existingLines += $line
                $dict[$key] = $value
            } else {
                # Could be a multiline or list; preserve as-is (unlikely)
                $existingLines += $line
            }
        }
    }

    # Determine title
    if ($dict.ContainsKey('title')) {
        $title = $dict['title']
    } else {
        $title = Extract-FirstHeading $rest
        if (-not $title) {
            $title = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
            Write-Warning "No heading found in $($file.FullName), using filename for title."
        }
    }

    # Determine nav_order based on relative path within collection
    # Calculate relative path from the nearest collection root (folder starting with _)
    $relPath = $file.FullName
    # find _en/ or _fr/ in path
    $match = [regex]::Match($relPath, '[_]{1}(en|fr)[/\\](.*)', 'IgnoreCase')
    if ($match.Success) {
        $collection = $match.Groups[1].Value
        $innerPath = $match.Groups[2].Value
        # For the root index, innerPath is 'index.md'
        $navOrder = Get-NavOrder $innerPath
    } else {
        # For root-level files (like README.md) not in collection, assign small order if needed
        if ($file.Name -eq 'README.md') {
            $navOrder = 1
        } else {
            $navOrder = 999
        }
    }

    # Update dictionary
    $dict['title'] = $title
    $dict['nav_order'] = $navOrder.ToString()

    # Build new front block lines: preserve original order except for title/nav_order?
    # Simpler: output title and nav_order first, then other keys (except we already have them in existingLines but we filtered duplicates)
    # We'll rebuild front block with existing non-title/non-nav_order lines, plus new title and nav_order at top.
    $filteredLines = @()
    foreach ($line in $existingLines) {
        if ($line -match '^\s*title\s*:') { continue }
        if ($line -match '^\s*nav_order\s*:') { continue }
        $filteredLines += $line
    }

    $newFrontLines = @()
    $newFrontLines += "title: $title"
    $newFrontLines += "nav_order: $navOrder"
    $newFrontLines += $filteredLines

    $newFront = ($newFrontLines -join "`r`n")
    $newContent = "---`r`n$newFront`r`n---`r`n$rest"

    Set-Content $file.FullName -Value $newContent -NoNewline
}

Write-Host "`nFront matter updated with title and nav_order."
