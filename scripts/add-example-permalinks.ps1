# Add explicit permalinks to all example files in _en/examples and _fr/examples
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

# English examples
$enExamples = @(
    'api-example',
    'dataflow-audio-pipeline',
    'index',
    'quickstart',
    'registry-discovery',
    'scheduled-pipeline',
    'tracking-demo',
    'websocket-demo'
)

# French examples
$frExamples = @(
    'api-example',
    'dataflow-audio-pipeline',
    'index',
    'quickstart',
    'registry-discovery',
    'scheduled-pipeline',
    'tracking-demo',
    'websocket-demo'
)

foreach ($name in $enExamples) {
    $filePath = "$base/_en/examples/$name.md"
    if (Test-Path $filePath) {
        $content = Get-Content $filePath -Raw
        # Check if permalink already present
        if ($content -notmatch 'permalink:') {
            # Insert permalink after the opening --- and before title/nav_order lines
            $lines = $content -split "`r?`n"
            $newLines = @()
            $frontDone = $false
            foreach ($line in $lines) {
                $newLines += $line
                if ($line -eq '---' -and -not $frontDone) {
                    # Add permalink line right after the opening delimiter
                    $newLines += "permalink: /en/examples/$name/"
                    $frontDone = $true
                }
            }
            $newContent = ($newLines -join "`r`n")
            Set-Content $filePath -Value $newContent -NoNewline
            Write-Host "Added permalink to $filePath"
        }
    }
}

foreach ($name in $frExamples) {
    $filePath = "$base/_fr/examples/$name.md"
    if (Test-Path $filePath) {
        $content = Get-Content $filePath -Raw
        if ($content -notmatch 'permalink:') {
            $lines = $content -split "`r?`n"
            $newLines = @()
            $frontDone = $false
            foreach ($line in $lines) {
                $newLines += $line
                if ($line -eq '---' -and -not $frontDone) {
                    $newLines += "permalink: /fr/examples/$name/"
                    $frontDone = $true
                }
            }
            $newContent = ($newLines -join "`r`n")
            Set-Content $filePath -Value $newContent -NoNewline
            Write-Host "Added permalink to $filePath"
        }
    }
}

Write-Host "`nPermalinks added to all example files."
