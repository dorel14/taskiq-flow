# Remove 'layout: page' from front matter of all markdown files in docs/
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_layouts\\' -and
    $_.Name -ne '_config.yml' -and
    $_.Name -ne 'Gemfile'
}

foreach ($file in $files) {
    Write-Host "Processing: $($file.FullName)"
    $content = Get-Content $file.FullName -Raw

    # Use regex to match YAML front block (--- ... ---)
    $pattern = '(?s)^(---\s*\r?\n)(.*?)(\r?\n---\s*\r?\n)'
    if ($content -match $pattern) {
        $frontDelimiterOpen = $matches[1]
        $frontContent = $matches[2]
        $rest = $content.Substring($matches[0].Length)  # content after the closing delimiter

        # Remove lines that are exactly 'layout: page' (with optional whitespace)
        $frontLines = $frontContent -split "`r?`n"
        $keptLines = @()
        foreach ($line in $frontLines) {
            if ($line -notmatch '^\s*layout\s*:\s*page\s*$') {
                $keptLines += $line
            }
        }

        $newFront = ($keptLines -join "`r`n")
        if ($newFront.Trim() -eq '') {
            # No other front matter, skip the front block entirely
            $newContent = $rest
        } else {
            $newContent = "$frontDelimiterOpen$newFront`r`n---`r`n$rest"
        }

        Set-Content $file.FullName -Value $newContent -NoNewline
    }
}

Write-Host "`nAll front matter cleaned."
