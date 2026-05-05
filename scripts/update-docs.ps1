# Script to update documentation markdown files for Jekyll/GitHub Pages compatibility
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

# Get all markdown files recursively, excluding specific directories and files
$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_layouts\\' -and
    $_.FullName -notmatch '\\archive\\' -and
    $_.Name -ne '_config.yml'
}

foreach ($file in $files) {
    Write-Host "Processing: $($file.FullName)"
    $content = Get-Content $file.FullName -Raw

    # Add front matter if not present
    if (-not $content.StartsWith('---')) {
        $front = "---`nlayout: page`n---`n`n"
        $content = $front + $content
    }

    # Replace absolute /docs/en/ and /docs/fr/ links with Jekyll relative_url
    # Pattern: ](/docs/en/path) -> ]({{ 'en/path' | relative_url }})
    $content = $content -replace '\]\(/docs/(en|fr)/([^)]+)\)', ']({{ ''$1/$2'' | relative_url }})'

    # Replace ../ links (pointing to repo root) with absolute GitHub URLs
    # Pattern: ](../file) -> ](https://github.com/SoniqueBay/taskiq-flow/blob/main/file)
    $content = $content -replace '\]\(\.\./([^)]+)\)', '](https://github.com/SoniqueBay/taskiq-flow/blob/main/$1)'

    # Additionally, convert any remaining plain root-relative paths starting with /en/ or /fr/
    # that are not yet wrapped in {{ }} (these were originally /docs/... but might have been missed? Actually we already handled /docs/,
    # but we also have in index.md links like /en/quickstart.md that need conversion)
    $content = $content -replace '\]\(/(en|fr)/([^)]+)\)', ']({{ ''/$1/$2'' | relative_url }})'

    Set-Content $file.FullName -Value $content -NoNewline
}

Write-Host "`nAll files updated successfully."
