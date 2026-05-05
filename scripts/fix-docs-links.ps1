# Second pass: fix leading slash for Jekyll relative_url, handle remaining /docs/ and /en/ links
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_layouts\\' -and
    $_.FullName -notmatch '\\archive\\' -and
    $_.Name -ne '_config.yml'
}

foreach ($file in $files) {
    Write-Host "Fixing: $($file.FullName)"
    $content = Get-Content $file.FullName -Raw

    # Convert any remaining raw /docs/(en|fr)... links to Jekyll relative_url (with leading slash)
    $content = $content -replace '\]\(/docs/(en|fr)([^)]*)\)', ']({{ ''/$1$2'' | relative_url }})'

    # Convert any remaining root-relative /en/... or /fr/... links (e.g., language switchers) to Jekyll relative_url
    $content = $content -replace '\]\(/(en|fr)([^)]*)\)', ']({{ ''/$1$2'' | relative_url }})'

    # Convert ../ links to external GitHub URLs
    $content = $content -replace '\]\(\.\./([^)]+)\)', '](https://github.com/SoniqueBay/taskiq-flow/blob/main/$1)'

    # Fix missing leading slash in already-converted Liquid tags ({{ 'en/... -> {{ '/en/...)
    # These were produced by first pass incorrectly.
    $content = $content.Replace("{{ 'en/", "{{ '/en/")
    $content = $content.Replace("{{ 'fr/", "{{ '/fr/")

    Set-Content $file.FullName -Value $content -NoNewline
}

Write-Host "`nCorrection pass completed."
