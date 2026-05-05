# Fix Jekyll pretty permalinks: remove .md extension and ensure trailing slash in relative_url links
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_config\.yml$' -and
    $_.FullName -notmatch '\\Gemfile$'
}

foreach ($file in $files) {
    Write-Host "Processing: $($file.FullName)"
    $content = Get-Content $file.FullName -Raw
    $original = $content

    # Regex to match: {{ '.../*.md' | relative_url }}  (with single quotes)
    # Capture the path without .md
    $pattern = '(?<pre>\{\{\s*''(?<path>(?:/?(?:en|fr)(?:/[\w\-]+)*)/?)(?:[^'']*?)\.md''\s*\|\s*relative_url\s*\}\})'

    # Actually, simpler: match any occurrence of '.md' inside the quoted string before the pipe
    # We'll use a regex that captures the whole thing and replaces just the .md part
    $new = [regex]::Replace($content, '\{\{\s*''([^'']*?)\.md''\s*\|\s*relative_url\s*\}\}', {
        param($m)
        $fullPath = $m.Groups[1].Value  # e.g. /en/guides/websocket
        # Ensure leading slash
        if (-not $fullPath.StartsWith('/')) {
            $fullPath = '/' + $fullPath
        }
        # Ensure trailing slash (unless already present)
        if ($fullPath.EndsWith('/')) {
            $url = $fullPath
        } else {
            $url = $fullPath + '/'
        }
        return "{{ '$url' | relative_url }}"
    })

    if ($new -ne $original) {
        Set-Content $file.FullName -Value $new -NoNewline
        Write-Host "  -> fixed"
    }
}

Write-Host "`nAll relative_url links updated to pretty permalinks."
