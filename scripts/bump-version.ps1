# Bump version from 0.3.2 to 0.4.0 in all documentation files
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_config\.yml$'
}

foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    $original = $content

    # Replace 0.3.2 with 0.4.0
    $new = $content -replace '0\.3\.2', '0.4.0'

    if ($new -ne $original) {
        Set-Content $file.FullName -Value $new -NoNewline
        Write-Host "Updated: $($file.FullName)"
    }
}

# Also update _config.yml footer
$configPath = "$base\_config.yml"
$config = Get-Content $configPath -Raw
$config = $config -replace 'Documentation version: 0\.3\.2', 'Documentation version: 0.4.0'
Set-Content $configPath -Value $config -NoNewline
Write-Host "Updated: _config.yml"

Write-Host "`nVersion bump complete: 0.3.2 → 0.4.0"
