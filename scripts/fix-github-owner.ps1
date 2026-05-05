# Replace all SoniqueBay GitHub links with dorel14
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$files = Get-ChildItem -Path $base -Filter *.md -Recurse | Where-Object {
    $_.FullName -notmatch '\\_config\.yml$' -and
    $_.Name -ne 'Gemfile'
}

foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    $original = $content

    # Replace SoniqueBay with dorel14
    $new = $content -replace 'github\.com/SoniqueBay/taskiq-flow', 'github.com/dorel14/taskiq-flow'

    if ($new -ne $original) {
        Set-Content $file.FullName -Value $new -NoNewline
        Write-Host "Fixed: $($file.FullName)"
    }
}

Write-Host "`nAll GitHub owner references updated to dorel14."
