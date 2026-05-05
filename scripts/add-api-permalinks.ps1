# Add explicit permalinks to API files in _en/api and _fr/api
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

# English API files
$enApi = @('core', 'decorators', 'execution', 'tracking', 'websocket')
foreach ($name in $enApi) {
    $filePath = "$base/_en/api/$name.md"
    if (Test-Path $filePath) {
        $content = Get-Content $filePath -Raw
        if ($content -notmatch 'permalink:') {
            $lines = $content -split "`r?`n"
            $newLines = @()
            $frontDone = $false
            foreach ($line in $lines) {
                $newLines += $line
                if ($line -eq '---' -and -not $frontDone) {
                    $newLines += "permalink: /en/api/$name/"
                    $frontDone = $true
                }
            }
            $newContent = ($newLines -join "`r`n")
            Set-Content $filePath -Value $newContent -NoNewline
            Write-Host "Added permalink to $filePath"
        }
    }
}

# French API files
$frApi = @('core', 'decorators', 'execution', 'tracking', 'websocket')
foreach ($name in $frApi) {
    $filePath = "$base/_fr/api/$name.md"
    if (Test-Path $filePath) {
        $content = Get-Content $filePath -Raw
        if ($content -notmatch 'permalink:') {
            $lines = $content -split "`r?`n"
            $newLines = @()
            $frontDone = $false
            foreach ($line in $lines) {
                $newLines += $line
                if ($line -eq '---' -and -not $frontDone) {
                    $newLines += "permalink: /fr/api/$name/"
                    $frontDone = $true
                }
            }
            $newContent = ($newLines -join "`r`n")
            Set-Content $filePath -Value $newContent -NoNewline
            Write-Host "Added permalink to $filePath"
        }
    }
}

Write-Host "`nAPI permalinks added."
