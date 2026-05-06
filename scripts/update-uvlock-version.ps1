# Update taskiq-flow version in uv.lock to match pyproject.toml
$uvlock = "C:\Users\david\Documents\devs\taskiq-flow\uv.lock"
if (Test-Path $uvlock) {
    $content = Get-Content $uvlock -Raw
    # Replace version for taskiq-flow package
    $new = [regex]::Replace($content, '(name\s*=\s*"taskiq-flow".*?version\s*=\s*")([^"]+)(")', { param($m) $m.Groups[1].Value + "0.4.0" + $m.Groups[3].Value }, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    if ($new -ne $content) {
        Set-Content $uvlock $new -NoNewline
        Write-Host "✅ Updated uv.lock to version 0.4.0"
    } else {
        Write-Warning "Could not find taskiq-flow entry in uv.lock to update"
    }
} else {
    Write-Warning "uv.lock not found"
}
