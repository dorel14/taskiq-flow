# Rename en/ -> _en and fr/ -> _fr to use Jekyll collections
$base = 'C:\Users\david\Documents\devs\taskiq-flow\docs'

$en = Join-Path $base 'en'
$fr = Join-Path $base 'fr'
$en_new = Join-Path $base '_en'
$fr_new = Join-Path $base '_fr'

if (Test-Path $en) {
    Rename-Item -Path $en -NewName '_en'
    Write-Host "Renamed en/ -> _en/"
}
if (Test-Path $fr) {
    Rename-Item -Path $fr -NewName '_fr'
    Write-Host "Renamed fr/ -> _fr/"
}

Write-Host "`nCollection directories renamed."
