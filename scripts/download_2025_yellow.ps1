$ErrorActionPreference = "Stop"

$YEAR = 2025
$OUT_DIR = "data\$YEAR"
$BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

New-Item -ItemType Directory -Force -Path $OUT_DIR | Out-Null

1..12 | ForEach-Object {
    $m = $_.ToString("00")
    $file = "yellow_tripdata_$YEAR-$m.parquet"
    $url  = "$BASE_URL/$file"
    $out  = Join-Path $OUT_DIR $file

    if (Test-Path $out) {
        Write-Host "Already exists: $out"
        return
    }

    Write-Host "Downloading $url"

    # curl with retries; if it fails (e.g., 403), warn and move on.
    & curl.exe -L --fail --retry 15 --retry-delay 2 -o $out $url
    if ($LASTEXITCODE -ne 0) {
        Write-Host "WARNING: could not download $file (server returned error). Skipping."
        if (Test-Path $out) { Remove-Item $out -Force }
    }
}

Write-Host "Done. Files are in $OUT_DIR"