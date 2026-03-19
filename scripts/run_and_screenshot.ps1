$ErrorActionPreference = "Stop"

cargo run --release -- --data-dir data/2025 --year 2025 | Tee-Object -FilePath output.txt

python -m pip install pillow
python scripts/make_screenshot.py output.txt screenshots/output.png

Write-Host "Saved screenshot to screenshots/output.png"