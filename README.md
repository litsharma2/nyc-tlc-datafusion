# NYC TLC Yellow Taxi 2025 Aggregations (Rust + DataFusion)

## What this project does

- Loads NYC TLC Yellow Taxi trip data for year 2025 from local Parquet files (all available months)
- Runs required aggregations using **DataFusion DataFrame API**
- Runs the same aggregations using **DataFusion SQL**
- Prints results to the terminal and saves a screenshot to `screenshots/output.png`

## Dataset source

NYC TLC Trip Record Data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data dictionary (Yellow Taxi):
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## How to download the data (NO Parquet committed)

This repository does **not** include Parquet files (automatic penalty if committed).

Run the download script (Windows PowerShell):

```powershell
.\scripts\download_2025_yellow.ps1
```
