# Sea Ice Concentration (SIC) Downloader

Downloads **Arctic Sea Ice Concentration** GeoTIFF files from the University of Bremen.

| Property | Value |
|---|---|
| Source | <https://seaice.uni-bremen.de/data/> |
| Dataset | ASI algorithm, daily gridded swath (`asi_daygrid_swath`) |
| Hemisphere | Northern (Arctic) |
| Resolution | **6.25 km** (`n6250`) |
| Region folder | `Arctic` |
| File format | GeoTIFF (`.tif`) |

## Sensors and temporal coverage

| Sensor | Folder | Years |
|---|---|---|
| AMSR-E | `amsre` | 2002 – 2011 |
| AMSR2 | `amsr2` | 2012 – present |

> **Note:** There is a short gap between the end of AMSR-E (October 2011) and the start of AMSR2 (July 2012). No data are available for that period at this resolution.

## Remote directory structure

```
https://seaice.uni-bremen.de/data/
└── {sensor}/                        # amsre | amsr2
    └── asi_daygrid_swath/
        └── n6250/
            └── {year}/              # e.g. 2010
                └── {month}/         # jan feb … dec
                    └── Arctic/
                        └── *.tif
```

## Installation

```bash
pip install -r requirements.txt
```

Python ≥ 3.10 is required (uses built-in `match` / union-type hints).

## Usage

### Download everything (all sensors, all years, all months)

```bash
python download_sic.py
```

Files are saved under `data/` mirroring the remote structure:

```
data/
├── amsre/asi_daygrid_swath/n6250/{year}/{month}/*.tif
└── amsr2/asi_daygrid_swath/n6250/{year}/{month}/*.tif
```

### Common options

| Flag | Description |
|---|---|
| `-o / --output-dir DIR` | Root directory for downloaded files (default: `data/`) |
| `--sensors amsre amsr2` | Limit to specific sensor(s) |
| `--years 2010 2011` | Limit to specific year(s) |
| `--months jan feb dec` | Limit to specific month(s) |
| `-j / --workers N` | Parallel download threads (default: 4) |
| `--update` | **Daily update mode** — only scan the last 2 months of AMSR2 |
| `--lookback N` | Like `--update` but scan the last N months instead of 2 |
| `--skip-old` | Skip `_old.tif` files (older algorithm version, ~10x larger) |
| `--dry-run` | Print what would be downloaded without downloading |
| `-v / --verbose` | Enable debug logging |

### Daily update (recommended for cron / scheduled runs)

The AMSR-E archive (2002–2011) is **permanently frozen** — those files never change.
Only the current AMSR2 month (and sometimes the previous month for late uploads) can have new files.

Use `--update` so the script only checks the last 2 months instead of scanning all ~300 directory listings:

```bash
# Efficient daily run — ~2 HTTP listing requests instead of ~300
python download_sic.py --update -o /mnt/storage/sea_ice
```

Schedule it with cron (runs at 06:00 every day):
```cron
0 6 * * * /usr/bin/python3 /path/to/download_sic.py --update -o /mnt/storage/sea_ice
```

If data from more than 2 months ago is sometimes uploaded late, increase the lookback:
```bash
python download_sic.py --lookback 4
```

### Examples

```bash
# Initial bulk download (all sensors, all years)
python download_sic.py

# Single sensor, single year
python download_sic.py --sensors amsr2 --years 2023

# Multiple years, specific months
python download_sic.py --years 2015 2016 2017 --months mar sep

# Preview what would be downloaded for AMSR-E 2005
python download_sic.py --sensors amsre --years 2005 --dry-run

# Use 8 threads and save to a custom directory
python download_sic.py -j 8 -o /mnt/storage/sea_ice
```

### Equivalent wget commands (for reference)

Single month:
```bash
wget -A .tif -r -nc -np -nH \
  "https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/n6250/2023/jun/Arctic/"
```

All months in a year range:
```bash
for y in {2012..2023}; do
  for m in jan feb mar apr may jun jul aug sep oct nov dec; do
    wget -A .tif -r -nc -nd -np -nH -nv \
      "seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/n6250/$y/$m/Arctic/"
  done
done
```

## Output and resuming

- Files that already exist are **skipped automatically** (no re-download).
- Incomplete downloads (e.g. interrupted) are written to a `.part` file and cleaned up on failure, so partial files never pollute your dataset.
- Re-running the script safely resumes where it left off.

## File naming convention

Bremen serves two versions of each daily file:

| File | Size | Description |
|---|---|---|
| `asi-AMSR2-n6250-{YYYYMMDD}-v5.4.tif` | ~200 KB | Current algorithm version |
| `asi-AMSR2-n6250-{YYYYMMDD}-v5.4_old.tif` | ~2 MB | Previous algorithm version |

Both are downloaded by default. Use `--skip-old` to download only the current algorithm version and save ~10x disk space.

- `YYYYMMDD` — date of the observation
- `v5.4` — algorithm version (may vary by year)

> **Note:** The server uses a self-signed SSL certificate. The script disables SSL verification (`verify=False`) and suppresses the related warning automatically.

## Citation

If you use this data in a publication, please cite:

> Spreen, G., L. Kaleschke, and G. Heygster (2008), Sea ice remote sensing using AMSR-E 89-GHz channels, *J. Geophys. Res.*, 103, C2, doi:10.1029/2005JC003384.

And acknowledge the data source:

> University of Bremen, Institute of Environmental Physics — <https://seaice.uni-bremen.de>
