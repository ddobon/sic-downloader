# Sea Ice Concentration (SIC) Downloader

Downloads **Arctic Sea Ice Concentration** data covering **1979 to present** from two sources:

- **University of Bremen** (AMSR-E / AMSR2) — no authentication required
- **NASA NSIDC-0051** (SMMR / SSM/I) — requires a free [NASA Earthdata account](https://urs.earthdata.nasa.gov/)

## Sensors and temporal coverage

| Sensor | Years | Source | Resolution | Format |
|---|---|---|---|---|
| `smmr` | 1979 – 1987 | NSIDC-0051 (Nimbus-7 SMMR) | 25 km | `.bin` |
| `ssmi` | 1987 – 2002 | NSIDC-0051 (DMSP SSM/I F8/F11/F13) | 25 km | `.bin` |
| `amsre` | 2002 – 2011 | U. Bremen (ASI algorithm) | 6.25 km | `.tif` |
| `amsr2` | 2012 – present | U. Bremen (ASI algorithm) | 6.25 km | `.tif` |

> **Note:** SMMR data is available every other day (not daily). There is also a short gap between AMSR-E (ends October 2011) and AMSR2 (starts July 2012).

> **Note:** Pre-2002 (SMMR/SSM/I) data is at 25 km resolution using the NASA Team algorithm, while post-2002 data is at 6.25 km using the ASI algorithm. The file formats also differ (`.bin` vs `.tif`).

## Remote directory structure

**University of Bremen (amsre / amsr2):**
```
https://data.seaice.uni-bremen.de/
└── {sensor}/                        # amsre | amsr2
    └── asi_daygrid_swath/
        └── n6250/
            └── {year}/              # e.g. 2010
                └── {month}/         # jan feb … dec
                    └── Arctic/
                        └── *.tif
```

**NSIDC-0051 (smmr / ssmi):**
```
https://n5eil01u.ecs.nsidc.org/NSIDC/NSIDC-0051.002/
└── {YYYY}.{MM}.{DD}/                # one directory per calendar day
    └── nt_{YYYYMMDD}_{platform}_{version}_n.bin
```

## Installation

```bash
pip install -r requirements.txt
```

Python ≥ 3.10 is required (uses built-in union-type hints).

## Authentication (NSIDC — smmr / ssmi only)

Pre-2002 data requires a free [NASA Earthdata account](https://urs.earthdata.nasa.gov/). Provide credentials in one of three ways (checked in this order):

1. **CLI flags:** `--earthdata-user USER --earthdata-pass PASS`
2. **Environment variables:** `EARTHDATA_USER` and `EARTHDATA_PASS`
3. **~/.netrc file:**
   ```
   machine urs.earthdata.nasa.gov
       login YOUR_USERNAME
       password YOUR_PASSWORD
   ```

Bremen sensors (`amsre`, `amsr2`) require no authentication.

## Usage

### Download everything (all sensors, all years, all months)

```bash
# Includes pre-2002 NSIDC data — Earthdata credentials required
python download_sic.py --earthdata-user USER --earthdata-pass PASS

# Bremen sensors only (no credentials needed)
python download_sic.py --sensors amsre amsr2
```

Files are saved under `data/` mirroring the remote structure:

```
data/
├── smmr/nsidc-0051/n25000/{year}/{month}/*.bin
├── ssmi/nsidc-0051/n25000/{year}/{month}/*.bin
├── amsre/asi_daygrid_swath/n6250/{year}/{month}/*.tif
└── amsr2/asi_daygrid_swath/n6250/{year}/{month}/*.tif
```

### Common options

| Flag | Description |
|---|---|
| `-o / --output-dir DIR` | Root directory for downloaded files (default: `data/`) |
| `--sensors smmr ssmi amsre amsr2` | Limit to specific sensor(s) |
| `--years 2010 2011` | Limit to specific year(s) |
| `--months jan feb dec` | Limit to specific month(s) |
| `-j / --workers N` | Parallel download threads (default: 4) |
| `--earthdata-user USER` | NASA Earthdata username (smmr/ssmi only) |
| `--earthdata-pass PASS` | NASA Earthdata password (smmr/ssmi only) |
| `--update` | **Daily update mode** — only scan the last 2 months of AMSR2 |
| `--lookback N` | Like `--update` but scan the last N months instead of 2 |
| `--skip-old` | Skip `_old.tif` files (older algorithm version, ~10x larger) |
| `--dry-run` | Print what would be downloaded without downloading |
| `-v / --verbose` | Enable debug logging |

### Daily update (recommended for cron / scheduled runs)

The AMSR-E and NSIDC archives are **permanently frozen** — those files never change.
Only the current AMSR2 month (and sometimes the previous month for late uploads) can have new files.

Use `--update` so the script only checks the last 2 months of AMSR2 instead of scanning all directories (NSIDC sensors are skipped automatically in update mode):

```bash
# Efficient daily run
python download_sic.py --update -o /mnt/storage/sea_ice
```

Schedule it with cron (runs at 06:00 every day):
```cron
0 6 * * * /usr/bin/python3 /path/to/download_sic.py --update -o /mnt/storage/sea_ice
```

### Examples

```bash
# Full historical download (1979–present), credentials via env vars
EARTHDATA_USER=myuser EARTHDATA_PASS=mypass python download_sic.py

# Pre-2002 data only
python download_sic.py --sensors smmr ssmi --earthdata-user USER --earthdata-pass PASS

# SMMR data for a specific year
python download_sic.py --sensors smmr --years 1984 --earthdata-user USER --earthdata-pass PASS

# Bremen sensors only (no auth required)
python download_sic.py --sensors amsre amsr2

# Single Bremen sensor, single year
python download_sic.py --sensors amsr2 --years 2023

# Multiple years, specific months
python download_sic.py --years 2015 2016 2017 --months mar sep

# Preview what would be downloaded for AMSR-E 2005
python download_sic.py --sensors amsre --years 2005 --dry-run

# Use 8 threads and save to a custom directory
python download_sic.py -j 8 -o /mnt/storage/sea_ice
```

### Equivalent wget command for Bremen data (for reference)

Single month:
```bash
wget -A .tif -r -nc -np -nH \
  "https://data.seaice.uni-bremen.de/amsr2/asi_daygrid_swath/n6250/2023/jun/Arctic/"
```

## Output and resuming

- Files that already exist are **skipped automatically** (no re-download).
- Incomplete downloads (e.g. interrupted) are written to a `.part` file and cleaned up on failure, so partial files never pollute your dataset.
- Re-running the script safely resumes where it left off.

## File naming conventions

**Bremen (amsre / amsr2):**

| File | Size | Description |
|---|---|---|
| `asi-AMSR2-n6250-{YYYYMMDD}-v5.4.tif` | ~200 KB | Current algorithm version |
| `asi-AMSR2-n6250-{YYYYMMDD}-v5.4_old.tif` | ~2 MB | Previous algorithm version |

Both are downloaded by default. Use `--skip-old` to download only the current algorithm version and save ~10x disk space.

**NSIDC-0051 (smmr / ssmi):**

| File | Description |
|---|---|
| `nt_{YYYYMMDD}_n07_v0.0_n.bin` | Nimbus-7 SMMR (1979–1987) |
| `nt_{YYYYMMDD}_f08_v1.1_n.bin` | DMSP SSM/I F8 (1987–1991) |
| `nt_{YYYYMMDD}_f11_v1.1_n.bin` | DMSP SSM/I F11 (1991–1995) |
| `nt_{YYYYMMDD}_f13_v1.1_n.bin` | DMSP SSM/I F13 (1995–2007) |

These are flat binary files on a 25 km polar stereographic grid (304×448 Northern Hemisphere). See the [NSIDC-0051 documentation](https://nsidc.org/data/nsidc-0051) for format details.

> **Note:** The Bremen server uses a self-signed SSL certificate. The script disables SSL verification (`verify=False`) and suppresses the related warning automatically.

## Citation

If you use Bremen data (AMSR-E / AMSR2) in a publication, please cite:

> Spreen, G., L. Kaleschke, and G. Heygster (2008), Sea ice remote sensing using AMSR-E 89-GHz channels, *J. Geophys. Res.*, 103, C2, doi:10.1029/2005JC003384.

And acknowledge:

> University of Bremen, Institute of Environmental Physics — <https://seaice.uni-bremen.de>

If you use NSIDC-0051 data (SMMR / SSM/I), please cite:

> Cavalieri, D. J., C. L. Parkinson, P. Gloersen, and H. J. Zwally (1996, updated yearly), Sea Ice Concentrations from Nimbus-7 SMMR and DMSP SSM/I-SSMIS Passive Microwave Data, Version 2. Boulder, Colorado USA. NASA National Snow and Ice Data Center Distributed Active Archive Center. doi:10.5067/MPYG15WAA4WX.
