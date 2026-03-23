#!/usr/bin/env python3
"""
Download Arctic Sea Ice Concentration (SIC) data from the University of Bremen.

Data source: https://seaice.uni-bremen.de/data/
Dataset: ASI algorithm, daily gridded swath
Resolution: 6.25 km (n6250), Northern Hemisphere
Sensors:
  - AMSR-E  : 2002–2011  (amsre)
  - AMSR2   : 2012–present (amsr2)
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://seaice.uni-bremen.de/data/"
DATASET = "asi_daygrid_swath"
RESOLUTION = "n6250"
REGION = "Arctic"

SENSORS = {
    "amsre": range(2002, 2012),   # 2002 – 2011 inclusive
    "amsr2": range(2012, date.today().year + 1),  # 2012 – current year
}

MONTHS = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]

DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_WORKERS = 4
DEFAULT_LOOKBACK = 2  # months to scan in --update mode
REQUEST_TIMEOUT = 30  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5       # seconds between retries

# AMSR-E ended Oct 2011; its archive is permanently frozen.
AMSRE_END = (2011, 10)  # (year, month-index 1-based)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    """Return a requests Session with a browser-like User-Agent and SSL verification disabled.

    The Bremen server uses a self-signed certificate chain; verification is
    intentionally skipped. InsecureRequestWarning is suppressed globally above.
    """
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SIC-Downloader/1.0; "
            "+https://github.com/your-org/sic-downloader)"
        )
    })
    return session


def list_tif_links(session: requests.Session, url: str, skip_old: bool = False) -> list[str]:
    """
    Fetch a directory listing page and return absolute URLs of .tif files.
    Returns an empty list if the page cannot be fetched or contains no .tif links.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS:
                log.warning("Could not fetch listing %s — %s", url, exc)
                return []
            time.sleep(RETRY_DELAY)

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        if href.lower().endswith(".tif"):
            if skip_old and href.lower().endswith("_old.tif"):
                continue
            links.append(urljoin(url, href))
    return links


def download_file(session: requests.Session, url: str, dest: Path) -> bool:
    """
    Download *url* to *dest*, skipping if the file already exists.
    Returns True on success, False on failure.
    """
    if dest.exists():
        log.debug("Skip (exists): %s", dest.name)
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                with tmp.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        fh.write(chunk)
            tmp.rename(dest)
            log.info("Downloaded: %s", dest.name)
            return True
        except requests.RequestException as exc:
            if tmp.exists():
                tmp.unlink()
            if attempt == RETRY_ATTEMPTS:
                log.error("Failed to download %s — %s", url, exc)
                return False
            log.warning("Retry %d/%d for %s", attempt, RETRY_ATTEMPTS, url)
            time.sleep(RETRY_DELAY)

    return False


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_url(sensor: str, year: int, month: str) -> str:
    """Construct the directory URL for a given sensor / year / month."""
    return (
        f"{BASE_URL}{sensor}/{DATASET}/{RESOLUTION}/{year}/{month}/{REGION}/"
    )


def build_dest(output_dir: Path, sensor: str, year: int, month: str, filename: str) -> Path:
    """Mirror the remote path structure under *output_dir*."""
    return output_dir / sensor / DATASET / RESOLUTION / str(year) / month / filename


def recent_year_months(lookback: int) -> list[tuple[str, int, str]]:
    """
    Return (sensor, year, month) tuples for the last *lookback* calendar months.
    Only amsr2 can have new data; amsre archive is frozen.
    """
    today = date.today()
    combos: list[tuple[str, int, str]] = []
    year, month_idx = today.year, today.month
    for _ in range(lookback):
        combos.append(("amsr2", year, MONTHS[month_idx - 1]))
        month_idx -= 1
        if month_idx == 0:
            month_idx = 12
            year -= 1
    return combos


def _fetch_one(
    args: tuple[requests.Session, str, int, str, str, Path, bool]
) -> list[tuple[str, Path]]:
    """Worker: fetch one directory listing and return (url, dest) task pairs."""
    session, sensor, year, month, dir_url, output_dir, skip_old = args
    tif_urls = list_tif_links(session, dir_url, skip_old=skip_old)
    if not tif_urls:
        log.debug("No .tif files: %s", dir_url)
        return []
    tasks = []
    for tif_url in tif_urls:
        filename = Path(urlparse(tif_url).path).name
        dest = build_dest(output_dir, sensor, year, month, filename)
        tasks.append((tif_url, dest))
    log.info(
        "Found %3d file(s) — sensor=%s  year=%d  month=%s",
        len(tif_urls), sensor, year, month,
    )
    return tasks


def collect_download_tasks(
    session: requests.Session,
    output_dir: Path,
    sensors: list[str],
    years: list[int] | None,
    months: list[str],
    workers: int,
    lookback: int | None,
    skip_old: bool = False,
) -> list[tuple[str, Path]]:
    """
    Walk the relevant sensor/year/month combinations and return (url, dest) pairs
    for every .tif file found. Directory listings are fetched in parallel.

    When *lookback* is set, only the last N calendar months of amsr2 are scanned
    (efficient for daily update runs). Otherwise all specified sensors/years/months
    are scanned (suitable for the initial bulk download).
    """
    # Build the list of (sensor, year, month) slots to scan.
    if lookback is not None:
        slots = recent_year_months(lookback)
        log.info("Update mode: scanning last %d month(s) of amsr2.", lookback)
    else:
        slots = []
        for sensor, sensor_years in SENSORS.items():
            if sensor not in sensors:
                continue
            for year in sensor_years:
                if years and year not in years:
                    continue
                for month in months:
                    slots.append((sensor, year, month))

    # Fetch all listings in parallel using the same thread pool size as downloads.
    fetch_args = [
        (session, sensor, year, month, build_url(sensor, year, month), output_dir, skip_old)
        for sensor, year, month in slots
    ]

    tasks: list[tuple[str, Path]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_fetch_one, fetch_args):
            tasks.extend(result)

    return tasks


def run_downloads(
    session: requests.Session,
    tasks: list[tuple[str, Path]],
    workers: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    """
    Download all tasks using a thread pool.
    Returns (total, downloaded, skipped) counts.
    """
    total = len(tasks)

    if dry_run:
        log.info("Dry-run: %d file(s) would be processed.", total)
        for url, dest in tasks:
            exists = " [exists]" if dest.exists() else ""
            log.info("  %s -> %s%s", url, dest, exists)
        return total, 0, 0

    downloaded = skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(download_file, session, url, dest): dest
            for url, dest in tasks
        }
        for future in as_completed(futures):
            dest = futures[future]
            ok = future.result()
            if ok:
                if dest.exists():
                    downloaded += 1
            else:
                pass  # error already logged inside download_file

    # Anything that existed before we started was skipped.
    skipped = sum(1 for _, dest in tasks if dest.exists()) - downloaded
    return total, downloaded, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Arctic Sea Ice Concentration .tif files from the "
            "University of Bremen (6.25 km, Northern Hemisphere)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Root directory for downloaded files.",
    )
    parser.add_argument(
        "--sensors",
        nargs="+",
        choices=list(SENSORS),
        default=list(SENSORS),
        metavar="SENSOR",
        help="Sensors to download (amsre and/or amsr2).",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        metavar="YEAR",
        help="Specific year(s) to download. Defaults to all available years.",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        choices=MONTHS,
        default=MONTHS,
        metavar="MONTH",
        help="Month(s) to download (e.g. jan feb dec).",
    )
    parser.add_argument(
        "-j", "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of parallel download threads.",
    )
    # --- update / incremental mode ---
    update_group = parser.add_argument_group(
        "incremental update (recommended for daily runs)"
    )
    update_group.add_argument(
        "--update",
        action="store_true",
        help=(
            f"Only scan the last {DEFAULT_LOOKBACK} months of amsr2 for new files. "
            "Ignores --sensors / --years / --months. "
            "Use this flag when running the script daily."
        ),
    )
    update_group.add_argument(
        "--lookback",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Like --update but scan the last N months instead of "
            f"{DEFAULT_LOOKBACK}. Implies amsr2 only."
        ),
    )

    parser.add_argument(
        "--skip-old",
        action="store_true",
        help=(
            "Skip files ending in '_old.tif' (older algorithm version, ~10x larger). "
            "By default both the current and old algorithm versions are downloaded."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be downloaded without actually downloading.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Resolve lookback: --update uses the default, --lookback N overrides it.
    lookback: int | None = None
    if args.update:
        lookback = DEFAULT_LOOKBACK
    elif args.lookback is not None:
        lookback = args.lookback

    log.info("=== Sea Ice Concentration Downloader ===")
    log.info("Output dir : %s", args.output_dir.resolve())
    if lookback is not None:
        log.info("Mode       : update (last %d month(s), amsr2 only)", lookback)
    else:
        log.info("Mode       : full scan")
        log.info("Sensors    : %s", ", ".join(args.sensors))
        log.info("Months     : %s", ", ".join(args.months))
        log.info("Years      : %s", "all" if args.years is None else ", ".join(map(str, args.years)))
    log.info("Workers    : %d", args.workers)
    log.info("Skip _old  : %s", args.skip_old)
    log.info("Dry run    : %s", args.dry_run)
    log.info("----------------------------------------")

    session = make_session()

    log.info("Scanning remote directory listings …")
    tasks = collect_download_tasks(
        session,
        args.output_dir,
        args.sensors,
        args.years,
        args.months,
        args.workers,
        lookback,
        skip_old=args.skip_old,
    )

    if not tasks:
        log.info("No new files found.")
        return 0

    log.info("Total files to process: %d", len(tasks))
    total, downloaded, skipped = run_downloads(
        session, tasks, args.workers, args.dry_run
    )

    log.info("----------------------------------------")
    log.info("Done. total=%d  downloaded=%d  skipped=%d", total, downloaded, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
