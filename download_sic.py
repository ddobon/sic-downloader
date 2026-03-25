#!/usr/bin/env python3
"""
Download Arctic Sea Ice Concentration (SIC) data.

Sources and coverage:
  1979–1987  NSIDC-0051  Nimbus-7 SMMR          25 km  .bin  (NASA Earthdata login required)
  1987–2002  NSIDC-0051  DMSP SSM/I (F8/F11/F13) 25 km  .bin  (NASA Earthdata login required)
  2002–2011  U. Bremen   AMSR-E  ASI algorithm   6.25 km .tif
  2012–now   U. Bremen   AMSR2   ASI algorithm   6.25 km .tif

NSIDC-0051 dataset:
  https://nsidc.org/data/nsidc-0051
  Requires a free NASA Earthdata account: https://urs.earthdata.nasa.gov/
  Credentials can be supplied via --earthdata-user/--earthdata-pass,
  the EARTHDATA_USER/EARTHDATA_PASS environment variables, or a ~/.netrc
  entry for machine urs.earthdata.nasa.gov.

Bremen dataset:
  https://data.seaice.uni-bremen.de/
  No authentication required.
"""

import argparse
import calendar
import logging
import netrc as netrc_module
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Constants — Bremen (AMSR-E / AMSR2)
# ---------------------------------------------------------------------------

BREMEN_BASE = "https://data.seaice.uni-bremen.de/"
DATASET = "asi_daygrid_swath"
RESOLUTION = "n6250"
REGION = "Arctic"

BREMEN_SENSORS = {
    "amsre": range(2002, 2012),                          # 2002–2011
    "amsr2": range(2012, date.today().year + 1),         # 2012–current
}

# ---------------------------------------------------------------------------
# Constants — NSIDC-0051 (SMMR / SSM/I)
# ---------------------------------------------------------------------------

NSIDC_BASE = "https://n5eil01u.ecs.nsidc.org/NSIDC/NSIDC-0051.002/"

# Maps (inclusive start, inclusive end) → (platform-id, version-string).
# Platform transitions inside a sensor type are handled transparently via
# directory listing — we don't need to guess the filename.
NSIDC_SENSORS = {
    "smmr": range(1979, 1988),   # Nimbus-7 SMMR, every other day
    "ssmi": range(1987, 2003),   # DMSP SSM/I F8→F11→F13, daily
}

# Combined sensor dict used for CLI validation.
SENSORS = {**NSIDC_SENSORS, **BREMEN_SENSORS}

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

MONTHS = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]
MONTH_NAME_TO_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}

DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_WORKERS = 4
DEFAULT_LOOKBACK = 2  # months to scan in --update mode
REQUEST_TIMEOUT = 30  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5       # seconds between retries

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
# Session helpers
# ---------------------------------------------------------------------------

def make_bremen_session() -> requests.Session:
    """Return a session for the Bremen server (SSL verification disabled)."""
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SIC-Downloader/1.0; "
            "+https://github.com/your-org/sic-downloader)"
        )
    })
    return session


def get_earthdata_credentials(
    user: str | None = None,
    password: str | None = None,
) -> tuple[str, str] | None:
    """
    Resolve NASA Earthdata credentials in priority order:
      1. Explicit user/password arguments
      2. EARTHDATA_USER / EARTHDATA_PASS environment variables
      3. ~/.netrc entry for urs.earthdata.nasa.gov
    Returns (user, password) or None if no credentials are found.
    """
    if user and password:
        return user, password

    env_user = os.environ.get("EARTHDATA_USER")
    env_pass = os.environ.get("EARTHDATA_PASS")
    if env_user and env_pass:
        return env_user, env_pass

    try:
        n = netrc_module.netrc()
        auth = n.authenticators("urs.earthdata.nasa.gov")
        if auth:
            return auth[0], auth[2]  # login, password (index 2; index 1 is account)
    except Exception:
        pass

    return None


def make_nsidc_session(
    user: str | None = None,
    password: str | None = None,
) -> requests.Session:
    """
    Return a requests Session configured for NASA Earthdata authentication.

    The session uses HTTP Basic Auth against urs.earthdata.nasa.gov; the
    NSIDC ECS server redirects unauthenticated requests to URS automatically.
    The session follows redirects so authentication is transparent.

    Raises RuntimeError if no credentials are available.
    """
    creds = get_earthdata_credentials(user, password)
    if creds is None:
        raise RuntimeError(
            "NASA Earthdata credentials are required to download SMMR/SSM/I data "
            "(1979–2002). Provide them via:\n"
            "  --earthdata-user / --earthdata-pass   CLI arguments\n"
            "  EARTHDATA_USER / EARTHDATA_PASS       environment variables\n"
            "  ~/.netrc  machine urs.earthdata.nasa.gov  login USER  password PASS\n"
            "Register for a free account at https://urs.earthdata.nasa.gov/"
        )
    session = requests.Session()
    session.auth = (creds[0], creds[1])
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SIC-Downloader/1.0; "
            "+https://github.com/your-org/sic-downloader)"
        )
    })
    return session


# ---------------------------------------------------------------------------
# Generic download helper (shared by both sources)
# ---------------------------------------------------------------------------

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
# Bremen — directory listing and task collection
# ---------------------------------------------------------------------------

def _list_links(
    session: requests.Session,
    url: str,
    extension: str,
    skip_old: bool = False,
) -> list[str]:
    """
    Fetch a directory listing page and return absolute URLs of files with
    the given *extension* (e.g. '.tif' or '.bin').
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
        if href.lower().endswith(extension):
            if skip_old and href.lower().endswith("_old.tif"):
                continue
            links.append(urljoin(url, href))
    return links


def _bremen_url(sensor: str, year: int, month: str) -> str:
    return f"{BREMEN_BASE}{sensor}/{DATASET}/{RESOLUTION}/{year}/{month}/{REGION}/"


def _bremen_dest(output_dir: Path, sensor: str, year: int, month: str, filename: str) -> Path:
    return output_dir / sensor / DATASET / RESOLUTION / str(year) / month / filename


def _fetch_bremen_month(
    args: tuple[requests.Session, str, int, str, str, Path, bool]
) -> list[tuple[str, Path]]:
    session, sensor, year, month, dir_url, output_dir, skip_old = args
    tif_urls = _list_links(session, dir_url, ".tif", skip_old=skip_old)
    if not tif_urls:
        log.debug("No .tif files: %s", dir_url)
        return []
    tasks = []
    for tif_url in tif_urls:
        filename = Path(urlparse(tif_url).path).name
        dest = _bremen_dest(output_dir, sensor, year, month, filename)
        tasks.append((tif_url, dest))
    log.info("Found %3d file(s) — sensor=%s  year=%d  month=%s", len(tif_urls), sensor, year, month)
    return tasks


def recent_year_months(lookback: int) -> list[tuple[str, int, str]]:
    """Return (sensor, year, month) tuples for the last *lookback* calendar months of amsr2."""
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


def collect_bremen_tasks(
    session: requests.Session,
    output_dir: Path,
    sensors: list[str],
    years: list[int] | None,
    months: list[str],
    workers: int,
    lookback: int | None,
    skip_old: bool = False,
) -> list[tuple[str, Path]]:
    """Collect (url, dest) download tasks for Bremen (AMSR-E / AMSR2) sensors."""
    if lookback is not None:
        slots = recent_year_months(lookback)
    else:
        slots = []
        for sensor, sensor_years in BREMEN_SENSORS.items():
            if sensor not in sensors:
                continue
            for year in sensor_years:
                if years and year not in years:
                    continue
                for month in months:
                    slots.append((sensor, year, month))

    fetch_args = [
        (session, sensor, year, month, _bremen_url(sensor, year, month), output_dir, skip_old)
        for sensor, year, month in slots
    ]

    tasks: list[tuple[str, Path]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_fetch_bremen_month, fetch_args):
            tasks.extend(result)
    return tasks


# ---------------------------------------------------------------------------
# NSIDC-0051 — directory listing and task collection
# ---------------------------------------------------------------------------

def _nsidc_day_url(year: int, month: int, day: int) -> str:
    """Return the NSIDC-0051 directory URL for a specific date."""
    return f"{NSIDC_BASE}{year}.{month:02d}.{day:02d}/"


def _nsidc_dest(output_dir: Path, sensor: str, year: int, month: int, filename: str) -> Path:
    """Local path mirroring the NSIDC sensor/year/month structure."""
    month_name = MONTHS[month - 1]
    return output_dir / sensor / "nsidc-0051" / "n25000" / str(year) / month_name / filename


def _fetch_nsidc_day(
    args: tuple[requests.Session, str, int, int, int, Path]
) -> list[tuple[str, Path]]:
    """Worker: list one NSIDC daily directory and return (url, dest) pairs for .bin files."""
    session, sensor, year, month, day, output_dir = args
    day_url = _nsidc_day_url(year, month, day)
    bin_urls = _list_links(session, day_url, ".bin")
    if not bin_urls:
        log.debug("No .bin files: %s", day_url)
        return []
    tasks = []
    for bin_url in bin_urls:
        filename = Path(urlparse(bin_url).path).name
        dest = _nsidc_dest(output_dir, sensor, year, month, filename)
        tasks.append((bin_url, dest))
    return tasks


def collect_nsidc_tasks(
    session: requests.Session,
    output_dir: Path,
    sensors: list[str],
    years: list[int] | None,
    months: list[str],
    workers: int,
) -> list[tuple[str, Path]]:
    """
    Collect (url, dest) download tasks for NSIDC-0051 (SMMR / SSM/I) sensors.

    Each calendar day gets one directory-listing request. These are issued in
    parallel with *workers* threads, so even scanning multiple years is fast.
    """
    month_nums = [MONTH_NAME_TO_NUM[m] for m in months]

    # Build list of (sensor, year, month, day) to check.
    day_slots: list[tuple[str, int, int, int]] = []
    for sensor, sensor_years in NSIDC_SENSORS.items():
        if sensor not in sensors:
            continue
        for year in sensor_years:
            if years and year not in years:
                continue
            for month in month_nums:
                _, days_in_month = calendar.monthrange(year, month)
                for day in range(1, days_in_month + 1):
                    day_slots.append((sensor, year, month, day))

    if not day_slots:
        return []

    log.info("NSIDC: checking %d day-directories (parallel=%d) …", len(day_slots), workers)

    fetch_args = [
        (session, sensor, year, month, day, output_dir)
        for sensor, year, month, day in day_slots
    ]

    tasks: list[tuple[str, Path]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_fetch_nsidc_day, fetch_args):
            tasks.extend(result)

    log.info("NSIDC: found %d file(s) across %d day-directories.", len(tasks), len(day_slots))
    return tasks


# ---------------------------------------------------------------------------
# Unified task collection
# ---------------------------------------------------------------------------

def collect_download_tasks(
    bremen_session: requests.Session,
    nsidc_session: requests.Session | None,
    output_dir: Path,
    sensors: list[str],
    years: list[int] | None,
    months: list[str],
    workers: int,
    lookback: int | None,
    skip_old: bool = False,
) -> list[tuple[str, Path]]:
    """
    Collect all (url, dest) download tasks from both Bremen and NSIDC sources.

    When *lookback* is set, only the last N months of amsr2 are scanned
    (skips all NSIDC sensors).
    """
    tasks: list[tuple[str, Path]] = []

    # --- Bremen sensors ---
    bremen_requested = [s for s in sensors if s in BREMEN_SENSORS]
    if lookback is not None:
        log.info("Update mode: scanning last %d month(s) of amsr2.", lookback)
        tasks += collect_bremen_tasks(
            bremen_session, output_dir, ["amsr2"], years, months, workers, lookback, skip_old
        )
    elif bremen_requested:
        tasks += collect_bremen_tasks(
            bremen_session, output_dir, bremen_requested, years, months, workers, None, skip_old
        )

    # --- NSIDC sensors (skipped in --update/--lookback mode) ---
    if lookback is None:
        nsidc_requested = [s for s in sensors if s in NSIDC_SENSORS]
        if nsidc_requested:
            if nsidc_session is None:
                log.error(
                    "NSIDC sensors requested (%s) but no Earthdata credentials provided. "
                    "Use --earthdata-user / --earthdata-pass or set EARTHDATA_USER / "
                    "EARTHDATA_PASS environment variables.",
                    ", ".join(nsidc_requested),
                )
            else:
                tasks += collect_nsidc_tasks(
                    nsidc_session, output_dir, nsidc_requested, years, months, workers
                )

    return tasks


# ---------------------------------------------------------------------------
# Download runner
# ---------------------------------------------------------------------------

def run_downloads(
    sessions: dict[str, requests.Session],
    tasks: list[tuple[str, Path]],
    workers: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    """
    Download all tasks using a thread pool.

    *sessions* maps URL prefix → session so the correct auth is used per file.
    Returns (total, downloaded, skipped) counts.
    """
    total = len(tasks)

    if dry_run:
        log.info("Dry-run: %d file(s) would be processed.", total)
        for url, dest in tasks:
            exists = " [exists]" if dest.exists() else ""
            log.info("  %s -> %s%s", url, dest, exists)
        return total, 0, 0

    def _pick_session(url: str) -> requests.Session:
        if url.startswith(NSIDC_BASE):
            return sessions.get("nsidc", sessions["bremen"])
        return sessions["bremen"]

    downloaded = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(download_file, _pick_session(url), url, dest): dest
            for url, dest in tasks
        }
        for future in as_completed(futures):
            dest = futures[future]
            ok = future.result()
            if ok and dest.exists():
                downloaded += 1

    skipped = sum(1 for _, dest in tasks if dest.exists()) - downloaded
    return total, downloaded, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Arctic Sea Ice Concentration data.\n\n"
            "Coverage:\n"
            "  1979–1987  NSIDC-0051  Nimbus-7 SMMR   25 km .bin  (sensor: smmr)\n"
            "  1987–2002  NSIDC-0051  DMSP SSM/I       25 km .bin  (sensor: ssmi)\n"
            "  2002–2011  U. Bremen   AMSR-E  ASI      6.25 km .tif (sensor: amsre)\n"
            "  2012–now   U. Bremen   AMSR2   ASI      6.25 km .tif (sensor: amsr2)\n\n"
            "NSIDC data requires a free NASA Earthdata account:\n"
            "  https://urs.earthdata.nasa.gov/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Root directory for downloaded files. (default: %(default)s)",
    )
    parser.add_argument(
        "--sensors",
        nargs="+",
        choices=list(SENSORS),
        default=list(SENSORS),
        metavar="SENSOR",
        help=(
            "Sensors to download. Choices: smmr ssmi amsre amsr2. "
            "(default: all four)"
        ),
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
        help="Month(s) to download (e.g. jan feb dec). (default: all)",
    )
    parser.add_argument(
        "-j", "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of parallel download threads. (default: %(default)s)",
    )

    # --- update / incremental mode ---
    update_group = parser.add_argument_group(
        "incremental update (recommended for daily runs, amsr2 only)"
    )
    update_group.add_argument(
        "--update",
        action="store_true",
        help=(
            f"Only scan the last {DEFAULT_LOOKBACK} months of amsr2 for new files. "
            "Ignores --sensors / --years / --months."
        ),
    )
    update_group.add_argument(
        "--lookback",
        type=int,
        default=None,
        metavar="N",
        help=f"Like --update but scan the last N months. Implies amsr2 only.",
    )

    # --- Bremen options ---
    bremen_group = parser.add_argument_group("Bremen options (amsre / amsr2)")
    bremen_group.add_argument(
        "--skip-old",
        action="store_true",
        help=(
            "Skip files ending in '_old.tif' (older algorithm version, ~10x larger). "
            "By default both the current and old algorithm versions are downloaded."
        ),
    )

    # --- NSIDC / Earthdata authentication ---
    nsidc_group = parser.add_argument_group(
        "NASA Earthdata authentication (required for smmr / ssmi sensors)"
    )
    nsidc_group.add_argument(
        "--earthdata-user",
        default=None,
        metavar="USER",
        help="NASA Earthdata username. Overrides EARTHDATA_USER env var and ~/.netrc.",
    )
    nsidc_group.add_argument(
        "--earthdata-pass",
        default=None,
        metavar="PASS",
        help="NASA Earthdata password. Overrides EARTHDATA_PASS env var and ~/.netrc.",
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

    # Resolve lookback.
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

    # Build sessions.
    bremen_session = make_bremen_session()
    nsidc_session: requests.Session | None = None

    nsidc_needed = (
        lookback is None
        and any(s in NSIDC_SENSORS for s in args.sensors)
    )
    if nsidc_needed:
        try:
            nsidc_session = make_nsidc_session(args.earthdata_user, args.earthdata_pass)
            log.info("Earthdata  : credentials found")
        except RuntimeError as exc:
            log.error("%s", exc)
            return 1

    log.info("Scanning remote directory listings …")
    tasks = collect_download_tasks(
        bremen_session,
        nsidc_session,
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
    sessions = {"bremen": bremen_session}
    if nsidc_session is not None:
        sessions["nsidc"] = nsidc_session

    total, downloaded, skipped = run_downloads(sessions, tasks, args.workers, args.dry_run)

    log.info("----------------------------------------")
    log.info("Done. total=%d  downloaded=%d  skipped=%d", total, downloaded, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
