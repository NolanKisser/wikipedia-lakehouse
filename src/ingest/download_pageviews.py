"""Download Wikipedia hourly pageview dumps and the wiki site list into a Unity Catalog volume.

Idempotent: hourly files already in the volume are skipped, so the daily job only fetches new
hours. Hours Wikimedia hasn't published yet are logged and skipped.
"""

import argparse
import csv
import io
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta

DUMPS_URL = "https://dumps.wikimedia.org/other/pageviews"
SITEMATRIX_URL = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json&formatversion=2"
# Wikimedia requires a User-Agent that identifies the client and how to contact its operator:
# https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
DEFAULT_USER_AGENT = "wikipedia-lakehouse/0.1 (Databricks portfolio project)"
WIKI_COLUMNS = ["language_code", "language_name", "project", "site_url", "dbname"]
# Hosts of sitematrix "specials" that appear in pageview dumps, mapped to their project.
SPECIAL_DOMAINS = {"wikimedia.org": "wikimedia", "wikidata.org": "wikidata", "mediawiki.org": "mediawiki"}


def expand_hours(spec: str, today: date | None = None) -> list[datetime]:
    """Expand a spec of comma-separated tokens into UTC hours.

    "2025-10-01" -> 24 hours, "2025-10-01:2025-10-03" -> every hour of those days,
    "2025-10-01T13" -> one hour, "last:N" -> the N full days before today.
    """
    today = today or datetime.now(UTC).date()
    hours = []
    for token in filter(None, (t.strip() for t in spec.split(","))):
        if "T" in token:
            hours.append(datetime.strptime(token, "%Y-%m-%dT%H").replace(tzinfo=UTC))
            continue
        if token.startswith("last:"):
            days = int(token.removeprefix("last:"))
            start, end = today - timedelta(days=days), today - timedelta(days=1)
        else:
            first, _, last = token.partition(":")
            start, end = date.fromisoformat(first), date.fromisoformat(last or first)
        day = start
        while day <= end:
            midnight = datetime(day.year, day.month, day.day, tzinfo=UTC)
            hours += [midnight + timedelta(hours=h) for h in range(24)]
            day += timedelta(days=1)
    return hours


def hour_file_name(hour: datetime) -> str:
    return f"pageviews-{hour:%Y%m%d-%H}0000.gz"


def hour_url(hour: datetime) -> str:
    return f"{DUMPS_URL}/{hour:%Y}/{hour:%Y-%m}/{hour_file_name(hour)}"


def flatten_sitematrix(data: dict) -> list[dict]:
    """Turn the sitematrix API response into one row per (language, project) wiki.

    Keys are matched to pageview domain codes: "en" + "wikipedia" for en.wikipedia.org, and
    for the multilingual "specials" the first host label + project, e.g. "commons" +
    "wikimedia" for commons.wikimedia.org and "www" + "wikidata" for www.wikidata.org.
    """
    rows = []
    for special in data["sitematrix"].get("specials", []):
        label, _, domain = (urllib.parse.urlparse(special["url"]).hostname or "").partition(".")
        if domain in SPECIAL_DOMAINS:
            rows.append(
                {
                    "language_code": label,
                    "language_name": "Multilingual",
                    "project": SPECIAL_DOMAINS[domain],
                    "site_url": special["url"],
                    "dbname": special["dbname"],
                }
            )
    for key, language in data["sitematrix"].items():
        if not key.isdigit():  # "count" and "specials"
            continue
        for site in language.get("site", []):
            rows.append(
                {
                    "language_code": language["code"],
                    "language_name": language.get("localname") or language.get("name"),
                    "project": "wikipedia" if site["code"] == "wiki" else site["code"],
                    "site_url": site["url"],
                    "dbname": site["dbname"],
                }
            )
    return rows


def _stage_and_copy(write, dest: str) -> None:
    """Write to a local temp file, then copy it into place, so readers never see partial files."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        write(tmp)
    try:
        shutil.copyfile(tmp.name, dest)
    finally:
        os.remove(tmp.name)


def fetch(url: str, dest: str, user_agent: str) -> bool:
    """Download url to dest unless it already exists. Returns False if it isn't published."""
    if os.path.exists(dest):
        print(f"skip  {dest} (already present)")
        return True
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        resp = urllib.request.urlopen(request, timeout=300)
    except urllib.error.HTTPError as err:
        if err.code == 404:
            print(f"miss  {url} (HTTP 404; not published yet?)")
            return False
        raise
    with resp:
        _stage_and_copy(lambda tmp: shutil.copyfileobj(resp, tmp), dest)
    print(f"saved {dest}")
    return True


def write_wikis(dest: str, user_agent: str) -> int:
    """Refresh the wiki site list CSV. Always overwritten; it's small and changes rarely."""
    request = urllib.request.Request(SITEMATRIX_URL, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(request, timeout=120) as resp:
        rows = flatten_sitematrix(json.load(resp))
    if not rows:
        raise SystemExit("Sitematrix returned no wikis")

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=WIKI_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    _stage_and_copy(lambda tmp: tmp.write(buffer.getvalue().encode("utf-8")), dest)
    print(f"saved {dest} ({len(rows)} wikis)")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days", required=True, help='e.g. "last:1", "2025-10-01:2025-10-07", "2025-10-01T13"'
    )
    parser.add_argument("--dest", required=True, help="Volume root, e.g. /Volumes/workspace/wikipedia/raw")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    args = parser.parse_args()

    write_wikis(f"{args.dest}/wikis/wikis.csv", args.user_agent)

    hours = expand_hours(args.days)
    available = [
        hour
        for hour in hours
        if fetch(hour_url(hour), f"{args.dest}/pageviews/{hour_file_name(hour)}", args.user_agent)
    ]
    if not available:
        raise SystemExit(f"None of the {len(hours)} requested hours are published yet")
    print(f"{len(available)}/{len(hours)} hours present in {args.dest}/pageviews")


if __name__ == "__main__":
    main()
