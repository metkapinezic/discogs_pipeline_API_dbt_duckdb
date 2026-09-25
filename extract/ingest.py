import json
import os
import time
from datetime import datetime, timezone
import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

## from documentation, connect to api data

BASE = "https://api.discogs.com"
HEADERS = {
    "User-Agent": os.environ["DISCOGS_USER_AGENT"],
    "Authorization": f"Discogs token={os.environ['DISCOGS_TOKEN']}",
}
DB_PATH = os.environ["DUCKDB_PATH"]
LABEL_ID = 26011         


## fetches one url, retries 3 times, timeout 10 seconds, 429 = too many requests

def get(url: str, params: dict | None = None, retries: int = 3) -> dict:  
    for attempt in range(retries):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if resp.status_code == 429:                     # rate limited
            time.sleep(5 * 2 ** attempt)
            continue
        resp.raise_for_status()
        if int(resp.headers.get("X-Discogs-Ratelimit-Remaining", 60)) < 5:
            time.sleep(10)
        return resp.json()
    raise RuntimeError(f"gave up after {retries} retries: {url}")

# function that gest through all the pages and it loops until the url exists

def paginate(url: str, key: str, params: dict | None = None):
    params = {"per_page": 100, **(params or {})}
    while url:
        data = get(url, params)
        yield from data[key]
        url = data["pagination"]["urls"].get("next")    # None on last page
        params = None

# Function that takes a database connection and a label id, returns how many rows it loaded.

def load_label_releases(con, label_id: int) -> int:
    con.execute("create schema if not exists discogs")          # <- new line
    con.execute("""
        create table if not exists discogs.raw_label_releases (
            label_id    bigint,
            release_id  bigint,
            payload     json,
            loaded_at   timestamptz
        )
    """)                                                         # <- discogs. added
    now = datetime.now(timezone.utc)
    rows = [
        (label_id, r["id"], json.dumps(r), now)
        for r in paginate(f"{BASE}/labels/{label_id}/releases", "releases")
    ]
    # full refresh for this label
    con.execute("delete from discogs.raw_label_releases where label_id = ?", [label_id])      # <- discogs. added
    con.executemany("insert into discogs.raw_label_releases values (?, ?, ?, ?)", rows)       # <- discogs. added
    return len(rows)


if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    n = load_label_releases(con, LABEL_ID)
    print(f"raw_label_releases: {n} rows for label {LABEL_ID}")
    con.close()