# Discogs Label Pipeline

End-to-end data pipeline that ingests the full release catalog of the Tommy Boy record label from the Discogs API, lands it raw in DuckDB, models it with dbt and orchestrates it with Dagster.

## Architecture

```
Discogs API
  → extract/ingest.py            (Python, paginated, retries, rate limit aware)
  → discogs.raw_label_releases   (DuckDB, raw JSON, full refresh)
  → dbt staging                  (flatten, cast, dedupe)
  → dbt marts                    (dim_artists)
  → Dagster                      (asset lineage, weekly schedule)
```

## Stack

Python, DuckDB, dbt, Dagster, Discogs API

## Project structure

```
├── extract/
│   └── ingest.py            # API extract + load to DuckDB
├── discogs_dbt/
│   ├── models/
│   │   ├── staging/         # stg_label_releases, sources, tests
│   │   ├── intermediate/
│   │   └── marts/           # dim_artists
│   ├── dbt_project.yml
│   └── profiles.yml
├── definitions.py           # Dagster assets, job and schedule
├── requirements.txt
└── .env.example
```

## Setup

1. Create a Discogs personal token at discogs.com/settings/developers
2. Copy `.env.example` to `.env` and fill in:

```
DISCOGS_TOKEN=
DISCOGS_USER_AGENT=
DUCKDB_PATH=/path/to/your.duckdb
DBT_PROFILES_DIR=/path/to/repo/discogs_dbt
DAGSTER_HOME=/path/outside/repo/dagster_home
```

3. Install:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Run

Step by step:

```bash
set -a; source .env; set +a
python extract/ingest.py
cd discogs_dbt && dbt build
```

With Dagster:

```bash
set -a; source .env; set +a
dagster dev -f definitions.py
```

Open `http://localhost:3000`, materialize all assets, or turn on the `weekly_refresh` schedule (Mondays 06:00).

## Data model

**raw_label_releases**: one row per listing from `/labels/26011/releases`, full API response stored as JSON with `loaded_at`. Full refresh on every run.

**stg_label_releases**: flattened payload. Title, artist, catalog number, format split into medium and descriptions, release year (0 → null), community want/have counts. Exact duplicates removed via an md5 row hash.

**dim_artists**: one row per artist as credited, with display name (Discogs `(n)` suffix removed), compilation flag, first/last release year and release count.

## Design decisions

- **ELT**: raw JSON lands untouched, all parsing happens in dbt. A change in the API response never breaks the load, and models can be rebuilt without calling the API again.
- **Full refresh raw, history downstream**: the label list is cheap to refetch (100 releases per request). Change history is planned via dbt snapshots.
- **Attributes vs metrics**: descriptive fields (title, format, year) change rarely and suit SCD2 snapshots. Community want/have counts change constantly, so they are planned as a periodic snapshot fact instead of bloating the dimension.
- **Dagster over Airflow**: the dbt integration exposes every model as an asset with lineage, and the ingest asset is linked to the dbt source, so dbt never runs on a failed load.

## Known limitations

- The label endpoint only returns artist names as one text field, no ids. Joint credits (`A & B`) count as one artist and spelling variants are not merged yet.
- DuckDB allows one writer at a time, so close other connections (e.g. DBeaver) before a run.

## Next steps

- Artist name cleaning: feat. extraction, lead artist, seed mapping for spelling variants
- dbt snapshot on release attributes, weekly stats fact table
- Full release details (`/releases/{id}`) for tracklists and real artist ids
- Docker Compose setup
