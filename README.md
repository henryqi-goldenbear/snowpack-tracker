# Snowpack Tracker

`snowpack-tracker` is a small Python toolkit for pulling daily SNOTEL data from the USDA NRCS report generator, rendering quick HTML reports, and loading historical snow-season observations into partitioned PostgreSQL tables.

It is still a lightweight local project rather than a full production app, but the core integration paths are now working end to end.

## What Works Now

- Single-station CLI fetches daily SNOTEL observations and writes deterministic HTML output
- Station lookup works by site ID, exact name, and popup-based natural-language search
- State-level dashboard code computes percentiles and optional narratives
- Bulk ingest can fetch real NRCS data, partition it by `site_id` and season bucket, and load it into PostgreSQL
- Real Postgres schema creation and real one-site ingest smoke tests have both been verified

## Project Layout

- [`snotel.py`](/C:/Users/zhiha/snowpack-tracker/snotel.py) - main CLI, HTML rendering, popup search flow
- [`snotel_sites.py`](/C:/Users/zhiha/snowpack-tracker/snotel_sites.py) - SNOTEL site metadata
- [`snotel_bulk_ingest.py`](/C:/Users/zhiha/snowpack-tracker/snotel_bulk_ingest.py) - historical SNOTEL to Postgres loader
- [`snotel_postgres.py`](/C:/Users/zhiha/snowpack-tracker/snotel_postgres.py) - Postgres connection and partitioned schema creation
- [`app.py`](/C:/Users/zhiha/snowpack-tracker/app.py) - Streamlit dashboard UI
- [`test_snotel.py`](/C:/Users/zhiha/snowpack-tracker/test_snotel.py), [`test_climate_report.py`](/C:/Users/zhiha/snowpack-tracker/test_climate_report.py), [`test_snotel_postgres.py`](/C:/Users/zhiha/snowpack-tracker/test_snotel_postgres.py), [`test_snotel_bulk_ingest.py`](/C:/Users/zhiha/snowpack-tracker/test_snotel_bulk_ingest.py) - test coverage

## Requirements

- Python 3
- `pandas`
- Internet access to reach the USDA SNOTEL endpoint
- For dashboard work: `streamlit` and optionally `pyarrow`
- For Postgres ingest: a reachable PostgreSQL server and either:
  - a working Python Postgres driver (`psycopg2`), or
  - `psql` available locally as a fallback client

## Setup

Create and activate a virtual environment, then install the minimal dependency:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install pandas
```

Optionally set your OpenAI API key for the natural-language site search flow:

```powershell
$env:OPENAI_API_KEY="your_api_key_here"
```

If the API key is not set, the popup-based flow will not run. If the API call fails, the search falls back to the local site catalog.

## CLI Usage

Run with an explicit site ID:

```powershell
python snotel.py 784 2026-04-01 2026-04-21
```

Run with an exact site name:

```powershell
python snotel.py "Palisades Tahoe" 2026-04-01 2026-04-21
```

Run without a site argument:

```powershell
python snotel.py 2026-04-01 2026-04-21
```

The generated report is written to the current directory with a deterministic filename such as `snotel_784_2026-04-01_2026-04-21.html`.

## State Dashboard

Install dependencies:

```powershell
pip install streamlit pyarrow
```

Run the dashboard:

```powershell
streamlit run app.py
```

The dashboard computes day-of-season percentiles, aggregates statewide median percentiles, and can generate a constrained narrative summary.

## Bulk Postgres Ingest

The bulk loader pulls daily observations for all known sites, filters to the snow season months, and stores them in a partitioned Postgres table.

Partitioning scheme:

- Parent table is hash-partitioned by `site_id`
- Each hash partition is sub-partitioned by `season_bucket`
- `season_bucket = 0` means `Nov-Jan`
- `season_bucket = 1` means `Feb-Apr`

Effective date window:

- Default requested range is `1950-01-01` through `2000-12-31`
- By default the loader first fetches `POR_BEGIN` / `POR_END`
- Actual start date per site is `max(1950-01-01, POR_BEGIN)`
- Use `--no-use-por` to disable that adjustment

Preferred Postgres client setup:

```powershell
pip install psycopg2-binary
```

Set your connection string:

```powershell
$env:DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

Run the full ingest:

```powershell
python snotel_bulk_ingest.py --hash-partitions 32
```

Progress is written to `data_cache/snotel_ingest_1950_2000.jsonl`. Inserts are idempotent via `ON CONFLICT DO NOTHING`.

### Postgres Client Fallback

The standard Postgres client path is `psycopg2`. Repo-local dependency directories such as `.deps` and `.pgdeps` are optional fallbacks, not the primary install target. If `psycopg2` is unavailable or broken, the repo can fall back to `psql` automatically for schema creation and bulk inserts. On the validation machine, that fallback was used successfully for the live integration and smoke ingest checks.

### Verified Smoke Test

This one-site ingest path was verified against a real local Postgres instance:

```powershell
$env:DATABASE_URL="postgresql://snowpack:snowpack@localhost:5432/snowpack"
python snotel_bulk_ingest.py --schema snotel_smoke --table daily_observations --hash-partitions 4 --site-ids 395 --start-date 2000-01-01 --end-date 2000-04-30 --no-use-por
```

That run inserted `121` rows for site `395` covering `2000-01-01` through `2000-04-30`, and the resulting rows were queryable from Postgres.

## Postgres Integration Test

Set `DATABASE_URL` and run:

```powershell
python run_live_postgres_integration.py
```

That test creates a temporary schema, builds the partitioned table structure, verifies the parent and child partitions exist, and then drops the temporary schema.

If you want to run Postgres in Docker instead of using an existing install, the repo also includes:

- [`docker-compose.yml`](/C:/Users/zhiha/snowpack-tracker/docker-compose.yml)
- [`run_postgres_integration.ps1`](/C:/Users/zhiha/snowpack-tracker/run_postgres_integration.ps1)

## Testing

Run the core test suite with:

```powershell
python -m unittest test_snotel.py test_climate_report.py test_snotel_postgres.py test_snotel_bulk_ingest.py
```

Live verification commands used successfully during validation:

```powershell
python run_live_postgres_integration.py
python snotel_bulk_ingest.py --schema snotel_smoke --table daily_observations --hash-partitions 4 --site-ids 395 --start-date 2000-01-01 --end-date 2000-04-30 --no-use-por
```

## Notes

- Exact-name lookup is case-insensitive; fuzzy search is only used in the popup flow fallback
- The vendored [`climata/`](/C:/Users/zhiha/snowpack-tracker/climata) directory is not the main entry point for this project
- Sample HTML files and `test_artifacts/` are reference outputs, not required runtime assets
