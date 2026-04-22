# Snowpack Tracker

`snowpack-tracker` is a small Python CLI for pulling daily SNOTEL data from the USDA NRCS report generator and rendering it as a simple black-bordered HTML table you can open in a browser.

It is geared toward quick station lookups and lightweight local reporting rather than a full web app.

## What It Does

- Fetches daily SNOTEL observations for a station and date range
- Includes snow water equivalent, snow depth, precipitation, and temperature columns
- Writes a deterministic HTML report like `snotel_784_2026-04-01_2026-04-21.html`
- Opens the generated report in your default browser
- Supports direct station lookup by site ID or exact site name
- Supports popup-based natural-language station search (requires `OPENAI_API_KEY`; falls back to fuzzy if the API call fails)
- Includes a state-level Streamlit dashboard for percentiles + narrative summaries

## Project Layout

- [`snotel.py`](/C:/Users/zhiha/snowpack-tracker/snotel.py) - main CLI, HTML rendering, popup search flow
- [`snotel_sites.py`](/C:/Users/zhiha/snowpack-tracker/snotel_sites.py) - SNOTEL site metadata used for station lookup
- [`climate_report.py`](/C:/Users/zhiha/snowpack-tracker/climate_report.py) - baseline percentile computations + caching
- [`narrative.py`](/C:/Users/zhiha/snowpack-tracker/narrative.py) - grounded AI narrative (JSON) + validation + fallback template
- [`app.py`](/C:/Users/zhiha/snowpack-tracker/app.py) - Streamlit dashboard UI
- [`test_snotel.py`](/C:/Users/zhiha/snowpack-tracker/test_snotel.py) - unit tests for the CLI and helpers
- [`test_climate_report.py`](/C:/Users/zhiha/snowpack-tracker/test_climate_report.py) - unit tests for percentiles + narrative validation
- [`climata/`](/C:/Users/zhiha/snowpack-tracker/climata) - vendored upstream library code referenced during development
- [`waterdata.py`](/C:/Users/zhiha/snowpack-tracker/waterdata.py) - separate USGS water data experiment

## Requirements

- Python 3
- `pandas`
- Internet access to reach the USDA SNOTEL endpoint
- For the dashboard: `streamlit` (and optionally `pyarrow` for Parquet caching)

## Setup

Create and activate a virtual environment, then install the minimal dependency:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install pandas
```

Optionally, set your OpenAI API key for enhanced natural-language site search:

```powershell
$env:OPENAI_API_KEY="your_api_key_here"
```

If the API key is not set, the popup-based flow will not run. If the API call fails (network/key/model issues), it will fall back to fuzzy search using the local site catalog.

## State Dashboard (Streamlit)

Install dependencies:

```powershell
pip install streamlit pyarrow
```

Run the dashboard:

```powershell
streamlit run app.py
```

The dashboard:

- Computes per-station **day-of-season percentiles** vs a baseline window (default `1991-2020`)
- Aggregates to a statewide **median percentile** time series
- Shows **top/bottom stations** for the selected end date + missingness/coverage
- Generates an optional **AI narrative** that is validated to avoid introducing numbers not present in the computed fact set

Raw station pulls are cached to `data_cache/` (Parquet when possible; otherwise CSV).

## Usage

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

In the no-site flow, the script:

1. Starts a small local HTTP server on `127.0.0.1`
2. Opens a browser popup asking for a natural-language site search
3. Uses OpenAI GPT (requires `OPENAI_API_KEY`; if the API call fails it falls back to fuzzy search)
4. Resolves the best matching site
5. Fetches SNOTEL data and writes the final HTML report

## Output

The generated report:

- is written to the current working directory
- uses a filename based on site ID and date range
- contains the station name and elevation in the heading when available
- shows rows in descending date order

Example output filename:

```text
snotel_784_2026-04-01_2026-04-21.html
```

The CLI also prints a summary message with the output path, site, and requested range.

## Data Columns

The HTML report currently displays these normalized columns:

- `date`
- `swe_in`
- `snow_depth_in`
- `precip_in`
- `tmax_f`
- `tmin_f`
- `tavg_f`

These are sourced from the NRCS daily station report fields:

- Snow Water Equivalent
- Snow Depth
- Precipitation Accumulation
- Air Temperature Maximum
- Air Temperature Minimum
- Air Temperature Average

## Testing

Run the unit tests with:

```powershell
python -m unittest test_snotel.py
```

The tests cover:

- station lookup by ID and exact name
- CLI argument parsing
- popup search flow
- HTML output naming and content
- display formatting behavior

## Notes

- Exact-name lookup is case-insensitive, but it is not fuzzy unless you use the popup search flow.
- If `OPENAI_API_KEY` is not set, the popup-based flow will not run.
- The vendored [`climata/`](/C:/Users/zhiha/snowpack-tracker/climata) directory appears to be an older upstream dependency snapshot and is not the main entry point for this tool.
- Sample generated HTML files and test artifacts in the repo are useful references, but they are not required to run the CLI.

## Example

```powershell
python snotel.py 395 2026-04-01 2026-04-21
```

This will fetch the requested daily data for site `395`, generate an HTML table, and open it in your browser.
