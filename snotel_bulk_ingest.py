import argparse
import csv
import json
import re
import sys
import tempfile
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from snotel_postgres import connect_postgres, ensure_snotel_partitioned_schema
from snotel_sites import SITE_ID_TO_INFO


DEFAULT_START_DATE = date(1950, 1, 1)
DEFAULT_END_DATE = date(2000, 12, 31)
POR_ELEMENT_CODES = ("POR_BEGIN", "POR_END")


def build_snotel_url(site_id, awdb_state, start_date, end_date, element_codes):
    elements = ",".join(f"{code}::value" for code in element_codes)
    return (
        "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
        f"customSingleStationReport/daily/{site_id}:{awdb_state}:SNTL%7Cid=%22%22%7Cname/"
        f"{start_date},{end_date}/{elements}"
    )


def _extract_csv_text(report_text):
    if "#------------------------------------------------- ERROR" in report_text:
        detail_lines = [
            line[1:].strip()
            for line in report_text.splitlines()
            if line.startswith("#") and "Stations do not exist" in line
        ]
        detail = detail_lines[0] if detail_lines else "Unknown SNOTEL report error."
        raise ValueError(detail)

    lines = report_text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("Date,"):
            return "\n".join(lines[index:])

    raise ValueError("Could not find the CSV header in the SNOTEL response.")


def _parse_float(value):
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "nan", "NaN", "None", "NULL"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number <= -900:
        return None
    return number


def _season_bucket(obs_date):
    month = obs_date.month
    if month in (11, 12, 1):
        return 0
    if month in (2, 3, 4):
        return 1
    return None


def parse_por_range(report_text):
    match = None
    for line in report_text.splitlines():
        if "POR_BEGIN" in line and "POR_END" in line and line.strip().startswith("POR_BEGIN"):
            match = line.strip()
            break

    if match:
        lines = report_text.splitlines()
        header_index = lines.index(match)
        if header_index + 1 < len(lines):
            values = [v.strip() for v in lines[header_index + 1].split(",")]
            if len(values) >= 2:
                try:
                    begin = datetime.strptime(values[0], "%Y-%m-%d").date()
                    end = datetime.strptime(values[1], "%Y-%m-%d").date()
                    return begin, end
                except Exception:
                    pass

    begin = None
    end = None
    for line in report_text.splitlines():
        text = line.lstrip("#").strip()
        if begin is None:
            m = re.search(r"\bPOR_BEGIN\b[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
            if m:
                try:
                    begin = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                except Exception:
                    begin = None
        if end is None:
            m = re.search(r"\bPOR_END\b[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
            if m:
                try:
                    end = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                except Exception:
                    end = None

    if begin and end:
        return begin, end
    return None


def fetch_site_por_range(*, site_id, awdb_state, timeout_seconds=60, retries=4, retry_backoff_seconds=1.0):
    url = build_snotel_url(site_id, awdb_state, "POR_BEGIN", "POR_END", POR_ELEMENT_CODES)
    headers = {"User-Agent": "snowpack-tracker/1.0 (+https://github.com/openai/codex)"}
    last_error = None
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout_seconds) as response:
                report_text = response.read().decode("utf-8", errors="replace")
            parsed = parse_por_range(report_text)
            if not parsed:
                raise ValueError("Could not parse POR_BEGIN/POR_END from SNOTEL response.")
            return parsed
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            if attempt >= retries:
                raise RuntimeError(f"SNOTEL POR fetch failed for site {site_id}: {exc}") from exc
            sleep_seconds = retry_backoff_seconds * (2**attempt)
            time.sleep(sleep_seconds)

    raise RuntimeError(f"SNOTEL POR fetch failed for site {site_id}: {last_error}")


def fetch_site_daily_rows(
    *,
    site_id,
    awdb_state,
    site_name,
    elevation_ft,
    start_date,
    end_date,
    element_codes,
    timeout_seconds=60,
    retries=4,
    retry_backoff_seconds=2.0,
):
    url = build_snotel_url(site_id, awdb_state, start_date.isoformat(), end_date.isoformat(), element_codes)
    headers = {"User-Agent": "snowpack-tracker/1.0 (+https://github.com/openai/codex)"}
    last_error = None
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout_seconds) as response:
                report_text = response.read().decode("utf-8", errors="replace")
            csv_text = _extract_csv_text(report_text)
            break
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            if attempt >= retries:
                raise RuntimeError(f"SNOTEL fetch failed for site {site_id}: {exc}") from exc
            sleep_seconds = retry_backoff_seconds * (2**attempt)
            time.sleep(sleep_seconds)
    else:
        raise RuntimeError(f"SNOTEL fetch failed for site {site_id}: {last_error}")

    reader = csv.DictReader(StringIO(csv_text))
    rows = []
    for row in reader:
        try:
            obs_dt = datetime.strptime(row["Date"].strip(), "%Y-%m-%d").date()
        except Exception:
            continue

        bucket = _season_bucket(obs_dt)
        if bucket is None:
            continue

        swe_in = _parse_float(row.get("Snow Water Equivalent (in) Start of Day Values"))
        snow_depth_in = _parse_float(row.get("Snow Depth (in) Start of Day Values"))
        precip_in = _parse_float(row.get("Precipitation Accumulation (in) Start of Day Values"))
        tmax_f = _parse_float(row.get("Air Temperature Maximum (degF)"))
        tmin_f = _parse_float(row.get("Air Temperature Minimum (degF)"))
        tavg_f = _parse_float(row.get("Air Temperature Average (degF)"))

        rows.append(
            (
                int(site_id),
                awdb_state,
                site_name,
                elevation_ft,
                obs_dt,
                bucket,
                swe_in,
                snow_depth_in,
                precip_in,
                tmax_f,
                tmin_f,
                tavg_f,
            )
        )

    return rows


def _insert_rows(cur, driver_name, full_table_rel, rows, page_size=5000):
    if not rows:
        return 0

    insert_sql = (
        f"INSERT INTO {full_table_rel} "
        "(site_id, awdb_state, site_name, elevation_ft, obs_date, season_bucket, "
        "swe_in, snow_depth_in, precip_in, tmax_f, tmin_f, tavg_f) "
        "VALUES %s "
        "ON CONFLICT DO NOTHING"
    )

    if driver_name == "psycopg2":
        from psycopg2.extras import execute_values  # type: ignore

        template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        execute_values(cur, insert_sql, rows, template=template, page_size=page_size)
        return len(rows)

    if driver_name == "psql":
        columns = (
            "site_id,awdb_state,site_name,elevation_ft,obs_date,season_bucket,"
            "swe_in,snow_depth_in,precip_in,tmax_f,tmin_f,tavg_f"
        )
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, suffix=".csv") as handle:
            csv_path = Path(handle.name)
            writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
            for row in rows:
                serialized = []
                for value in row:
                    if value is None:
                        serialized.append(r"\N")
                    elif hasattr(value, "isoformat"):
                        serialized.append(value.isoformat())
                    else:
                        serialized.append(value)
                writer.writerow(serialized)
        try:
            copy_sql = (
                f"\\copy {full_table_rel} ({columns}) FROM '{csv_path.as_posix()}' "
                "WITH (FORMAT csv, NULL '\\N')"
            )
            cur._connection._run_sql(copy_sql)
        finally:
            csv_path.unlink(missing_ok=True)
        return len(rows)

    insert_one_sql = (
        f"INSERT INTO {full_table_rel} "
        "(site_id, awdb_state, site_name, elevation_ft, obs_date, season_bucket, "
        "swe_in, snow_depth_in, precip_in, tmax_f, tmin_f, tavg_f) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING"
    )
    cur.executemany(insert_one_sql, rows)
    return len(rows)


def _parse_site_ids(value):
    if not value:
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts or None


def _append_progress(progress_path, payload):
    if not progress_path:
        return
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Fetch daily SNOTEL snow-season observations for all sites (or a subset) "
            "between 1950-01-01 and 2000-12-31 and load into Postgres with hash/list partitions."
        )
    )
    parser.add_argument("--dsn", help="Postgres DSN. If omitted, uses DATABASE_URL or PG_DSN.")
    parser.add_argument("--schema", default="snotel")
    parser.add_argument("--table", default="daily_observations")
    parser.add_argument("--hash-partitions", type=int, default=32)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--end-date", default=DEFAULT_END_DATE.isoformat())
    parser.add_argument(
        "--site-ids",
        help="Comma-separated site IDs to ingest (defaults to all known sites).",
    )
    parser.add_argument(
        "--elements",
        default="WTEQ,SNWD,PREC,TMAX,TMIN,TAVG",
        help="Comma-separated element codes to request from the report generator.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--use-por",
        action="store_true",
        default=True,
        help="Adjust per-site start date to max(--start-date, POR_BEGIN) (default: on).",
    )
    parser.add_argument(
        "--no-use-por",
        dest="use_por",
        action="store_false",
        help="Disable POR_BEGIN lookup; always use --start-date as-is.",
    )
    parser.add_argument("--progress-file", default="data_cache/snotel_ingest_1950_2000.jsonl")
    parser.add_argument("--commit-every-sites", type=int, default=1)
    args = parser.parse_args(argv)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    element_codes = [c.strip() for c in args.elements.split(",") if c.strip()]

    site_ids = _parse_site_ids(args.site_ids)
    if site_ids is None:
        site_ids = sorted(SITE_ID_TO_INFO.keys(), key=lambda v: int(v))
    else:
        missing = [site_id for site_id in site_ids if site_id not in SITE_ID_TO_INFO]
        if missing:
            raise SystemExit(f"Unknown site ids (not in snotel_sites.py): {', '.join(missing)}")

    progress_path = Path(args.progress_file) if args.progress_file else None

    conn, driver = connect_postgres(args.dsn)
    ensure_snotel_partitioned_schema(
        conn,
        driver,
        schema=args.schema,
        table=args.table,
        hash_partitions=args.hash_partitions,
    )

    full_table_rel = f"{args.schema}.{args.table}"
    cur = conn.cursor()

    total_sites = len(site_ids)
    total_rows = 0
    succeeded = 0
    failed = 0

    commit_counter = 0
    for index, site_id in enumerate(site_ids, start=1):
        info = SITE_ID_TO_INFO[site_id]
        awdb_state = info["awdb_state"]
        site_name = info["name"]
        elevation_ft = info.get("elevation_ft")
        started_at = datetime.now().isoformat(timespec="seconds")
        effective_start_date = start_date
        try:
            por_begin = None
            por_end = None
            if args.use_por:
                por_begin, por_end = fetch_site_por_range(
                    site_id=site_id,
                    awdb_state=awdb_state,
                    timeout_seconds=args.timeout_seconds,
                    retries=args.retries,
                )
                effective_start_date = max(start_date, por_begin)
                if effective_start_date > end_date:
                    raise RuntimeError(
                        f"Effective start {effective_start_date} exceeds end date {end_date}."
                    )

            rows = fetch_site_daily_rows(
                site_id=site_id,
                awdb_state=awdb_state,
                site_name=site_name,
                elevation_ft=elevation_ft,
                start_date=effective_start_date,
                end_date=end_date,
                element_codes=element_codes,
                timeout_seconds=args.timeout_seconds,
                retries=args.retries,
            )
            inserted = _insert_rows(cur, driver.name, full_table_rel, rows)
            total_rows += inserted
            succeeded += 1
            status = "ok"
            error = None
        except Exception as exc:
            conn.rollback()
            failed += 1
            status = "error"
            error = str(exc)
            inserted = 0

        _append_progress(
            progress_path,
            {
                "site_id": site_id,
                "site_name": site_name,
                "awdb_state": awdb_state,
                "start_date": effective_start_date,
                "end_date": end_date,
                "status": status,
                "inserted_rows": inserted,
                "started_at": started_at,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "error": error,
                "por_begin": por_begin if args.use_por else None,
                "por_end": por_end if args.use_por else None,
                "index": index,
                "total_sites": total_sites,
            },
        )

        commit_counter += 1
        if status == "ok" and commit_counter >= max(int(args.commit_every_sites), 1):
            conn.commit()
            commit_counter = 0

        print(
            f"[{index}/{total_sites}] site {site_id} ({site_name}) -> {status} "
            f"(rows={inserted}, total_rows={total_rows}, ok={succeeded}, err={failed})",
            file=sys.stderr,
        )

    if commit_counter:
        conn.commit()

    print(
        json.dumps(
            {
                "sites_total": total_sites,
                "sites_ok": succeeded,
                "sites_error": failed,
                "rows_inserted": total_rows,
                "table": full_table_rel,
            }
        )
    )


if __name__ == "__main__":
    main()
