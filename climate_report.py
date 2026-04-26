import hashlib
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional

import pandas as pd

import snotel
from snotel_sites import SITE_ID_TO_INFO


METRIC_OPTIONS = {
    "swe": "swe_in",
    "snow_depth": "snow_depth_in",
    "precip": "precip_in",
    "tavg": "tavg_f",
}


def month_day_key(value) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, datetime):
        value = value.date()
    if not isinstance(value, date):
        raise TypeError("month_day_key expects a date/datetime value.")
    if value.month == 2 and value.day == 29:
        return "02-28"
    return value.strftime("%m-%d")


def default_season_dates(today: Optional[date] = None) -> tuple[str, str]:
    today = today or date.today()
    start_year = today.year if today.month >= 10 else today.year - 1
    start = date(start_year, 10, 1)
    return start.isoformat(), today.isoformat()


def list_awdb_states(site_map=None) -> list[str]:
    site_map = SITE_ID_TO_INFO if site_map is None else site_map
    states = {info["awdb_state"] for info in site_map.values()}
    return sorted(states)


def sites_for_awdb_state(awdb_state: str, site_map=None) -> list[tuple[str, dict]]:
    site_map = SITE_ID_TO_INFO if site_map is None else site_map
    matches = [
        (site_id, info)
        for site_id, info in site_map.items()
        if info.get("awdb_state") == awdb_state
    ]
    return sorted(matches, key=lambda item: (item[1].get("elevation_ft") or 0, item[0]))


def select_representative_sites(
    sites: list[tuple[str, dict]], total_n: int = 45
) -> list[tuple[str, dict]]:
    if not sites:
        return []

    total_n = max(int(total_n), 1)
    per_band = max(total_n // 3, 1)
    ordered = sorted(sites, key=lambda item: (item[1].get("elevation_ft") or 0, item[0]))

    n = len(ordered)
    one_third = n // 3
    two_third = (2 * n) // 3
    low = ordered[:one_third] or ordered[:1]
    mid = ordered[one_third:two_third] or ordered[:1]
    high = ordered[two_third:] or ordered[:1]

    selected = low[:per_band] + mid[:per_band] + high[:per_band]
    selected_ids = {site_id for site_id, _ in selected}
    if len(selected_ids) >= total_n:
        return selected[:total_n]

    for site_id, info in ordered:
        if site_id in selected_ids:
            continue
        selected.append((site_id, info))
        selected_ids.add(site_id)
        if len(selected_ids) >= total_n:
            break
    return selected


def filter_sites_by_elevation(
    sites: list[tuple[str, dict]],
    *,
    min_ft: Optional[int] = None,
    max_ft: Optional[int] = None,
    include_unknown: bool = False,
) -> list[tuple[str, dict]]:
    if min_ft is None and max_ft is None:
        return list(sites)

    out: list[tuple[str, dict]] = []
    for site_id, info in sites:
        elevation = info.get("elevation_ft")
        if elevation is None:
            if include_unknown:
                out.append((site_id, info))
            continue
        if min_ft is not None and elevation < min_ft:
            continue
        if max_ft is not None and elevation > max_ft:
            continue
        out.append((site_id, info))
    return out


def _safe_cache_stem(site_id: str, awdb_state: str, start_date: str, end_date: str) -> str:
    key = f"{site_id}_{awdb_state}_{start_date}_{end_date}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    return f"{site_id}_{awdb_state}_{start_date}_{end_date}_{digest}"


def _cache_paths(cache_dir: Path, stem: str) -> tuple[Path, Path]:
    return cache_dir / f"{stem}.parquet", cache_dir / f"{stem}.csv"


def _includes_today(end_date: str) -> bool:
    try:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    return end >= date.today()


def _try_read_cached(cache_dir: Path, stem: str) -> Optional[pd.DataFrame]:
    parquet_path, csv_path = _cache_paths(cache_dir, stem)
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        df = pd.read_csv(csv_path, parse_dates=["Date"])
        return df
    return None


def _try_write_cache(cache_dir: Path, stem: str, df: pd.DataFrame) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path, csv_path = _cache_paths(cache_dir, stem)
    try:
        df.to_parquet(parquet_path, index=False)
        if csv_path.exists():
            try:
                csv_path.unlink()
            except OSError:
                pass
        return
    except Exception:
        df.to_csv(csv_path, index=False)
        if parquet_path.exists():
            try:
                parquet_path.unlink()
            except OSError:
                pass


def _chunk_ranges(start: date, end: date, max_days: int = 1200) -> list[tuple[str, str]]:
    if start > end:
        return []
    ranges: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=max_days - 1))
        ranges.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)
    return ranges


def fetch_station_daily_data(
    site_id: str,
    awdb_state: str,
    start_date: str,
    end_date: str,
    *,
    cache_dir: str | Path = "data_cache",
    refresh_if_end_includes_today: bool = True,
    max_days_per_request: int = 1200,
) -> pd.DataFrame:
    cache_dir = Path(cache_dir)
    stem = _safe_cache_stem(site_id, awdb_state, start_date, end_date)

    if not (refresh_if_end_includes_today and _includes_today(end_date)):
        cached = _try_read_cached(cache_dir, stem)
        if cached is not None:
            return cached

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    pieces: list[pd.DataFrame] = []
    for chunk_start, chunk_end in _chunk_ranges(start, end, max_days=max_days_per_request):
        pieces.append(snotel.get_snotel_data(site_id, awdb_state, chunk_start, chunk_end))

    df = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    if not df.empty:
        df = df.drop_duplicates(subset=["Date"]).sort_values("Date")
    _try_write_cache(cache_dir, stem, df)
    return df


def _normalized_metric_column(metric_key: str) -> str:
    metric_key = (metric_key or "").strip().casefold()
    if metric_key not in METRIC_OPTIONS:
        raise ValueError(f"Unknown metric '{metric_key}'. Expected one of: {', '.join(METRIC_OPTIONS)}.")
    return METRIC_OPTIONS[metric_key]


def _normalize_snotel_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    renamed = df.rename(columns=snotel.DISPLAY_COLUMNS)
    if "date" not in renamed.columns and "Date" in df.columns:
        renamed = renamed.rename(columns={"Date": "date"})
    if "date" in renamed.columns:
        renamed["date"] = pd.to_datetime(renamed["date"])
    return renamed


def percentile_from_baseline(value: float, baseline_values: Iterable[float]) -> float:
    baseline_list = [v for v in baseline_values if pd.notna(v)]
    if pd.isna(value) or not baseline_list:
        return float("nan")
    count_le = sum(1 for v in baseline_list if v <= value)
    return (count_le / len(baseline_list)) * 100.0


def compute_station_percentiles(
    baseline_df: pd.DataFrame, current_df: pd.DataFrame, metric_key: str
) -> pd.DataFrame:
    metric_col = _normalized_metric_column(metric_key)
    baseline_df = _normalize_snotel_df(baseline_df)
    current_df = _normalize_snotel_df(current_df)

    if baseline_df.empty or current_df.empty:
        return pd.DataFrame(columns=["date", "value", "percentile"])

    baseline_df = baseline_df[["date", metric_col]].copy()
    baseline_df["month_day"] = baseline_df["date"].dt.date.map(month_day_key)
    baseline_values = (
        baseline_df.dropna(subset=[metric_col])
        .groupby("month_day")[metric_col]
        .apply(list)
        .to_dict()
    )
    baseline_medians = {
        month_day: float(pd.Series(values).median())
        for month_day, values in baseline_values.items()
        if values
    }

    out = current_df[["date", metric_col]].copy()
    out = out.rename(columns={metric_col: "value"})
    out["month_day"] = out["date"].dt.date.map(month_day_key)
    out["percentile"] = out.apply(
        lambda row: percentile_from_baseline(row["value"], baseline_values.get(row["month_day"], [])),
        axis=1,
    )
    out["baseline_median"] = out["month_day"].map(lambda md: baseline_medians.get(md, float("nan")))
    return out[["date", "value", "percentile", "baseline_median"]]


def build_state_series(
    percentiles: pd.DataFrame,
    *,
    season_start: str,
    season_end: str,
    total_sites: int,
) -> pd.DataFrame:
    if percentiles.empty:
        median_series = pd.Series(dtype="float64")
        used_series = pd.Series(dtype="int64")
    else:
        by_date = percentiles.groupby(percentiles["date"].dt.date)["percentile"]
        median_series = by_date.median()
        used_series = by_date.apply(lambda s: int(s.notna().sum()))

    season_start_date = datetime.strptime(season_start, "%Y-%m-%d").date()
    season_end_date = datetime.strptime(season_end, "%Y-%m-%d").date()
    day_index = pd.date_range(season_start_date, season_end_date, freq="D").date

    state_series = pd.DataFrame({"date": list(day_index)})
    state_series["median_percentile"] = state_series["date"].map(
        lambda d: float(median_series.get(d, float("nan")))
    )
    state_series["stations_used"] = state_series["date"].map(lambda d: int(used_series.get(d, 0)))
    state_series["stations_missing"] = int(total_sites) - state_series["stations_used"]
    state_series["pct_with_data"] = state_series["stations_used"].map(
        lambda used: (used / total_sites) * 100.0 if total_sites else 0.0
    )
    return state_series


@dataclass(frozen=True)
class StateReportInputs:
    awdb_state: str
    metric_key: str
    season_start: str
    season_end: str
    baseline_start_year: int = 1991
    baseline_end_year: int = 2020
    coverage_mode: str = "representative"
    representative_n: int = 45
    elevation_min_ft: Optional[int] = None
    elevation_max_ft: Optional[int] = None
    include_unknown_elevation: bool = False
    cache_dir: str | Path = "data_cache"


@dataclass(frozen=True)
class StateReport:
    inputs: StateReportInputs
    state_series: pd.DataFrame
    station_snapshot: pd.DataFrame
    narrative_facts: dict


def compute_state_report(
    inputs: StateReportInputs,
    *,
    progress: Optional[Callable[[int, int], None]] = None,
) -> StateReport:
    all_sites = sites_for_awdb_state(inputs.awdb_state)
    all_sites = filter_sites_by_elevation(
        all_sites,
        min_ft=inputs.elevation_min_ft,
        max_ft=inputs.elevation_max_ft,
        include_unknown=inputs.include_unknown_elevation,
    )
    if inputs.coverage_mode == "all":
        selected = all_sites
    else:
        selected = select_representative_sites(all_sites, total_n=inputs.representative_n)

    total = len(selected)
    baseline_start = f"{inputs.baseline_start_year}-01-01"
    baseline_end = f"{inputs.baseline_end_year}-12-31"

    if total == 0:
        state_series = build_state_series(
            pd.DataFrame(),
            season_start=inputs.season_start,
            season_end=inputs.season_end,
            total_sites=0,
        )
        state_series["median_value"] = float("nan")
        state_series["median_baseline_value"] = float("nan")
        snapshot_df = pd.DataFrame(
            columns=["site_id", "name", "elevation_ft", "value", "percentile", "has_data"]
        )
        facts = {
            "state": inputs.awdb_state,
            "metric_key": inputs.metric_key,
            "metric_label": _normalized_metric_column(inputs.metric_key),
            "season_start": inputs.season_start,
            "season_end": inputs.season_end,
            "baseline_years": f"{inputs.baseline_start_year}-{inputs.baseline_end_year}",
            "coverage_mode": inputs.coverage_mode,
            "representative_n": inputs.representative_n,
            "elevation_min_ft": inputs.elevation_min_ft,
            "elevation_max_ft": inputs.elevation_max_ft,
            "include_unknown_elevation": inputs.include_unknown_elevation,
            "station_count_total": 0,
            "end_date": inputs.season_end,
            "end_statewide_median_percentile": "NA",
            "end_stations_used": 0,
            "end_stations_missing": 0,
            "end_pct_with_data": "NA",
            "top_stations": [],
            "bottom_stations": [],
            "missing_station_examples": [],
        }
        return StateReport(
            inputs=inputs,
            state_series=state_series,
            station_snapshot=snapshot_df,
            narrative_facts=facts,
        )

    per_station_end_rows: list[dict] = []
    all_percentiles: list[pd.DataFrame] = []
    all_values: list[pd.DataFrame] = []

    for index, (site_id, info) in enumerate(selected, start=1):
        if progress is not None:
            progress(index, total)

        baseline_df = fetch_station_daily_data(
            site_id,
            inputs.awdb_state,
            baseline_start,
            baseline_end,
            cache_dir=inputs.cache_dir,
        )
        current_df = fetch_station_daily_data(
            site_id,
            inputs.awdb_state,
            inputs.season_start,
            inputs.season_end,
            cache_dir=inputs.cache_dir,
        )

        station_pct = compute_station_percentiles(baseline_df, current_df, inputs.metric_key)
        if station_pct.empty:
            end_value = float("nan")
            end_pct = float("nan")
        else:
            end_date = pd.to_datetime(inputs.season_end).normalize()
            end_row = station_pct.loc[station_pct["date"].dt.normalize() == end_date]
            if end_row.empty:
                end_value = float("nan")
                end_pct = float("nan")
            else:
                end_value = float(end_row.iloc[-1]["value"])
                end_pct = float(end_row.iloc[-1]["percentile"])

        snapshot_row = {
            "site_id": site_id,
            "name": info["name"],
            "elevation_ft": info.get("elevation_ft"),
            "value": end_value,
            "percentile": end_pct,
        }
        per_station_end_rows.append(snapshot_row)

        if not station_pct.empty:
            pct_df = station_pct[["date", "percentile"]].copy()
            pct_df["site_id"] = site_id
            all_percentiles.append(pct_df)

            value_df = station_pct[["date", "value", "baseline_median"]].copy()
            value_df["site_id"] = site_id
            all_values.append(value_df)

    snapshot_df = pd.DataFrame(per_station_end_rows)
    snapshot_df["has_data"] = snapshot_df["percentile"].notna()

    total_sites = len(selected)
    stacked = pd.concat(all_percentiles, ignore_index=True) if all_percentiles else pd.DataFrame()
    state_series = build_state_series(
        stacked,
        season_start=inputs.season_start,
        season_end=inputs.season_end,
        total_sites=total_sites,
    )

    stacked_values = pd.concat(all_values, ignore_index=True) if all_values else pd.DataFrame()
    if stacked_values.empty:
        value_medians = pd.Series(dtype="float64")
        baseline_medians = pd.Series(dtype="float64")
    else:
        by_date = stacked_values.groupby(stacked_values["date"].dt.date)
        value_medians = by_date["value"].median()
        baseline_medians = by_date["baseline_median"].median()

    state_series["median_value"] = state_series["date"].map(
        lambda d: float(value_medians.get(d, float("nan")))
    )
    state_series["median_baseline_value"] = state_series["date"].map(
        lambda d: float(baseline_medians.get(d, float("nan")))
    )

    end_day = datetime.strptime(inputs.season_end, "%Y-%m-%d").date()
    end_row = state_series.loc[state_series["date"] == end_day]
    statewide_end_pct = float(end_row.iloc[0]["median_percentile"]) if not end_row.empty else float("nan")
    end_used = int(end_row.iloc[0]["stations_used"]) if not end_row.empty else 0
    end_missing = int(end_row.iloc[0]["stations_missing"]) if not end_row.empty else total_sites
    end_cov = float(end_row.iloc[0]["pct_with_data"]) if not end_row.empty else 0.0

    ranked = snapshot_df.sort_values("percentile", ascending=False, na_position="last")
    top_10 = ranked.head(10)
    bottom_10 = ranked.dropna(subset=["percentile"]).tail(10).sort_values("percentile", ascending=True)
    missing = snapshot_df.loc[snapshot_df["percentile"].isna()].sort_values("name").head(25)

    def _fmt(value, digits=1):
        if pd.isna(value):
            return "NA"
        return f"{float(value):.{digits}f}"

    facts = {
        "state": inputs.awdb_state,
        "metric_key": inputs.metric_key,
        "metric_label": _normalized_metric_column(inputs.metric_key),
        "season_start": inputs.season_start,
        "season_end": inputs.season_end,
        "baseline_years": f"{inputs.baseline_start_year}-{inputs.baseline_end_year}",
        "coverage_mode": inputs.coverage_mode,
        "representative_n": inputs.representative_n,
        "elevation_min_ft": inputs.elevation_min_ft,
        "elevation_max_ft": inputs.elevation_max_ft,
        "include_unknown_elevation": inputs.include_unknown_elevation,
        "station_count_total": int(total_sites),
        "end_date": inputs.season_end,
        "end_statewide_median_percentile": _fmt(statewide_end_pct),
        "end_stations_used": int(end_used),
        "end_stations_missing": int(end_missing),
        "end_pct_with_data": _fmt(end_cov),
        "top_stations": [
            {
                "site_id": row.site_id,
                "name": row.name,
                "elevation_ft": row.elevation_ft,
                "percentile": _fmt(row.percentile),
            }
            for row in top_10.itertuples(index=False)
            if pd.notna(row.percentile)
        ],
        "bottom_stations": [
            {
                "site_id": row.site_id,
                "name": row.name,
                "elevation_ft": row.elevation_ft,
                "percentile": _fmt(row.percentile),
            }
            for row in bottom_10.itertuples(index=False)
            if pd.notna(row.percentile)
        ],
        "missing_station_examples": [
            {"site_id": row.site_id, "name": row.name}
            for row in missing.itertuples(index=False)
        ],
    }

    return StateReport(
        inputs=inputs,
        state_series=state_series,
        station_snapshot=snapshot_df,
        narrative_facts=facts,
    )
