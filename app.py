from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

import climate_report
import narrative
import os


st.set_page_config(page_title="Snowpack Tracker", layout="wide")


def _load_dotenv_simple(*paths: str) -> None:
    # Minimal .env loader (no external deps). Intended for local dev only.
    for path in paths:
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'").strip('"')
                    if key and key not in os.environ:
                        os.environ[key] = value
        except OSError:
            continue


_load_dotenv_simple(".env.local", ".env")


def _parse_iso(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


@st.cache_data(show_spinner=False)
def _compute_report_cached(inputs_dict: dict) -> climate_report.StateReport:
    inputs = climate_report.StateReportInputs(**inputs_dict)
    return climate_report.compute_state_report(inputs)


def main():
    st.title("Snowpack Tracker - State-level SNOTEL Percentiles")
    st.caption("Grounded in SNOTEL station observations; not a forecast.")

    default_start, default_end = climate_report.default_season_dates()

    with st.sidebar.form("controls"):
        st.subheader("Controls")
        awdb_state = st.selectbox("State (AWDB)", climate_report.list_awdb_states(), index=0)
        metric_key = st.selectbox("Metric", ["swe", "snow_depth", "precip", "tavg"], index=0)

        col1, col2 = st.columns(2)
        with col1:
            baseline_start_year = st.number_input(
                "Baseline start year", min_value=1900, max_value=2100, value=1991, step=1
            )
        with col2:
            baseline_end_year = st.number_input(
                "Baseline end year", min_value=1900, max_value=2100, value=2020, step=1
            )

        season_start = st.date_input("Season start", value=_parse_iso(default_start))
        season_end = st.date_input("Season end", value=_parse_iso(default_end))

        st.subheader("Elevation filter")
        enable_elevation_filter = st.checkbox("Filter stations by elevation", value=False)
        include_unknown_elevation = False
        elevation_min_ft = 0
        elevation_max_ft = 0
        if enable_elevation_filter:
            col_e1, col_e2 = st.columns(2)
            with col_e1:
                elevation_min_ft = st.number_input(
                    "Min elevation (ft)",
                    min_value=0,
                    max_value=20000,
                    value=0,
                    step=250,
                    help="0 disables the minimum bound.",
                )
            with col_e2:
                elevation_max_ft = st.number_input(
                    "Max elevation (ft)",
                    min_value=0,
                    max_value=20000,
                    value=0,
                    step=250,
                    help="0 disables the maximum bound.",
                )
            include_unknown_elevation = st.checkbox(
                "Include stations with unknown elevation", value=False
            )

        coverage_mode_label = st.selectbox(
            "Coverage mode",
            ["Representative stations", "All stations"],
            index=0,
        )
        representative_n = st.slider("Representative station count", min_value=15, max_value=120, value=45, step=5)

        st.subheader("OpenAI")
        openai_model = st.text_input(
            "OpenAI model",
            value=os.environ.get("OPENAI_MODEL", "gpt-5-mini"),
            help="Example: gpt-5-mini",
        ).strip()

        use_ai = st.checkbox("Use AI narrative (grounded JSON)", value=True)
        submitted = st.form_submit_button("Generate report")

    if not submitted:
        st.info("Set controls and click **Generate report**.")
        return

    if baseline_start_year > baseline_end_year:
        st.error("Baseline start year must be <= baseline end year.")
        return
    if season_start > season_end:
        st.error("Season start must be <= season end.")
        return
    if enable_elevation_filter and elevation_min_ft and elevation_max_ft and elevation_min_ft > elevation_max_ft:
        st.error("Min elevation must be <= max elevation (or set one of them to 0 to disable).")
        return

    coverage_mode = "all" if coverage_mode_label == "All stations" else "representative"
    if coverage_mode == "all":
        st.warning("All-stations mode can be slow. Representative mode is the default for faster iteration.")

    progress_bar = st.progress(0)
    progress_text = st.empty()

    def _progress(current: int, total: int):
        frac = current / max(total, 1)
        progress_bar.progress(min(max(frac, 0.0), 1.0))
        progress_text.caption(f"Fetching + computing station {current}/{total}…")

    inputs = climate_report.StateReportInputs(
        awdb_state=awdb_state,
        metric_key=metric_key,
        season_start=season_start.isoformat(),
        season_end=season_end.isoformat(),
        baseline_start_year=int(baseline_start_year),
        baseline_end_year=int(baseline_end_year),
        coverage_mode=coverage_mode,
        representative_n=int(representative_n),
        elevation_min_ft=(int(elevation_min_ft) if enable_elevation_filter and elevation_min_ft else None),
        elevation_max_ft=(int(elevation_max_ft) if enable_elevation_filter and elevation_max_ft else None),
        include_unknown_elevation=bool(enable_elevation_filter and include_unknown_elevation),
    )

    with st.spinner("Computing percentiles…"):
        report = climate_report.compute_state_report(inputs, progress=_progress)

    progress_bar.empty()
    progress_text.empty()

    series = report.state_series.copy()
    series["date"] = pd.to_datetime(series["date"])
    series = series.set_index("date")

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.subheader("Statewide median percentile over time")
        st.line_chart(series["median_percentile"])
        st.caption("Median of per-station percentiles (same month-day vs baseline years).")

        st.subheader("Statewide median metric vs baseline median")
        metric_df = series[["median_value", "median_baseline_value"]].rename(
            columns={
                "median_value": "This season (median across stations)",
                "median_baseline_value": "Baseline median (same month-day)",
            }
        )
        st.line_chart(metric_df)
        st.caption("Compares the median station value this season vs the median baseline value for the same month-day.")

    with col_b:
        st.subheader("Coverage (end date)")
        facts = report.narrative_facts
        st.metric("Statewide median percentile", f"{facts.get('end_statewide_median_percentile', 'NA')}th")
        st.metric("Stations reporting", f"{facts.get('end_stations_used', 0)}/{facts.get('station_count_total', 0)}")
        st.metric("Percent with data", f"{facts.get('end_pct_with_data', 'NA')}%")
        st.caption(f"Baseline: {facts.get('baseline_years')} | End date: {facts.get('end_date')}")

    st.subheader("Top / bottom stations (end date)")
    snapshot = report.station_snapshot.copy()
    snapshot["percentile"] = snapshot["percentile"].round(1)
    top = snapshot.sort_values("percentile", ascending=False, na_position="last").head(10).assign(group="Top 10")
    bottom = (
        snapshot.dropna(subset=["percentile"])
        .sort_values("percentile", ascending=True)
        .head(10)
        .assign(group="Bottom 10")
    )
    station_table = pd.concat([top, bottom], ignore_index=True)[
        ["group", "site_id", "name", "elevation_ft", "value", "percentile", "has_data"]
    ]
    st.dataframe(station_table, use_container_width=True, hide_index=True)

    st.subheader("Narrative (grounded)")
    if use_ai and not (narrative.os.environ.get(narrative.OPENAI_API_KEY_ENV, "").strip()):
        st.warning("OPENAI_API_KEY is not set; using template narrative.")
    story = narrative.generate_grounded_narrative(
        report.narrative_facts, use_ai=use_ai, model=openai_model or None
    )
    st.markdown(f"### {story.get('headline','')}")
    for bullet in story.get("bullets", []):
        st.write(f"- {bullet}")
    st.caption(story.get("disclaimer", ""))
    st.expander("Narrative JSON").json(story)

    st.subheader("Downloads")
    series_out = report.state_series.copy()
    series_out["date"] = series_out["date"].astype(str)
    st.download_button(
        "Download statewide series CSV",
        data=series_out.to_csv(index=False),
        file_name=f"state_series_{awdb_state}_{metric_key}_{inputs.season_start}_{inputs.season_end}.csv",
        mime="text/csv",
    )
    st.download_button(
        "Download station snapshot CSV",
        data=report.station_snapshot.to_csv(index=False),
        file_name=f"station_snapshot_{awdb_state}_{metric_key}_{inputs.season_end}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
