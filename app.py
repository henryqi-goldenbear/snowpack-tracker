from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

import climate_report
import narrative


st.set_page_config(page_title="Snowpack Tracker", layout="wide")


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

        coverage_mode_label = st.selectbox(
            "Coverage mode",
            ["Representative stations", "All stations"],
            index=0,
        )
        representative_n = st.slider("Representative station count", min_value=15, max_value=120, value=45, step=5)

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
    story = narrative.generate_grounded_narrative(report.narrative_facts, use_ai=use_ai)
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
