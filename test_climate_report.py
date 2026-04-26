import unittest
from datetime import date

import pandas as pd

import climate_report
import narrative


class ClimateReportTest(unittest.TestCase):
    def test_month_day_key_maps_feb_29_to_feb_28(self):
        self.assertEqual(climate_report.month_day_key(date(2020, 2, 29)), "02-28")
        self.assertEqual(climate_report.month_day_key(date(2021, 2, 28)), "02-28")

    def test_percentile_from_baseline_is_inclusive(self):
        pct = climate_report.percentile_from_baseline(10.0, [0.0, 10.0, 20.0])
        self.assertAlmostEqual(pct, (2 / 3) * 100.0, places=6)

    def test_compute_station_percentiles_uses_feb_28_baseline_for_feb_29(self):
        baseline_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["1991-02-28", "1992-02-28"]),
                "swe_in": [10.0, 20.0],
            }
        )
        current_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-02-29"]),
                "swe_in": [15.0],
            }
        )
        out = climate_report.compute_station_percentiles(baseline_df, current_df, "swe")
        self.assertEqual(len(out), 1)
        self.assertAlmostEqual(float(out.iloc[0]["percentile"]), 50.0, places=6)

    def test_build_state_series_median_and_missingness(self):
        percentiles = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2026-04-01", "2026-04-01", "2026-04-02"]
                ),
                "percentile": [10.0, 90.0, 30.0],
                "site_id": ["A", "B", "A"],
            }
        )
        series = climate_report.build_state_series(
            percentiles,
            season_start="2026-04-01",
            season_end="2026-04-02",
            total_sites=2,
        )
        self.assertEqual(len(series), 2)
        self.assertAlmostEqual(float(series.iloc[0]["median_percentile"]), 50.0, places=6)
        self.assertEqual(int(series.iloc[0]["stations_used"]), 2)
        self.assertEqual(int(series.iloc[0]["stations_missing"]), 0)

        self.assertAlmostEqual(float(series.iloc[1]["median_percentile"]), 30.0, places=6)
        self.assertEqual(int(series.iloc[1]["stations_used"]), 1)
        self.assertEqual(int(series.iloc[1]["stations_missing"]), 1)

    def test_select_representative_sites_is_deterministic(self):
        sites = [
            (str(i), {"elevation_ft": i * 100, "name": f"S{i}"})
            for i in range(1, 10)
        ]
        selected = climate_report.select_representative_sites(sites, total_n=6)
        self.assertEqual([site_id for site_id, _ in selected], ["1", "2", "4", "5", "7", "8"])

    def test_filter_sites_by_elevation_bounds(self):
        sites = [
            ("A", {"elevation_ft": 1000, "name": "A"}),
            ("B", {"elevation_ft": 5000, "name": "B"}),
            ("C", {"elevation_ft": None, "name": "C"}),
            ("D", {"elevation_ft": 8000, "name": "D"}),
        ]

        out = climate_report.filter_sites_by_elevation(sites, min_ft=2000, max_ft=7000)
        self.assertEqual([site_id for site_id, _ in out], ["B"])

        out = climate_report.filter_sites_by_elevation(sites, min_ft=2000, include_unknown=True)
        self.assertEqual([site_id for site_id, _ in out], ["B", "C", "D"])


class NarrativeValidationTest(unittest.TestCase):
    def test_narrative_validation_rejects_unexpected_numbers(self):
        facts = {
            "state": "UT",
            "baseline_years": "1991-2020",
            "end_date": "2026-04-22",
            "station_count_total": 45,
            "end_stations_used": 40,
            "end_pct_with_data": "88.9",
            "end_statewide_median_percentile": "55.0",
        }
        ok_narrative = {
            "headline": "UT summary for 2026-04-22 at 55.0th percentile",
            "bullets": [
                "Baseline is 1991-2020.",
                "Coverage 40 of 45 stations (88.9%).",
                "Statewide median is 55.0th percentile.",
            ],
            "disclaimer": "Grounded in SNOTEL station observations; not a forecast.",
        }
        ok, reason = narrative.validate_narrative_json(ok_narrative, facts)
        self.assertTrue(ok, reason)

        bad_narrative = dict(ok_narrative)
        bad_narrative["headline"] = "UT summary: 56.0th percentile"
        ok, reason = narrative.validate_narrative_json(bad_narrative, facts)
        self.assertFalse(ok)
