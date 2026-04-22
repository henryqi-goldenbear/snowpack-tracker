import unittest
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import snotel
from snotel_sites import SITE_ID_TO_INFO


class SnotelCliTest(unittest.TestCase):
    def test_site_info_mapping_is_populated(self):
        self.assertGreater(len(SITE_ID_TO_INFO), 0)

    def test_site_lookup_by_id_and_exact_name(self):
        self.assertEqual(snotel.get_site_info("395"), SITE_ID_TO_INFO["395"])
        self.assertEqual(snotel.get_site_name("395"), "Chemult Alternate")
        self.assertEqual(snotel.get_site_state("395"), "south cascades")
        self.assertEqual(snotel.get_site_awdb_state("395"), "OR")
        self.assertEqual(snotel.get_site_elevation_ft("395"), 4850)
        self.assertEqual(snotel.get_site_info("Chemult Alternate"), SITE_ID_TO_INFO["395"])
        self.assertEqual(snotel.get_site_name("chemult alternate"), "Chemult Alternate")

    def test_default_site_is_palisades_tahoe(self):
        self.assertEqual(snotel.get_default_site_id(), "784")
        self.assertEqual(snotel.get_site_name("Palisades Tahoe"), "Palisades Tahoe")
        self.assertEqual(snotel.get_site_state("Palisades Tahoe"), "sierra nevada")
        self.assertEqual(snotel.get_site_elevation_ft("Palisades Tahoe"), 8010)

    def test_non_target_states_fall_back_to_other_region(self):
        self.assertEqual(snotel.get_site_state("201"), "other")
        self.assertEqual(snotel.get_site_awdb_state("201"), "AK")

    def test_unknown_site_raises_key_error(self):
        with self.assertRaises(KeyError):
            snotel.get_site_info("999999")

        with self.assertRaises(KeyError):
            snotel.get_site_info("Not A Real Station")

    def test_parse_cli_args_with_explicit_site(self):
        site_id, site_info, start_date, end_date = snotel.parse_cli_args(
            ["395", "2026-04-01", "2026-04-21"]
        )
        self.assertEqual(site_id, "395")
        self.assertEqual(site_info["name"], "Chemult Alternate")
        self.assertEqual(start_date, "2026-04-01")
        self.assertEqual(end_date, "2026-04-21")

    def test_parse_cli_args_uses_popup_ai_when_first_arg_is_date(self):
        with patch(
            "snotel.resolve_site_from_popup_search",
            return_value=("784", SITE_ID_TO_INFO["784"]),
        ):
            site_id, site_info, start_date, end_date = snotel.parse_cli_args(
                ["2026-04-01", "2026-04-21"]
            )
        self.assertEqual(site_id, "784")
        self.assertEqual(site_info["name"], "Palisades Tahoe")
        self.assertEqual(start_date, "2026-04-01")
        self.assertEqual(end_date, "2026-04-21")

    def test_parse_cli_args_rejects_bad_dates_and_missing_args(self):
        with self.assertRaises(ValueError):
            snotel.parse_cli_args(["395", "2026/04/01", "2026-04-21"])

        with self.assertRaises(ValueError):
            snotel.parse_cli_args(["395", "2026-04-21"])

        with self.assertRaises(ValueError):
            snotel.parse_cli_args(["395", "2026-04-21", "2026-04-01"])

    def test_output_filename_is_deterministic(self):
        self.assertEqual(
            snotel.build_output_filename("395", "2026-04-01", "2026-04-21"),
            "snotel_395_2026-04-01_2026-04-21.html",
        )

    def test_format_site_heading_includes_elevation(self):
        heading = snotel.format_site_heading(SITE_ID_TO_INFO["784"])
        self.assertEqual(heading, "Palisades Tahoe (8010 ft)")

    def test_resolve_site_from_ai_response_prefers_site_id(self):
        response = {
            "choices": [
                {
                    "message": {
                        "content": '{"site_id":"395","site_name":"Chemult Alternate","rationale":"best match"}'
                    }
                }
            ]
        }
        site_id, site_info = snotel.resolve_site_from_ai_response(response)
        self.assertEqual(site_id, "395")
        self.assertEqual(site_info["name"], "Chemult Alternate")

    def test_resolve_site_from_ai_response_falls_back_to_site_name(self):
        response = {
            "choices": [
                {
                    "message": {
                        "content": '{"site_id":"","site_name":"Palisades Tahoe","rationale":"mentions Tahoe"}'
                    }
                }
            ]
        }
        site_id, site_info = snotel.resolve_site_from_ai_response(response)
        self.assertEqual(site_id, "784")
        self.assertEqual(site_info["name"], "Palisades Tahoe")

    def test_resolve_site_with_ai_requires_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ValueError):
                snotel.resolve_site_with_ai("Tahoe snow")

    def test_resolve_site_from_popup_search_uses_prompt_and_ai(self):
        with patch("snotel.prompt_for_site_query", return_value="Tahoe snow"), patch(
            "snotel.resolve_site_with_ai",
            return_value=("784", SITE_ID_TO_INFO["784"]),
        ) as resolver:
            site_id, site_info = snotel.resolve_site_from_popup_search()

        self.assertEqual(site_id, "784")
        self.assertEqual(site_info["name"], "Palisades Tahoe")
        resolver.assert_called_once_with("Tahoe snow")

    def test_prompt_handler_serves_form_and_returns_submitted_query(self):
        query_queue = snotel.Queue(maxsize=1)
        server = snotel.ThreadingHTTPServer(
            ("127.0.0.1", 0), snotel._make_prompt_handler(query_queue)
        )
        server.daemon_threads = True
        try:
            with patch("snotel.webbrowser.open", return_value=True):
                import threading

                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                base_url = f"http://127.0.0.1:{server.server_port}"

                with urlopen(base_url + "/") as response:
                    html_text = response.read().decode("utf-8")
                self.assertIn("Find a SNOTEL site", html_text)

                data = urlencode({"query": "Tahoe snow"}).encode("utf-8")
                request = Request(base_url + "/submit", data=data, method="POST")
                with urlopen(request) as response:
                    success_text = response.read().decode("utf-8")
                self.assertIn("Search received", success_text)
                self.assertEqual(query_queue.get(timeout=1), "Tahoe snow")
        finally:
            server.shutdown()
            server.server_close()

    def test_format_snotel_for_display_does_not_truncate_when_rows_is_none(self):
        dates = pd.date_range("2026-04-01", periods=12, freq="D")
        df = pd.DataFrame(
            {
                "Date": dates,
                "Snow Water Equivalent (in) Start of Day Values": range(12),
                "Snow Depth (in) Start of Day Values": range(12),
                "Precipitation Accumulation (in) Start of Day Values": range(12),
                "Air Temperature Maximum (degF)": range(12),
                "Air Temperature Minimum (degF)": range(12),
                "Air Temperature Average (degF)": range(12),
            }
        )

        display_df = snotel.format_snotel_for_display(df)
        self.assertEqual(len(display_df), 12)
        self.assertEqual(display_df.iloc[0]["date"], "2026-04-12")
        self.assertEqual(display_df.iloc[-1]["date"], "2026-04-01")

    def test_run_cli_writes_html_and_opens_browser(self):
        dates = pd.date_range("2026-04-01", periods=12, freq="D")
        df = pd.DataFrame(
            {
                "Date": dates,
                "Snow Water Equivalent (in) Start of Day Values": range(12),
                "Snow Depth (in) Start of Day Values": range(12),
                "Precipitation Accumulation (in) Start of Day Values": range(12),
                "Air Temperature Maximum (degF)": range(12),
                "Air Temperature Minimum (degF)": range(12),
                "Air Temperature Average (degF)": range(12),
            }
        )

        scratch_dir = Path("test_artifacts")
        scratch_dir.mkdir(exist_ok=True)
        original_cwd = Path.cwd()
        try:
            os.chdir(scratch_dir)
            with patch("snotel.get_snotel_data", return_value=df), patch(
                "snotel.webbrowser.open", return_value=True
            ) as browser_open:
                output_file = snotel.run_cli(
                    ["395", "2026-04-01", "2026-04-21"],
                    open_browser=True,
                )
                self.assertTrue(output_file.exists())
                self.assertEqual(
                    output_file.name,
                    "snotel_395_2026-04-01_2026-04-21.html",
                )
                html_text = output_file.read_text(encoding="utf-8")
                self.assertIn("Chemult Alternate (4850 ft)", html_text)
                self.assertIn("2026-04-12", html_text)
                self.assertIn("2026-04-01", html_text)
                browser_open.assert_called_once_with(output_file.resolve().as_uri())
                output_file.unlink()
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    unittest.main()
