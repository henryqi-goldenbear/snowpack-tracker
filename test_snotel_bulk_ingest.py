import sys
import types
import unittest
from datetime import date
from unittest import mock

import snotel_bulk_ingest


class _FakeResponse:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class BulkHelpersTest(unittest.TestCase):
    def test_season_bucket(self):
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 11, 1)), 0)
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 12, 1)), 0)
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 1, 1)), 0)
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 2, 1)), 1)
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 3, 1)), 1)
        self.assertEqual(snotel_bulk_ingest._season_bucket(date(2000, 4, 1)), 1)
        self.assertIsNone(snotel_bulk_ingest._season_bucket(date(2000, 5, 1)))
        self.assertIsNone(snotel_bulk_ingest._season_bucket(date(2000, 10, 1)))

    def test_parse_por_range_from_csv_block(self):
        report = "\n".join(
            [
                "# comment",
                "POR_BEGIN,POR_END",
                "1978-10-01,2004-06-30",
            ]
        )
        begin, end = snotel_bulk_ingest.parse_por_range(report)
        self.assertEqual(begin, date(1978, 10, 1))
        self.assertEqual(end, date(2004, 6, 30))

    def test_parse_por_range_from_comment_lines(self):
        report = "\n".join(
            [
                "# POR_BEGIN: 1981-09-15",
                "# POR_END: 2026-04-25",
            ]
        )
        begin, end = snotel_bulk_ingest.parse_por_range(report)
        self.assertEqual(begin, date(1981, 9, 15))
        self.assertEqual(end, date(2026, 4, 25))

    def test_extract_csv_text_skips_headers(self):
        report = "\n".join(
            [
                "# some header",
                "# more header",
                "Date,Snow Water Equivalent (in) Start of Day Values",
                "2000-01-01,1.0",
            ]
        )
        csv_text = snotel_bulk_ingest._extract_csv_text(report)
        self.assertTrue(csv_text.startswith("Date,"))
        self.assertIn("2000-01-01,1.0", csv_text)


class BulkFetchTest(unittest.TestCase):
    def test_fetch_site_daily_rows_parses_and_filters_season(self):
        report = "\n".join(
            [
                "# header",
                "Date,Snow Water Equivalent (in) Start of Day Values,Snow Depth (in) Start of Day Values,Precipitation Accumulation (in) Start of Day Values,Air Temperature Maximum (degF),Air Temperature Minimum (degF),Air Temperature Average (degF)",
                "2000-01-15,12.5,30,1.0,40,20,30",
                "2000-02-01,13.0,31,1.5,41,21,31",
                "2000-05-01,99,99,99,99,99,99",
                "2000-12-01,-9999,10,,,-999,-999",
            ]
        ).encode("utf-8")

        with mock.patch("snotel_bulk_ingest.urlopen", return_value=_FakeResponse(report)):
            rows = snotel_bulk_ingest.fetch_site_daily_rows(
                site_id="395",
                awdb_state="OR",
                site_name="Chemult Alternate",
                elevation_ft=4850,
                start_date=date(1950, 1, 1),
                end_date=date(2000, 12, 31),
                element_codes=["WTEQ", "SNWD", "PREC", "TMAX", "TMIN", "TAVG"],
                retries=0,
            )

        # 2000-05-01 excluded (out of season); remaining 3 rows.
        self.assertEqual(len(rows), 3)
        jan = [r for r in rows if r[4] == date(2000, 1, 15)][0]
        feb = [r for r in rows if r[4] == date(2000, 2, 1)][0]
        dec = [r for r in rows if r[4] == date(2000, 12, 1)][0]

        self.assertEqual(jan[5], 0)  # season_bucket
        self.assertEqual(feb[5], 1)
        self.assertEqual(dec[5], 0)
        self.assertEqual(jan[6], 12.5)  # swe_in
        self.assertIsNone(dec[6])  # swe_in sentinel removed
        self.assertIsNone(dec[8])  # precip_in blank -> None


class BulkInsertTest(unittest.TestCase):
    def test_insert_rows_psycopg2_uses_execute_values(self):
        called = {}

        def execute_values(cur, sql, rows, template=None, page_size=None):
            called["sql"] = sql
            called["rows"] = list(rows)
            called["template"] = template
            called["page_size"] = page_size

        fake_extras = types.SimpleNamespace(execute_values=execute_values)
        fake_psycopg2 = types.SimpleNamespace(extras=fake_extras)
        with mock.patch.dict(sys.modules, {"psycopg2": fake_psycopg2, "psycopg2.extras": fake_extras}):
            cur = types.SimpleNamespace()
            rows = [(395, "OR", "X", 1, date(2000, 1, 1), 0, 1.0, None, None, None, None, None)]
            inserted = snotel_bulk_ingest._insert_rows(cur, "psycopg2", "snotel.daily_observations", rows)

        self.assertEqual(inserted, 1)
        self.assertIn("INSERT INTO snotel.daily_observations", called["sql"])
        self.assertEqual(len(called["rows"]), 1)

    def test_insert_rows_default_uses_executemany(self):
        calls = {}

        class Cur:
            def executemany(self, sql, rows):
                calls["sql"] = sql
                calls["rows"] = list(rows)

        cur = Cur()
        rows = [(395, "OR", "X", 1, date(2000, 1, 1), 0, 1.0, None, None, None, None, None)]
        inserted = snotel_bulk_ingest._insert_rows(cur, "psycopg", "snotel.daily_observations", rows)
        self.assertEqual(inserted, 1)
        self.assertIn("INSERT INTO snotel.daily_observations", calls["sql"])
        self.assertEqual(len(calls["rows"]), 1)
