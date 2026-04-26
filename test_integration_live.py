import os
import time
import unittest
from datetime import date
from urllib.request import urlopen

import snotel_postgres


def _env_flag(name):
    value = os.environ.get(name, "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _has_postgres_driver():
    try:
        snotel_postgres._load_postgres_driver()
        return True
    except Exception:
        return bool(snotel_postgres._find_psql_executable())


@unittest.skipUnless(_env_flag("RUN_LIVE_NRCS"), "Set RUN_LIVE_NRCS=1 to run live NRCS smoke test.")
class LiveNrcsTest(unittest.TestCase):
    def test_live_nrcs_pull_contains_csv(self):
        url = (
            "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
            "customSingleStationReport/daily/395:OR:SNTL%7Cid=%22%22%7Cname/"
            "2000-01-01,2000-01-05/WTEQ::value,SNWD::value"
        )
        text = urlopen(url, timeout=30).read(50_000).decode("utf-8", errors="replace")
        self.assertIn("Date,", text)
        self.assertIn("2000-01-0", text)


@unittest.skipUnless(
    os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN"),
    "Set DATABASE_URL (or PG_DSN) to run Postgres integration test.",
)
@unittest.skipUnless(
    _has_postgres_driver(),
    "Install a Postgres driver (psycopg or psycopg2) to run Postgres integration test.",
)
@unittest.skipUnless(
    _env_flag("RUN_LIVE_POSTGRES"),
    "Set RUN_LIVE_POSTGRES=1 to run Postgres integration test (creates/drops a test schema).",
)
class LivePostgresTest(unittest.TestCase):
    def test_schema_partitions_exist_in_real_postgres(self):
        conn, driver = snotel_postgres.connect_postgres(dsn=None)
        schema = f"snotel_test_{int(time.time())}"
        table = "daily_observations"
        try:
            snotel_postgres.ensure_snotel_partitioned_schema(
                conn,
                driver,
                schema=schema,
                table=table,
                hash_partitions=4,
            )
            cur = conn.cursor()
            parent_rel = f"{schema}.{table}"
            cur.execute("SELECT to_regclass(%s)", (parent_rel,))
            self.assertIsNotNone(cur.fetchone()[0])

            # Check a couple of partitions exist.
            for remainder in (0, 3):
                hash_rel = f"{schema}.{table}_h{remainder:03d}"
                cur.execute("SELECT to_regclass(%s)", (hash_rel,))
                self.assertIsNotNone(cur.fetchone()[0])

                for bucket_suffix in ("njf", "fma"):
                    child_rel = f"{hash_rel}_{bucket_suffix}"
                    cur.execute("SELECT to_regclass(%s)", (child_rel,))
                    self.assertIsNotNone(cur.fetchone()[0])
        finally:
            cur = conn.cursor()
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.commit()
