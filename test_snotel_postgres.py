import types
import unittest
from unittest import mock

import snotel_postgres


class _FakeCursor:
    def __init__(self):
        self.statements = []
        self._last_select = None

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if isinstance(sql, str) and sql.strip().upper().startswith("SELECT TO_REGCLASS"):
            self._last_select = (sql, params)

    def executemany(self, sql, seq):
        self.statements.append((sql, list(seq)))

    def fetchone(self):
        # Always behave as if the relation doesn't exist yet.
        if self._last_select:
            return (None,)
        return None


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class PostgresSchemaTest(unittest.TestCase):
    def test_ensure_schema_creates_parent_and_partitions(self):
        conn = _FakeConn()
        driver = types.SimpleNamespace(name="psycopg2")
        snotel_postgres.ensure_snotel_partitioned_schema(
            conn,
            driver,
            schema="snotel",
            table="daily_observations",
            hash_partitions=2,
        )

        create_table_sql = [
            stmt for (stmt, _params) in conn.cursor_obj.statements if "CREATE TABLE" in str(stmt)
        ]
        # parent (1) + per-hash (1 hash + 2 list children) * 2 = 7 total CREATE TABLEs
        self.assertEqual(len(create_table_sql), 7)
        self.assertGreaterEqual(conn.commits, 1)

    def test_hash_partitions_must_be_positive(self):
        conn = _FakeConn()
        driver = types.SimpleNamespace(name="psycopg2")
        with self.assertRaises(ValueError):
            snotel_postgres.ensure_snotel_partitioned_schema(
                conn, driver, hash_partitions=0
            )


class PostgresConnectTest(unittest.TestCase):
    def test_connect_postgres_requires_dsn(self):
        def _fake_driver():
            return snotel_postgres.PostgresDriver(
                name="psycopg2",
                module=types.SimpleNamespace(connect=lambda _dsn: object()),
            )

        with mock.patch("snotel_postgres._load_postgres_driver", _fake_driver), mock.patch.dict(
            "os.environ", {}, clear=True
        ):
            with self.assertRaises(ValueError):
                snotel_postgres.connect_postgres(dsn=None)

    def test_load_driver_skips_incomplete_psycopg_namespace_and_uses_psycopg2(self):
        fake_psycopg = types.SimpleNamespace()
        fake_psycopg2 = types.SimpleNamespace(connect=lambda dsn: ("ok", dsn))

        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "psycopg":
                return fake_psycopg
            if name == "psycopg2":
                return fake_psycopg2
            return real_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            driver = snotel_postgres._load_postgres_driver()

        self.assertEqual(driver.name, "psycopg2")
        self.assertIs(driver.module, fake_psycopg2)

    def test_load_driver_raises_for_incomplete_driver_install(self):
        fake_psycopg = types.SimpleNamespace()
        fake_psycopg2 = types.SimpleNamespace()

        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "psycopg":
                return fake_psycopg
            if name == "psycopg2":
                return fake_psycopg2
            return real_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(snotel_postgres.PostgresDependencyError):
                snotel_postgres._load_postgres_driver()

    def test_connect_postgres_falls_back_to_psql(self):
        with mock.patch(
            "snotel_postgres._load_postgres_driver",
            side_effect=snotel_postgres.PostgresDependencyError("broken"),
        ), mock.patch(
            "snotel_postgres._find_psql_executable",
            return_value=r"C:\Program Files\PostgreSQL\17\bin\psql.exe",
        ):
            conn, driver = snotel_postgres.connect_postgres(
                dsn="postgresql://snowpack:snowpack@localhost:5432/snowpack"
            )

        self.assertEqual(driver.name, "psql")
        self.assertEqual(conn.psql_path, r"C:\Program Files\PostgreSQL\17\bin\psql.exe")
