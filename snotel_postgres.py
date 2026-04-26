import os
import shutil
import site
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


class PostgresDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class PostgresDriver:
    name: str
    module: object


class _PsqlCursor:
    def __init__(self, connection):
        self._connection = connection
        self._rows = []

    def execute(self, sql, params=None):
        rendered_sql = _render_sql(sql, params)
        self._rows = self._connection._run_sql(rendered_sql)

    def executemany(self, sql, seq):
        for params in seq:
            self.execute(sql, params)

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]


class _PsqlConnection:
    def __init__(self, dsn, psql_path):
        self.dsn = dsn
        self.psql_path = psql_path

    def cursor(self):
        return _PsqlCursor(self)

    def commit(self):
        return None

    def rollback(self):
        return None

    def _run_sql(self, sql):
        command = [self.psql_path, self.dsn, "-X", "-A", "-t", "-F", "\t", "-c", sql]
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        rows = []
        for line in completed.stdout.splitlines():
            if not line.strip():
                continue
            rows.append(tuple(_parse_psql_value(part) for part in line.split("\t")))
        return rows


def _add_local_dependency_paths():
    candidate_paths = []
    base_dir = Path(__file__).resolve().parent
    for dependency_dir in (".pgdeps", ".deps"):
        candidate_paths.append(base_dir / dependency_dir)

    try:
        candidate_paths.append(Path(site.getusersitepackages()))
    except Exception:
        pass

    for candidate in candidate_paths:
        candidate_str = str(candidate)
        if candidate_str in sys.path:
            continue
        try:
            if candidate.is_dir():
                sys.path.insert(0, candidate_str)
        except PermissionError:
            sys.path.insert(0, candidate_str)


def _find_psql_executable():
    candidates = [
        shutil.which("psql"),
        r"C:\Program Files\PostgreSQL\17\bin\psql.exe",
        r"C:\Program Files\PostgreSQL\16\bin\psql.exe",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _quote_sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _render_sql(sql, params):
    if not params:
        return sql
    rendered = sql
    for value in params:
        rendered = rendered.replace("%s", _quote_sql_literal(value), 1)
    return rendered


def _parse_psql_value(value):
    if value == "":
        return None
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return int(value)
    return value


def _load_postgres_driver():
    _add_local_dependency_paths()

    try:
        import psycopg  # type: ignore

        if hasattr(psycopg, "connect"):
            return PostgresDriver(name="psycopg", module=psycopg)
    except ImportError:
        pass

    try:
        import psycopg2  # type: ignore

        if hasattr(psycopg2, "connect"):
            return PostgresDriver(name="psycopg2", module=psycopg2)
    except ImportError as exc:
        raise PostgresDependencyError(
            "Missing Postgres driver. Install one of:\n"
            "  - pip install 'psycopg[binary]'\n"
            "  - pip install psycopg2-binary\n"
        ) from exc

    raise PostgresDependencyError(
        "Found a Postgres driver package, but it is incomplete and does not expose connect(). "
        "Reinstall one of:\n"
        "  - pip install 'psycopg[binary]'\n"
        "  - pip install psycopg2-binary\n"
    )


def connect_postgres(dsn=None):
    if dsn is None:
        dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN")
    if not dsn:
        raise ValueError("Missing Postgres DSN. Set DATABASE_URL (recommended) or PG_DSN.")

    try:
        driver = _load_postgres_driver()
        conn = driver.module.connect(dsn)
        return conn, driver
    except PostgresDependencyError:
        psql_path = _find_psql_executable()
        if not psql_path:
            raise
        conn = _PsqlConnection(dsn, psql_path)
        return conn, PostgresDriver(name="psql", module=conn)


def _to_regclass(cur, relation):
    cur.execute("SELECT to_regclass(%s)", (relation,))
    row = cur.fetchone()
    return row[0] if row else None


def ensure_snotel_partitioned_schema(
    conn,
    driver,
    *,
    schema="snotel",
    table="daily_observations",
    hash_partitions=32,
):
    if hash_partitions < 1:
        raise ValueError("hash_partitions must be >= 1")

    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    parent_rel = f"{schema}.{table}"
    if not _to_regclass(cur, parent_rel):
        cur.execute(
            f"""
            CREATE TABLE {parent_rel} (
              site_id INTEGER NOT NULL,
              awdb_state TEXT NOT NULL,
              site_name TEXT,
              elevation_ft INTEGER,
              obs_date DATE NOT NULL,
              season_bucket SMALLINT NOT NULL,
              swe_in REAL,
              snow_depth_in REAL,
              precip_in REAL,
              tmax_f REAL,
              tmin_f REAL,
              tavg_f REAL,
              ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (site_id, season_bucket, obs_date),
              CHECK (season_bucket IN (0, 1))
            ) PARTITION BY HASH (site_id)
            """
        )

    # Partitioned indexes (Postgres will create per-partition indexes automatically).
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table}_site_date_idx ON {parent_rel} (site_id, obs_date)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table}_date_idx ON {parent_rel} (obs_date)"
    )

    for remainder in range(hash_partitions):
        hash_name = f"{table}_h{remainder:03d}"
        hash_rel = f"{schema}.{hash_name}"
        if not _to_regclass(cur, hash_rel):
            cur.execute(
                f"""
                CREATE TABLE {hash_rel}
                  PARTITION OF {parent_rel}
                  FOR VALUES WITH (MODULUS {hash_partitions}, REMAINDER {remainder})
                  PARTITION BY LIST (season_bucket)
                """
            )

        njf_rel = f"{schema}.{hash_name}_njf"
        if not _to_regclass(cur, njf_rel):
            cur.execute(
                f"""
                CREATE TABLE {njf_rel}
                  PARTITION OF {hash_rel}
                  FOR VALUES IN (0)
                """
            )

        fma_rel = f"{schema}.{hash_name}_fma"
        if not _to_regclass(cur, fma_rel):
            cur.execute(
                f"""
                CREATE TABLE {fma_rel}
                  PARTITION OF {hash_rel}
                  FOR VALUES IN (1)
                """
            )

    conn.commit()
