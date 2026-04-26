import os
import sys
import unittest
from pathlib import Path


def load_dotenv(path):
    dotenv_path = Path(path)
    if not dotenv_path.exists():
        return
    for raw in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"')
        os.environ.setdefault(key, value)


def main():
    sys.path.insert(0, str(Path(".deps").resolve()))
    load_dotenv(".env")

    if not (os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN")):
        raise SystemExit(
            "Missing DATABASE_URL/PG_DSN. Set it in your environment or in .env, then rerun."
        )

    os.environ["RUN_LIVE_POSTGRES"] = "1"
    suite = unittest.defaultTestLoader.loadTestsFromName("test_integration_live.LivePostgresTest")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()

