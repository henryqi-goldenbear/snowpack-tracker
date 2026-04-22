import json
import os
import re
import sys
import threading
import webbrowser
from datetime import datetime
from difflib import SequenceMatcher
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from queue import Empty, Queue
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

import pandas as pd

from snotel_sites import SITE_ID_TO_INFO


DEFAULT_SITE_NAME = "Palisades Tahoe"
DISPLAY_COLUMNS = {
    "Date": "date",
    "Snow Water Equivalent (in) Start of Day Values": "swe_in",
    "Snow Depth (in) Start of Day Values": "snow_depth_in",
    "Precipitation Accumulation (in) Start of Day Values": "precip_in",
    "Air Temperature Maximum (degF)": "tmax_f",
    "Air Temperature Minimum (degF)": "tmin_f",
    "Air Temperature Average (degF)": "tavg_f",
}
USAGE = (
    "Usage: python snotel.py [site_id_or_exact_name] <start_date> <end_date>\n"
    "If site is omitted, a popup uses Perplexity Sonar to resolve a site from "
    "your natural-language search.\n"
    "Dates must use YYYY-MM-DD."
)
PERPLEXITY_API_URL = "https://api.perplexity.ai/v1/sonar"
PERPLEXITY_MODEL = "sonar"
PERPLEXITY_API_KEY_ENV = "PERPLEXITY_API_KEY"
PROMPT_TIMEOUT_SECONDS = 300


def build_snotel_url(site_id, state, start_date, end_date):
    return (
        "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
        f"customSingleStationReport/daily/{site_id}:{state}:SNTL%7Cid=%22%22%7Cname/"
        f"{start_date},{end_date}/"
        "WTEQ::value,SNWD::value,PREC::value,TMAX::value,TMIN::value,TAVG::value"
    )


def _extract_csv_text(report_text):
    if "#------------------------------------------------- ERROR" in report_text:
        error_lines = [
            line[1:].strip()
            for line in report_text.splitlines()
            if line.startswith("#")
            and "ERROR" not in line
            and "Stations do not exist" in line
        ]
        detail = error_lines[0] if error_lines else "Unknown SNOTEL report error."
        raise ValueError(detail)

    lines = report_text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("Date,"):
            return "\n".join(lines[index:])

    raise ValueError("Could not find the CSV header in the SNOTEL response.")


def get_snotel_data(site_id, state, start_date, end_date):
    url = build_snotel_url(site_id, state, start_date, end_date)

    try:
        with urlopen(url, timeout=30) as response:
            report_text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"SNOTEL request failed with HTTP {exc.code}.") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach the SNOTEL service: {exc.reason}") from exc

    csv_text = _extract_csv_text(report_text)
    return pd.read_csv(StringIO(csv_text), parse_dates=["Date"])


def get_site_info(site_or_name):
    site_key = str(site_or_name).strip()
    if site_key in SITE_ID_TO_INFO:
        return SITE_ID_TO_INFO[site_key]

    normalized_target = site_key.casefold()
    for info in SITE_ID_TO_INFO.values():
        if info["name"].strip().casefold() == normalized_target:
            return info

    raise KeyError(f"No site info mapping found for {site_key}.")


def get_site_id(site_or_name):
    site_key = str(site_or_name).strip()
    if site_key in SITE_ID_TO_INFO:
        return site_key

    normalized_target = site_key.casefold()
    for site_id, info in SITE_ID_TO_INFO.items():
        if info["name"].strip().casefold() == normalized_target:
            return site_id

    raise KeyError(f"No site id mapping found for {site_key}.")


def get_site_name(site_or_name):
    return get_site_info(site_or_name)["name"]


def get_site_state(site_or_name):
    return get_site_info(site_or_name)["state"]


def get_site_awdb_state(site_or_name):
    return get_site_info(site_or_name)["awdb_state"]


def get_site_elevation_ft(site_or_name):
    return get_site_info(site_or_name)["elevation_ft"]


def get_default_site_id():
    return get_site_id(DEFAULT_SITE_NAME)


def _normalize_search_text(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value).casefold()).strip()


def _site_search_score(user_query, site_id, site_info):
    normalized_query = _normalize_search_text(user_query)
    if not normalized_query:
        return 0.0

    normalized_name = _normalize_search_text(site_info["name"])
    query_tokens = set(normalized_query.split())
    name_tokens = set(normalized_name.split())
    token_overlap = len(query_tokens & name_tokens) / max(len(query_tokens), 1)
    ratio = SequenceMatcher(None, normalized_query, normalized_name).ratio()
    if normalized_query == str(site_id):
        return 2.0
    if normalized_query == normalized_name:
        return 1.5
    if normalized_query in normalized_name:
        return 1.0 + token_overlap
    return ratio + token_overlap


def get_site_candidates(user_query, limit=25):
    ranked = sorted(
        SITE_ID_TO_INFO.items(),
        key=lambda item: (-_site_search_score(user_query, item[0], item[1]), item[1]["name"]),
    )
    return ranked[:limit]


def build_candidate_catalog(user_query, limit=25):
    candidates = get_site_candidates(user_query, limit=limit)
    return "\n".join(
        f"- {site_id}: {info['name']} [{info['state']}]"
        for site_id, info in candidates
    )


def _build_prompt_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Find SNOTEL Site</title>
  <style>
    :root {
      --bg: #f3f0e7;
      --panel: #fffdfa;
      --ink: #1b1f18;
      --accent: #315f3d;
      --accent-2: #8aa05b;
      --border: #d7cfbf;
    }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background:
        radial-gradient(circle at top, rgba(138, 160, 91, 0.18), transparent 32%),
        linear-gradient(180deg, #f7f4ec 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
    }
    .panel {
      width: min(680px, calc(100vw - 40px));
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: 0 24px 80px rgba(49, 95, 61, 0.12);
      padding: 28px;
    }
    h1 {
      margin: 0 0 10px;
      font-size: 32px;
      line-height: 1.05;
    }
    p {
      margin: 0 0 18px;
      font-size: 17px;
      line-height: 1.5;
    }
    form {
      display: grid;
      gap: 14px;
    }
    input {
      width: 100%;
      box-sizing: border-box;
      font: inherit;
      font-size: 18px;
      padding: 16px 18px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: #fff;
    }
    button {
      justify-self: start;
      font: inherit;
      font-size: 16px;
      padding: 12px 18px;
      border-radius: 999px;
      border: 0;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      color: #fff;
      cursor: pointer;
    }
    .hint {
      color: #586253;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <main class="panel">
    <h1>Find a SNOTEL site</h1>
    <p>Describe the station you want, like "Palisades Tahoe", "Mt Hood snow site", or "best Tahoe SNOTEL".</p>
    <form method="post" action="/submit">
      <input autofocus name="query" placeholder="Search for a snow site" />
      <button type="submit">Search with Sonar</button>
    </form>
    <p class="hint">This page stays local on your machine and only sends your typed query to the Python process running `snotel.py`.</p>
  </main>
</body>
</html>
"""


def _build_prompt_success_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Search Received</title>
  <style>
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #f3f0e7;
      color: #1b1f18;
      font-family: Georgia, "Times New Roman", serif;
    }
    .message {
      background: #fffdfa;
      border: 1px solid #d7cfbf;
      border-radius: 18px;
      padding: 24px 28px;
      box-shadow: 0 24px 80px rgba(49, 95, 61, 0.12);
      width: min(520px, calc(100vw - 40px));
    }
  </style>
</head>
<body>
  <div class="message">
    <h1>Search received</h1>
    <p>You can close this tab. The SNOTEL request is continuing in Python.</p>
  </div>
</body>
</html>
"""


def _make_prompt_handler(query_queue):
    class PromptHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            body = _build_prompt_page().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            parsed = urlparse(self.path)
            if parsed.path != "/submit":
                self.send_error(HTTPStatus.NOT_FOUND)
                return

            content_length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(content_length).decode("utf-8", errors="replace")
            form = parse_qs(payload, keep_blank_values=True)
            query = form.get("query", [""])[0].strip()
            query_queue.put(query)

            body = _build_prompt_success_page().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    return PromptHandler


def prompt_for_site_query():
    query_queue = Queue(maxsize=1)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_prompt_handler(query_queue))
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        prompt_url = f"http://127.0.0.1:{server.server_port}/"
        webbrowser.open(prompt_url)
        try:
            query = query_queue.get(timeout=PROMPT_TIMEOUT_SECONDS)
        except Empty as exc:
            raise ValueError("Timed out waiting for a site search query.") from exc
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=1)

    return query or None


def build_perplexity_messages(user_query):
    return [
        {
            "role": "system",
            "content": (
                "You resolve a user's natural-language snow site request to one site from "
                "the provided catalog. Return JSON only with keys site_id, site_name, and rationale. "
                "Choose only from the catalog. If uncertain, pick the closest match."
            ),
        },
        {
            "role": "user",
            "content": (
                f"User request: {user_query}\n\n"
                "Candidate catalog:\n"
                f"{build_candidate_catalog(user_query)}"
            ),
        },
    ]


def _extract_json_object(text):
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("Perplexity did not return a JSON object.")
    return json.loads(match.group(0))


def query_perplexity_for_site(user_query, api_key):
    payload = json.dumps(
        {
            "model": PERPLEXITY_MODEL,
            "messages": build_perplexity_messages(user_query),
            "temperature": 0,
        }
    ).encode("utf-8")
    request = Request(
        PERPLEXITY_API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Perplexity request failed with HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach the Perplexity service: {exc.reason}") from exc


def resolve_site_from_ai_response(response_payload):
    try:
        content = response_payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("Perplexity response did not include a chat message.") from exc

    parsed = _extract_json_object(content)
    site_id = str(parsed.get("site_id", "")).strip()
    if site_id in SITE_ID_TO_INFO:
        return site_id, SITE_ID_TO_INFO[site_id]

    site_name = str(parsed.get("site_name", "")).strip()
    if site_name:
        return get_site_id(site_name), get_site_info(site_name)

    raise ValueError("Perplexity response did not include a usable site_id or site_name.")


def resolve_site_with_ai(user_query):
    api_key = os.environ.get(PERPLEXITY_API_KEY_ENV, "").strip()
    if not api_key:
        raise ValueError(
            f"Set {PERPLEXITY_API_KEY_ENV} before using popup search without a site argument."
        )
    response_payload = query_perplexity_for_site(user_query, api_key)
    return resolve_site_from_ai_response(response_payload)


def resolve_site_from_popup_search():
    user_query = prompt_for_site_query()
    if user_query is None or not user_query.strip():
        raise ValueError("No site search was provided.")
    return resolve_site_with_ai(user_query.strip())


def parse_iso_date(value):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc

    if parsed.strftime("%Y-%m-%d") != value:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD.")

    return value


def looks_like_date(value):
    try:
        parse_iso_date(value)
    except ValueError:
        return False
    return True


def parse_cli_args(argv):
    args = list(argv)
    if len(args) == 2 and looks_like_date(args[0]):
        site_id, site_info = resolve_site_from_popup_search()
        start_date, end_date = args
    elif len(args) == 3:
        site_id = get_site_id(args[0])
        site_info = get_site_info(site_id)
        start_date, end_date = args[1], args[2]
    else:
        raise ValueError(USAGE)

    start_date = parse_iso_date(start_date)
    end_date = parse_iso_date(end_date)
    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date.")

    return site_id, site_info, start_date, end_date


def format_snotel_for_display(df, rows=None):
    display_df = df.rename(columns=DISPLAY_COLUMNS).sort_values(
        "date", ascending=False
    )
    if rows is not None:
        display_df = display_df.head(rows)
    display_df = display_df.copy()

    if "date" in display_df.columns:
        display_df["date"] = pd.to_datetime(display_df["date"]).dt.strftime("%Y-%m-%d")

    return display_df.fillna("-")


def build_output_filename(site_id, start_date, end_date):
    return f"snotel_{site_id}_{start_date}_{end_date}.html"


def format_site_heading(site_info):
    elevation_ft = site_info.get("elevation_ft")
    if elevation_ft is None:
        return site_info["name"]
    return f"{site_info['name']} ({elevation_ft} ft)"


def build_html_table(df, site_info):
    table_html = df.to_html(index=False, border=0, classes="snotel-table")
    heading = format_site_heading(site_info)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SNOTEL Table</title>
  <style>
    body {{
      margin: 24px;
      font-family: Arial, sans-serif;
      background: #ffffff;
      color: #000000;
    }}

    h1 {{
      margin-bottom: 16px;
      font-size: 24px;
    }}

    .snotel-table {{
      border-collapse: collapse;
      border: 2px solid #000000;
      font-size: 14px;
      min-width: 720px;
    }}

    .snotel-table th,
    .snotel-table td {{
      border: 1px solid #000000;
      padding: 10px 12px;
      text-align: center;
      color: #000000;
      background: #ffffff;
    }}

    .snotel-table th {{
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <h1>{heading}</h1>
  {table_html}
</body>
</html>
"""


def write_html_table(df, site_info, output_path):
    output_file = Path(output_path)
    output_file.write_text(build_html_table(df, site_info), encoding="utf-8")
    return output_file.resolve()


def open_html_table(output_file):
    return webbrowser.open(output_file.resolve().as_uri())


def run_cli(argv=None, open_browser=True):
    argv = sys.argv[1:] if argv is None else argv
    site_id, site_info, start_date, end_date = parse_cli_args(argv)
    df_snow = get_snotel_data(site_id, site_info["awdb_state"], start_date, end_date)
    display_df = format_snotel_for_display(df_snow)
    output_file = write_html_table(
        display_df,
        site_info,
        build_output_filename(site_id, start_date, end_date),
    )

    if open_browser:
        open_html_table(output_file)

    print(
        "Black-bordered table written to: "
        f"{output_file} for site {site_id} "
        f"({site_info['name']}, {site_info['state']}) from {start_date} to {end_date}"
    )
    return output_file


if __name__ == "__main__":
    try:
        run_cli()
    except (KeyError, ValueError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
