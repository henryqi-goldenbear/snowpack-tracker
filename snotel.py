from io import StringIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pandas as pd


DISPLAY_COLUMNS = {
    "Date": "date",
    "Snow Water Equivalent (in) Start of Day Values": "swe_in",
    "Snow Depth (in) Start of Day Values": "snow_depth_in",
    "Precipitation Accumulation (in) Start of Day Values": "precip_in",
    "Air Temperature Maximum (degF)": "tmax_f",
    "Air Temperature Minimum (degF)": "tmin_f",
    "Air Temperature Average (degF)": "tavg_f",
}


def build_snotel_url(site_id, state):
    return (
        "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
        f'customSingleStationReport/daily/{site_id}:{state}:SNTL%7Cid=%22%22%7Cname/'
        "POR_BEGIN,POR_END/WTEQ::value,SNWD::value,PREC::value,"
        "TMAX::value,TMIN::value,TAVG::value"
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


def get_snotel_data(site_id, state):
    url = build_snotel_url(site_id, state)

    try:
        with urlopen(url, timeout=30) as response:
            report_text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"SNOTEL request failed with HTTP {exc.code}.") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach the SNOTEL service: {exc.reason}") from exc

    csv_text = _extract_csv_text(report_text)
    df = pd.read_csv(StringIO(csv_text), parse_dates=["Date"])
    return df


def format_snotel_for_display(df, rows=10):
    display_df = (
        df.rename(columns=DISPLAY_COLUMNS)
        .sort_values("date", ascending=False)
        .head(rows)
        .copy()
    )

    if "date" in display_df.columns:
        display_df["date"] = pd.to_datetime(display_df["date"]).dt.strftime("%Y-%m-%d")

    return display_df.fillna("-")


def build_html_table(df):
    table_html = df.to_html(index=False, border=0, classes="snotel-table")
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
  <h1>SNOTEL Data</h1>
  {table_html}
</body>
</html>
"""


def write_html_table(df, output_path="snotel_table.html"):
    output_file = Path(output_path)
    output_file.write_text(build_html_table(df), encoding="utf-8")
    return output_file.resolve()


if __name__ == "__main__":
    df_snow = get_snotel_data("395", "OR")
    display_df = format_snotel_for_display(df_snow)
    output_file = write_html_table(display_df)
    print(f"Black-bordered table written to: {output_file}")
