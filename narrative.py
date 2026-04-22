import json
import os
import re
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import snotel


DEFAULT_NARRATIVE_MODEL = os.environ.get("OPENAI_MODEL", getattr(snotel, "OPENAI_MODEL", "gpt-4o-mini"))
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"


def extract_json_object(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("Model response did not include a JSON object.")
    return json.loads(match.group(0))


def _numeric_tokens(text: str) -> set[str]:
    return set(re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)", text))


def validate_narrative_json(narrative: dict[str, Any], facts: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(narrative, dict):
        return False, "Narrative must be a JSON object."

    required = {"headline", "bullets", "disclaimer"}
    missing = required - set(narrative.keys())
    if missing:
        return False, f"Missing keys: {', '.join(sorted(missing))}."

    headline = narrative.get("headline")
    bullets = narrative.get("bullets")
    disclaimer = narrative.get("disclaimer")

    if not isinstance(headline, str) or not headline.strip():
        return False, "headline must be a non-empty string."
    if not isinstance(disclaimer, str) or not disclaimer.strip():
        return False, "disclaimer must be a non-empty string."
    if not isinstance(bullets, list) or not all(isinstance(item, str) for item in bullets):
        return False, "bullets must be a list of strings."
    if not (3 <= len(bullets) <= 6):
        return False, "bullets must have 3 to 6 items."

    allowed = _numeric_tokens(json.dumps(facts, sort_keys=True))
    produced = _numeric_tokens(" ".join([headline, disclaimer, *bullets]))
    unexpected = sorted(produced - allowed)
    if unexpected:
        return False, f"Unexpected numbers in narrative: {', '.join(unexpected[:12])}."

    return True, ""


def build_narrative_messages(facts: dict[str, Any]) -> list[dict[str, str]]:
    system = (
        "You write a short public-facing climate/snowpack summary that is strictly grounded in provided facts. "
        "Return JSON only with keys: headline (string), bullets (array of 3-6 strings), disclaimer (string). "
        "Do NOT introduce any numbers not present in the facts. Reuse numeric strings exactly as given."
    )
    user = (
        "Write the narrative for these facts. Use plain language. Avoid predictions. "
        "Include coverage/missing-data context.\n\n"
        f"FACTS_JSON:\n{json.dumps(facts, indent=2, sort_keys=True)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def query_openai_json(
    messages: list[dict[str, str]],
    *,
    api_key: str,
    model: str = DEFAULT_NARRATIVE_MODEL,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    payload = json.dumps({"model": model, "messages": messages, "temperature": 0}).encode("utf-8")
    request = Request(
        getattr(snotel, "OPENAI_API_URL", "https://api.openai.com/v1/chat/completions"),
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI request failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach the OpenAI service: {exc.reason}") from exc


def generate_template_narrative(facts: dict[str, Any]) -> dict[str, Any]:
    state = facts.get("state", "")
    metric = facts.get("metric_key", "swe")
    end_date = facts.get("end_date", "")
    pct = facts.get("end_statewide_median_percentile", "NA")
    used = facts.get("end_stations_used", 0)
    total = facts.get("station_count_total", 0)
    cov = facts.get("end_pct_with_data", "NA")

    metric_label = {
        "swe": "snow water equivalent (SWE)",
        "snow_depth": "snow depth",
        "precip": "precipitation",
        "tavg": "average temperature",
    }.get(metric, metric)

    top = (facts.get("top_stations") or [])[:1]
    bottom = (facts.get("bottom_stations") or [])[:1]
    top_text = (
        f"Highest station percentile example: {top[0]['name']} ({top[0]['percentile']}th)."
        if top
        else "No top-station example available."
    )
    bottom_text = (
        f"Lowest station percentile example: {bottom[0]['name']} ({bottom[0]['percentile']}th)."
        if bottom
        else "No bottom-station example available."
    )

    return {
        "headline": f"{state}: {metric_label} is around the {pct}th percentile (median across stations) on {end_date}",
        "bullets": [
            f"Statewide median percentile on {end_date}: {pct}th (median across stations).",
            f"Data coverage: {used}/{total} stations reported ({cov}%).",
            top_text,
            bottom_text,
        ],
        "disclaimer": "Grounded in SNOTEL station observations; not a forecast. Coverage varies by station and day.",
    }


def generate_grounded_narrative(
    facts: dict[str, Any],
    *,
    use_ai: bool = True,
    api_key: Optional[str] = None,
) -> dict[str, Any]:
    api_key = (api_key if api_key is not None else os.environ.get(OPENAI_API_KEY_ENV, "")).strip()
    if not use_ai or not api_key:
        return generate_template_narrative(facts)

    messages = build_narrative_messages(facts)
    try:
        payload = query_openai_json(messages, api_key=api_key)
        content = payload["choices"][0]["message"]["content"]
        candidate = extract_json_object(content)
        ok, reason = validate_narrative_json(candidate, facts)
        if ok:
            return candidate
    except Exception:
        pass

    return generate_template_narrative(facts)
