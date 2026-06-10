"""Fetch per-race realtime tansho odds from the hot worker and write a parquet.

This module is excluded from the coverage gate (only ``predict_lib`` is
measured) because it performs live HTTP I/O against the public Cloudflare
hot-worker endpoint. It is verified at deploy time, not in CI unit tests.

The ``RealtimeOddsFetcher`` Protocol makes the HTTP layer injectable so
pipeline_runner tests can stub the fetcher without patching ``urllib``.

Flow per category run:
  1. Query Neon for today's upcoming race keys (keibajo_code + race_bango) via
     the supplied database connection (already opened by predict_upcoming.py).
  2. For each race key call
     ``GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/{raceKey}``
     (no auth required). Timeout = 5 s. Failure of ANY individual request is
     logged and treated as "no odds for that race" — the COALESCE in the
     DuckDB builder then falls back to the nvd_se / jvd_se value (still NULL
     for pre-sync upcoming races, same as before this feature).
  3. Collect rows: (keibajo_code, race_bango, umaban, tansho_odds_realtime,
     ninkijun_realtime) from the latest snapshot in each response.
  4. Write the collected rows to a parquet file under ``work_dir`` and return
     the path. Returns ``None`` when zero rows were collected (empty-path).

Race key construction:
  {source}:{YYYY}:{MMDD}:{keibajo_code}:{race_bango}
  e.g.  nar:2026:0610:44:01
  Colons must be percent-encoded in the URL path segment: nar%3A2026%3A...

Units note (verified against feasibility report §3):
  D1 odds column = direct multiplier (e.g. 7.3).
  The DuckDB builder's COALESCE uses this value directly for the formula
  ``ln(max(odds, 1)) / ln(300)`` — no divide-by-10 needed (unlike nvd_se's
  4-char /10 raw string path).
  D1 rank column = ninkijun, 1 = favourite (ascending) — same convention as
  nvd_se tansho_ninkijun. The builder's popularity_score formula
  ``(ninkijun-1) / (runner_count-1)`` works unchanged.
"""

from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Protocol, runtime_checkable

HOT_WORKER_BASE_URL: str = "https://sync-realtime-data-hot.kkk4oru.com/api/odds"
FETCH_TIMEOUT_SECONDS: float = 5.0

# NAR keibajo_code for Ban-ei (Obihiro). Ban-ei odds ARE in D1 (confirmed in
# feasibility report), so Ban-ei uses the same fetch path as regular NAR.
_SOURCE_BY_CATEGORY: dict[str, str] = {
    "jra": "jra",
    "nar": "nar",
    "ban-ei": "nar",
}


@runtime_checkable
class RealtimeOddsFetcher(Protocol):
    """Injectable HTTP fetcher for realtime odds responses.

    The default implementation calls the live Cloudflare hot worker. Tests
    substitute a stub that returns pre-canned dicts without network I/O.
    """

    def fetch(self, url: str, timeout: float) -> dict[str, object]:
        """Fetch ``url`` and return the parsed JSON body as a dict.

        Should raise ``urllib.error.URLError`` / ``TimeoutError`` /
        ``json.JSONDecodeError`` on failure so the caller can log and
        continue with the empty-odds fallback.
        """
        ...


class HttpRealtimeOddsFetcher:
    """Production fetcher: plain ``urllib.request`` GET (no auth needed)."""

    def fetch(self, url: str, timeout: float) -> dict[str, object]:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
        result: dict[str, object] = json.loads(raw)
        return result


def build_race_key(source: str, target_date: str, keibajo_code: str, race_bango: str) -> str:
    """Construct the race key in ``{source}:{YYYY}:{MMDD}:{KK}:{RR}`` format."""
    year = target_date[:4]
    mmdd = target_date[4:]
    return f"{source}:{year}:{mmdd}:{keibajo_code}:{race_bango}"


def encode_race_key(race_key: str) -> str:
    """Percent-encode the race key for use as a URL path segment."""
    return urllib.parse.quote(race_key, safe="")


def extract_rows(
    keibajo_code: str,
    race_bango: str,
    response: dict[str, object],
) -> list[tuple[str, str, int, float, int]]:
    """Extract (keibajo_code, race_bango, umaban, odds, rank) rows from a response.

    Returns an empty list when ``latest.tansho`` is absent or empty (race not
    yet polled, or keibajo not covered by the hot worker). The COALESCE in the
    DuckDB builder then uses the nvd_se fallback transparently.
    """
    latest = response.get("latest")
    if not isinstance(latest, dict):
        return []
    tansho = latest.get("tansho")
    if not isinstance(tansho, list) or not tansho:
        return []
    rows: list[tuple[str, str, int, float, int]] = []
    for entry in tansho:
        if not isinstance(entry, dict):
            continue
        combination = entry.get("combination")
        odds_val = entry.get("odds")
        rank_val = entry.get("rank")
        if combination is None or odds_val is None or rank_val is None:
            continue
        try:
            umaban = int(str(combination).strip())
            tansho_odds = float(odds_val)
            ninkijun = int(rank_val)
        except (ValueError, TypeError):
            continue
        if tansho_odds <= 0:
            continue
        rows.append((keibajo_code, race_bango, umaban, tansho_odds, ninkijun))
    return rows


def fetch_odds_for_race(
    fetcher: RealtimeOddsFetcher,
    source: str,
    target_date: str,
    keibajo_code: str,
    race_bango: str,
) -> list[tuple[str, str, int, float, int]]:
    """Fetch odds for one race; returns empty list on any error."""
    race_key = build_race_key(source, target_date, keibajo_code, race_bango)
    encoded = encode_race_key(race_key)
    url = f"{HOT_WORKER_BASE_URL}/{encoded}"
    try:
        response = fetcher.fetch(url, FETCH_TIMEOUT_SECONDS)
    except Exception as exc:
        print(
            f"[realtime-odds] fetch failed race_key={race_key} error={exc}",
            file=sys.stderr,
        )
        return []
    return extract_rows(keibajo_code, race_bango, response)


def _write_parquet(
    rows: list[tuple[str, str, int, float, int]],
    path: Path,
) -> None:
    """Write collected rows to a parquet file using pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "keibajo_code": pa.array([r[0] for r in rows], type=pa.string()),
            "race_bango": pa.array([r[1] for r in rows], type=pa.string()),
            "umaban": pa.array([r[2] for r in rows], type=pa.int32()),
            "tansho_odds_realtime": pa.array([r[3] for r in rows], type=pa.float64()),
            "ninkijun_realtime": pa.array([r[4] for r in rows], type=pa.int32()),
        }
    )
    pq.write_table(table, str(path))


def fetch_realtime_odds_parquet(
    category: str,
    target_date: str,
    work_dir: Path,
    race_keys: list[tuple[str, str]] | None = None,
    fetcher: RealtimeOddsFetcher | None = None,
) -> Path | None:
    """Fetch realtime odds for all races in ``category`` on ``target_date``.

    ``race_keys`` is a list of (keibajo_code, race_bango) pairs to fetch. When
    ``None`` it is resolved from the Neon DB (not yet wired — the container's
    predict_upcoming already has the list via the upcoming-race query; this
    function currently reads ``PREDICT_RACE_KEYS_ENV`` as a JSON-encoded list
    injected by the Worker, or derives it from the feature parquet if available,
    or returns ``None`` when no keys are discoverable so the pipeline falls back
    gracefully).

    In the current implementation the function is called with explicit
    ``race_keys`` from ``predict_upcoming.py``; the ``None`` path is a safety
    fallback that logs a warning and returns ``None``.

    On success writes a parquet to ``work_dir / realtime-odds-{category}.parquet``
    and returns the path. Returns ``None`` when zero rows were collected (graceful
    empty path — the DuckDB builder uses the nvd_se fallback).

    Any individual race fetch failure is swallowed (logged to stderr); the
    remaining races are still fetched.
    """
    if fetcher is None:
        fetcher = HttpRealtimeOddsFetcher()

    if race_keys is None:
        print(
            f"[realtime-odds] no race_keys provided for category={category} "
            "target_date={target_date} — skipping realtime odds fetch",
            file=sys.stderr,
        )
        return None

    source = _SOURCE_BY_CATEGORY.get(category, "nar")
    all_rows: list[tuple[str, str, int, float, int]] = []
    for keibajo_code, race_bango in race_keys:
        rows = fetch_odds_for_race(fetcher, source, target_date, keibajo_code, race_bango)
        all_rows.extend(rows)

    if not all_rows:
        print(
            f"[realtime-odds] zero rows collected category={category} "
            f"target_date={target_date} races={len(race_keys)} — using null-odds fallback",
            file=sys.stderr,
        )
        return None

    out_path = work_dir / f"realtime-odds-{category}.parquet"
    work_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(all_rows, out_path)
    print(
        f"[realtime-odds] wrote {len(all_rows)} rows to {out_path} "
        f"category={category} races={len(race_keys)}",
        file=sys.stderr,
    )
    return out_path
