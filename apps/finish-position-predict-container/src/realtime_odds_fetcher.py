"""Fetch per-race realtime tansho odds and bataiju from the Cloudflare workers.

This module is excluded from the coverage gate (only ``predict_lib`` is
measured) because it performs live HTTP I/O against the public Cloudflare
worker endpoints. It is verified at deploy time, not in CI unit tests.

The ``RealtimeOddsFetcher`` Protocol makes the HTTP layer injectable so
pipeline_runner tests can stub the fetcher without patching ``urllib``.

Flow per category run:
  1. Query Neon for today's upcoming race keys (keibajo_code + race_bango) via
     the supplied database connection (already opened by predict_upcoming.py).
  2. For each race key call
     ``GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/{raceKey}``
     (hot worker, no auth required). Timeout = 5 s. Failure of ANY individual
     request is logged and treated as "no odds for that race" — the COALESCE in
     the DuckDB builder then falls back to the nvd_se / jvd_se value (still NULL
     for pre-sync upcoming races, same as before this feature).
  3. Also fetch
     ``GET https://sync-realtime-data.kkk4oru.com/api/horse-weight/{raceKey}``
     (main worker, no auth required). Timeout = 5 s. Failure is swallowed; the
     DuckDB builder then COALESCEs the bataiju realtime value first, then falls
     back to the nvd_se/jvd_se raw string field.
  4. Collect rows: (keibajo_code, race_bango, umaban, tansho_odds_realtime,
     ninkijun_realtime, bataiju_realtime) merging odds + weight by umaban.
  5. Write the collected rows to a parquet file under ``work_dir`` and return
     the path. Returns ``None`` when zero rows were collected (empty-path).

Race key construction:
  {source}:{YYYY}:{MMDD}:{keibajo_code}:{race_bango}
  e.g.  nar:2026:0610:44:01
  Colons must be percent-encoded in the URL path segment: nar%3A2026%3A...

Units notes:
  D1 odds column = direct multiplier (e.g. 7.3). No divide-by-10.
  D1 rank column = ninkijun, 1 = favourite (ascending). Same as nvd_se.
  D1 / DO weight column = integer kg (e.g. 447). Same units as nvd_se.bataiju
  (which is also a raw integer-string, e.g. "447"). No conversion needed.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Protocol, runtime_checkable

HOT_WORKER_BASE_URL: str = "https://sync-realtime-data-hot.kkk4oru.com/api/odds"
WEIGHT_WORKER_BASE_URL: str = "https://sync-realtime-data.kkk4oru.com/api/horse-weight"
FETCH_TIMEOUT_SECONDS: float = 5.0

# Cloudflare WAF blocks Python's default User-Agent (empty / "Python-urllib/3.x").
# Any explicit, non-empty UA string passes. We use a descriptive internal UA so
# logs on the worker side are legible and clearly identify the predict container.
_REQUEST_HEADERS: dict[str, str] = {
    "Accept": "application/json",
    "User-Agent": "horse-racing-data-predict/1.0",
}

FETCH_MAX_RETRIES: int = 2
FETCH_BACKOFF_BASE_SECONDS: float = 0.5

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
    """Production fetcher: plain ``urllib.request`` GET with explicit headers.

    Cloudflare WAF (protecting both the hot worker and the main worker) rejects
    requests with Python's default empty User-Agent with HTTP 403.  Setting an
    explicit ``User-Agent`` (any non-empty string) and ``Accept`` header makes
    every request pass the WAF without weakening the graceful-fallback logic.
    """

    def fetch(self, url: str, timeout: float) -> dict[str, object]:
        req = urllib.request.Request(url, headers=_REQUEST_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
        result: dict[str, object] = json.loads(raw)
        return result


def fetch_with_retry(
    fetcher: RealtimeOddsFetcher,
    url: str,
    timeout: float,
    max_retries: int = FETCH_MAX_RETRIES,
    backoff_base: float = FETCH_BACKOFF_BASE_SECONDS,
) -> dict[str, object]:
    """Fetch ``url`` with exponential-backoff retry on transient errors.

    Retries up to ``max_retries`` times with ``backoff_base * 2**attempt``
    seconds between attempts (0.5s, 1.0s for the defaults). Raises on the
    final failure so the caller can decide how to log/swallow.

    Total added latency per race: at most 0.5 + 1.0 = 1.5 s extra, well within
    the >50 min run budget.
    """
    for attempt in range(max_retries + 1):
        try:
            return fetcher.fetch(url, timeout)
        except Exception as exc:
            if attempt == max_retries:
                raise
            sleep_seconds = backoff_base * (2**attempt)
            print(
                f"[realtime-odds] fetch attempt {attempt + 1} failed url={url} "
                f"error={exc!r} — retrying in {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
    # unreachable — loop always raises or returns
    raise RuntimeError("unreachable")  # pragma: no cover


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


def extract_weight_map(response: dict[str, object]) -> dict[int, int]:
    """Extract ``{umaban -> bataiju_kg}`` from a horse-weight response.

    Returns an empty dict when the response has no ``horses`` list or when
    individual entries are malformed (graceful degradation — the DuckDB builder
    falls back to the nvd_se field for missing umaban keys).
    """
    horses = response.get("horses")
    if not isinstance(horses, list):
        return {}
    result: dict[int, int] = {}
    for entry in horses:
        if not isinstance(entry, dict):
            continue
        horse_number = entry.get("horseNumber")
        weight_val = entry.get("weight")
        if horse_number is None or weight_val is None:
            continue
        try:
            umaban = int(str(horse_number).strip())
            bataiju = int(weight_val)
        except (ValueError, TypeError):
            continue
        if bataiju > 0:
            result[umaban] = bataiju
    return result


def fetch_weight_for_race(
    fetcher: RealtimeOddsFetcher,
    source: str,
    target_date: str,
    keibajo_code: str,
    race_bango: str,
) -> dict[int, int]:
    """Fetch bataiju map ``{umaban -> kg}`` for one race; empty dict on any error."""
    race_key = build_race_key(source, target_date, keibajo_code, race_bango)
    encoded = encode_race_key(race_key)
    url = f"{WEIGHT_WORKER_BASE_URL}/{encoded}"
    try:
        response = fetch_with_retry(fetcher, url, FETCH_TIMEOUT_SECONDS)
    except Exception as exc:
        print(
            f"[realtime-weight] fetch failed race_key={race_key} error={exc}",
            file=sys.stderr,
        )
        return {}
    return extract_weight_map(response)


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
        response = fetch_with_retry(fetcher, url, FETCH_TIMEOUT_SECONDS)
    except Exception as exc:
        print(
            f"[realtime-odds] fetch failed race_key={race_key} error={exc}",
            file=sys.stderr,
        )
        return []
    return extract_rows(keibajo_code, race_bango, response)


# OddsRow = (keibajo_code, race_bango, umaban, tansho_odds, ninkijun)
_OddsRow = tuple[str, str, int, float, int]
# RealtimeRow = (keibajo_code, race_bango, umaban, tansho_odds, ninkijun, bataiju|None)
_RealtimeRow = tuple[str, str, int, float, int, int | None]


def merge_weight_into_rows(
    odds_rows: list[_OddsRow],
    weight_map: dict[int, int],
) -> list[_RealtimeRow]:
    """Merge bataiju values into odds rows by umaban.

    Horses present in ``odds_rows`` but absent from ``weight_map`` get
    ``None`` for bataiju so the DuckDB COALESCE falls back to nvd_se.
    """
    return [
        (r[0], r[1], r[2], r[3], r[4], weight_map.get(r[2]))
        for r in odds_rows
    ]


def _write_parquet(
    rows: list[_RealtimeRow],
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
            "bataiju_realtime": pa.array(
                [r[5] for r in rows], type=pa.int32()
            ),
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
    """Fetch realtime odds + bataiju for all races in ``category`` on ``target_date``.

    ``race_keys`` is a list of (keibajo_code, race_bango) pairs to fetch. When
    ``None`` the function logs a warning and returns ``None`` so the caller
    falls back to the NULL-odds / NULL-bataiju path gracefully.

    In the current implementation the function is called with explicit
    ``race_keys`` from ``predict_upcoming.py``; the ``None`` path is a safety
    fallback.

    On success writes a parquet to ``work_dir / realtime-odds-{category}.parquet``
    with columns (keibajo_code, race_bango, umaban, tansho_odds_realtime,
    ninkijun_realtime, bataiju_realtime) and returns the path. Returns ``None``
    when zero odds rows were collected (graceful empty path — the DuckDB builder
    uses the nvd_se fallback for odds; bataiju_realtime column is absent so the
    COALESCE falls through to the se field).

    Bataiju fetch failures are swallowed individually; horses missing from the
    weight response get NULL bataiju_realtime so COALESCE falls back to nvd_se.
    Any individual odds fetch failure is also swallowed (logged to stderr).
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
    all_rows: list[_RealtimeRow] = []
    for keibajo_code, race_bango in race_keys:
        odds_rows = fetch_odds_for_race(fetcher, source, target_date, keibajo_code, race_bango)
        if not odds_rows:
            continue
        weight_map = fetch_weight_for_race(fetcher, source, target_date, keibajo_code, race_bango)
        all_rows.extend(merge_weight_into_rows(odds_rows, weight_map))

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
    bataiju_count = sum(1 for r in all_rows if r[5] is not None)
    print(
        f"[realtime-odds] wrote {len(all_rows)} rows to {out_path} "
        f"category={category} races={len(race_keys)} bataiju={bataiju_count}/{len(all_rows)}",
        file=sys.stderr,
    )
    return out_path
