"""Seed per-race feat-cache from tonight's healthy full-day host parquet.

Uses the same DuckDB COPY(SELECT * WHERE race_id=...) as
predict_upcoming._split_parquet_by_race. Overwrites degenerate production
HITs because those objects kill pedigree on the next weight rescore.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import duckdb

sys.path.insert(0, "apps/finish-position-predict-container/src")
from predict_lib.r2_client import _build_signed_request
from predict_lib.serve import R2Config, build_r2_per_race_feat_cache_key

SOURCES = {
    "jra": Path("/tmp/predict-upcoming/feat-jra-layer-16"),
    "nar": Path("/tmp/predict-upcoming/feat-nar-v7-final"),
    "ban-ei": Path("/tmp/predict-upcoming/feat-ban-ei-v7-final"),
}
WORK = Path("/tmp/feat-cache-seed-healthy-0816")


def signed_put(r2: R2Config, object_key: str, body: bytes) -> int:
    now = datetime.now(UTC)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    host = f"{r2.account_id}.r2.cloudflarestorage.com"
    canonical_headers = (
        f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"PUT\n/{r2.bucket}/{object_key}\n\n{canonical_headers}\n"
        f"{signed_headers}\n{payload_hash}"
    )
    scope = f"{datestamp}/auto/s3/aws4_request"

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    signing_key = _sign(
        _sign(
            _sign(_sign(f"AWS4{r2.secret_access_key}".encode(), datestamp), "auto"),
            "s3",
        ),
        "aws4_request",
    )
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n"
        + hashlib.sha256(canonical_request.encode()).hexdigest()
    )
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    auth = (
        f"AWS4-HMAC-SHA256 Credential={r2.access_key_id}/{scope},"
        f" SignedHeaders={signed_headers}, Signature={signature}"
    )
    url = f"https://{host}/{r2.bucket}/{object_key}"
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Authorization": auth,
            "x-amz-date": amzdate,
            "x-amz-content-sha256": payload_hash,
            "Content-Type": "application/octet-stream",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return int(resp.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def race_ids(con: duckdb.DuckDBPyConnection, glob_path: str) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "SELECT DISTINCT race_id FROM read_parquet(?, hive_partitioning = false) "
            "ORDER BY 1",
            [glob_path],
        ).fetchall()
    ]


def write_race(
    con: duckdb.DuckDBPyConnection, glob_path: str, race_id: str, dest: Path
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.unlink(missing_ok=True)
    out_sql = dest.as_posix().replace("'", "''")
    con.execute(
        "COPY (SELECT * FROM read_parquet(?, hive_partitioning = false) "
        f"WHERE race_id = ?) TO '{out_sql}' (FORMAT PARQUET)",
        [glob_path, race_id],
    )


def score_live(path: Path) -> tuple[int, int, int]:
    row = duckdb.sql(
        f"""
        select
          count(*) as n,
          count(*) filter (where pedigree_score_for_race > 0) as pos,
          count(*) filter (where pedigree_score_for_race is null) as nnull
        from read_parquet('{path}')
        """
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), int(row[2])


def main() -> int:
    r2 = R2Config(
        account_id=os.environ["R2_ACCOUNT_ID"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        bucket=os.environ["R2_BUCKET"],
    )
    WORK.mkdir(parents=True, exist_ok=True)
    only = os.environ.get("SEED_ONLY")  # e.g. jra:04:01
    dry = os.environ.get("SEED_DRY") == "1"
    put_ok = skipped = failed = 0
    for category, src in SOURCES.items():
        files = list(src.rglob("*.parquet"))
        if not files:
            print(f"MISSING {src}", flush=True)
            failed += 1
            continue
        glob_path = str(src / "**" / "*.parquet")
        con = duckdb.connect(":memory:")
        try:
            for race_id in race_ids(con, glob_path):
                parts = race_id.split(":")
                if len(parts) < 5:
                    print(f"SKIP malformed {race_id}", flush=True)
                    skipped += 1
                    continue
                keibajo, bango = parts[3], parts[4]
                if only and only != f"{category}:{keibajo}:{bango}":
                    continue
                dest = WORK / f"{category}-{keibajo}-{bango}.parquet"
                write_race(con, glob_path, race_id, dest)
                n, pos, nnull = score_live(dest)
                print(
                    f"SLICE {category} {keibajo}/{bango} n={n} score_pos={pos} score_null={nnull}",
                    flush=True,
                )
                if dry:
                    skipped += 1
                    continue
                key = build_r2_per_race_feat_cache_key(
                    category, "20260816", keibajo, bango
                )
                status = signed_put(r2, key, dest.read_bytes())
                if status in {200, 204}:
                    put_ok += 1
                    print(f"PUT {status} {key}", flush=True)
                else:
                    failed += 1
                    print(f"PUT FAIL {status} {key}", flush=True)
        finally:
            con.close()
    print(f"DONE put_ok={put_ok} skipped={skipped} failed={failed} dry={dry}", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
