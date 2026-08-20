"""PUT per-race feat-cache from tonight's full-day host parquet. Skip existing HITs."""

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
import pandas as pd

sys.path.insert(0, "apps/finish-position-predict-container/src")
from predict_lib.r2_client import _build_signed_request
from predict_lib.serve import R2Config, build_r2_per_race_feat_cache_key

SKIP = {
    ("jra", "04", "12"),
    ("jra", "07", "01"),
    ("nar", "35", "05"),
    ("nar", "35", "06"),
    ("nar", "35", "07"),
    ("nar", "44", "10"),
    ("ban-ei", "83", "03"),
    ("ban-ei", "83", "06"),
    ("ban-ei", "83", "07"),
    ("ban-ei", "83", "10"),
}
SOURCES = {
    "jra": Path("/tmp/predict-upcoming/feat-jra-layer-16"),
    "nar": Path("/tmp/predict-upcoming/feat-nar-v7-final"),
    "ban-ei": Path("/tmp/predict-upcoming/feat-ban-ei-v7-final"),
}
OUT = Path("/tmp/feat-cache-seed-0816")
EMPTY_SHA = hashlib.sha256(b"").hexdigest()


def signed_put(r2: R2Config, object_key: str, body: bytes) -> int:
    now = datetime.now(UTC)
    amzdate = now.strftime("%Y%m%dT%H:%M:%SZ").replace(":", "").replace("-", "")
    # keep same format as GET helper
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
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def slice_race(df: pd.DataFrame, keibajo: str, bango: str) -> pd.DataFrame:
    out = df[
        (df["keibajo_code"].astype(str) == keibajo)
        & (df["race_bango"].astype(str) == bango)
    ].copy()
    if "race_year" in out.columns:
        out = out.drop(columns=["race_year"])
    return out


def main() -> int:
    r2 = R2Config(
        account_id=os.environ["R2_ACCOUNT_ID"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        bucket=os.environ["R2_BUCKET"],
    )
    OUT.mkdir(parents=True, exist_ok=True)
    put_ok = 0
    skipped = 0
    failed = 0
    for category, src in SOURCES.items():
        files = list(src.rglob("*.parquet"))
        if not files:
            print(f"MISSING {src}", flush=True)
            failed += 1
            continue
        df = duckdb.sql(f"select * from read_parquet('{files[0]}')").df()
        races = (
            df[["keibajo_code", "race_bango"]]
            .astype(str)
            .drop_duplicates()
            .sort_values(["keibajo_code", "race_bango"])
        )
        for _, row in races.iterrows():
            keibajo = str(row["keibajo_code"])
            bango = str(row["race_bango"])
            if (category, keibajo, bango) in SKIP:
                skipped += 1
                print(f"SKIP existing HIT {category} {keibajo}/{bango}", flush=True)
                continue
            key = build_r2_per_race_feat_cache_key(category, "20260816", keibajo, bango)
            head = _build_signed_request(r2, key, "HEAD")
            try:
                with urllib.request.urlopen(head, timeout=15):
                    skipped += 1
                    print(f"SKIP unexpected HIT {category} {keibajo}/{bango}", flush=True)
                    continue
            except urllib.error.HTTPError as exc:
                if exc.code != 404:
                    failed += 1
                    print(f"HEAD {category} {keibajo}/{bango} {exc.code}", flush=True)
                    continue
            sliced = slice_race(df, keibajo, bango)
            if sliced.empty:
                failed += 1
                print(f"EMPTY {category} {keibajo}/{bango}", flush=True)
                continue
            dest = OUT / f"{category}-{keibajo}-{bango}.parquet"
            sliced.to_parquet(dest, index=False)
            status = signed_put(r2, key, dest.read_bytes())
            if status in {200, 204}:
                put_ok += 1
                print(
                    f"PUT {status} {category} {keibajo}/{bango} rows={len(sliced)} cols={sliced.shape[1]}",
                    flush=True,
                )
            else:
                failed += 1
                print(f"PUT FAIL {status} {category} {keibajo}/{bango}", flush=True)
    print(f"DONE put_ok={put_ok} skipped={skipped} failed={failed}", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
