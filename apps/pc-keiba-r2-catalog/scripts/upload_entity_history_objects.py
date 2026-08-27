#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "aiohttp>=3.13,<4",
# ]
# ///
"""Upload immutable entity-history objects and publish their manifest last."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Final
from urllib.parse import quote

import aiohttp

DEFAULT_BUCKET: Final[str] = "pc-keiba-r2-catalog"
DEFAULT_CONCURRENCY: Final[int] = 50
PREFIX: Final[str] = "entity-serving-v1"
MAX_ATTEMPTS: Final[int] = 5


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output", nargs="?", type=Path, default=Path("tmp/entity-history-objects")
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    args = parser.parse_args(argv)
    if args.concurrency < 1 or args.concurrency > 100:
        parser.error("--concurrency must be between 1 and 100")
    return args


def object_url(account_id: str, bucket: str, key: str) -> str:
    encoded_bucket = quote(bucket, safe="")
    encoded_key = quote(key, safe="/")
    return (
        "https://api.cloudflare.com/client/v4/accounts/"
        f"{quote(account_id, safe='')}/r2/buckets/{encoded_bucket}/objects/{encoded_key}"
    )


def required_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    raise RuntimeError(f"one of {', '.join(names)} is required")


async def upload_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    account_id: str,
    bucket: str,
    token: str,
    path: Path,
    key: str,
) -> None:
    body = path.read_bytes()
    url = object_url(account_id, bucket, key)
    async with semaphore:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                async with session.put(
                    url,
                    data=body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/octet-stream",
                    },
                ) as response:
                    text = await response.text()
                    if response.status == 200:
                        return
                    if response.status < 500 and response.status != 429:
                        raise RuntimeError(
                            f"object upload failed ({response.status}) for {key}: {text[:500]}"
                        )
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == MAX_ATTEMPTS:
                    raise
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"object upload retries exhausted for {key}")
            await asyncio.sleep(min(2**attempt, 10))


async def upload_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    account_id: str,
    bucket: str,
    token: str,
    output: Path,
) -> int:
    data_root = output / "data"
    files = sorted(path for path in data_root.rglob("*") if path.is_file())
    for offset in range(0, len(files), 1000):
        batch = files[offset : offset + 1000]
        await asyncio.gather(
            *(
                upload_one(
                    session,
                    semaphore,
                    account_id,
                    bucket,
                    token,
                    path,
                    f"{PREFIX}/data/{path.relative_to(data_root).as_posix()}",
                )
                for path in batch
            )
        )
        print(
            json.dumps(
                {
                    "event": "entity_history_objects_uploaded",
                    "objects": min(offset + len(batch), len(files)),
                    "total": len(files),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    return len(files)


async def upload(args: argparse.Namespace) -> int:
    output: Path = args.output
    manifest = output / "generations.json"
    if not (output / "data").is_dir() or not manifest.is_file():
        raise RuntimeError(
            "object data and generations.json must be built before upload"
        )
    account_id = required_env("R2_ACCOUNT_ID", "CLOUDFLARE_ACCOUNT_ID")
    token = required_env("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_DEBUG_TOKEN")
    timeout = aiohttp.ClientTimeout(total=300)
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    semaphore = asyncio.Semaphore(args.concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        count = await upload_data(
            session,
            semaphore,
            account_id,
            args.bucket,
            token,
            output,
        )
        await upload_one(
            session,
            semaphore,
            account_id,
            args.bucket,
            token,
            manifest,
            f"{PREFIX}/generations.json",
        )
    print(
        json.dumps(
            {
                "bucket": args.bucket,
                "event": "entity_history_objects_published",
                "objects": count,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return count


def main() -> int:
    asyncio.run(upload(parse_args()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
