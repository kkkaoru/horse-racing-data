"""Restore the private JV-Link SDK required by the existing Container build."""

import hashlib
import os
from pathlib import Path

import boto3

from models import ObjectStore

SDK_SHA256 = "03ea24d98978a472cb0acf8d3278370978d7dc6943ea1f58d577fe259430bdc7"
SDK_SIZE = 18144528
SDK_PATH = Path("apps/jra-van-datalab-cloudflare-demo/sdk/JVLinkSetup.exe")


def restore(*, store: ObjectStore, bucket: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".download")
    try:
        store.download_file(bucket, f"ci-artifacts/sha256/{SDK_SHA256}", str(temporary))
        if temporary.stat().st_size != SDK_SIZE:
            raise ValueError("JV-Link SDK size mismatch")
        with temporary.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != SDK_SHA256:
            raise ValueError("JV-Link SDK digest mismatch")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    store = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    restore(store=store, bucket=os.environ["PRODUCTION_ARTIFACT_BUCKET"], destination=SDK_PATH)
    print("Verified private JV-Link SDK installer")


if __name__ == "__main__":
    main()
