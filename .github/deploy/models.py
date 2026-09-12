"""Publish/restore immutable, digest-checked model bytes for Linux CI builds."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Protocol

import boto3
from predict_lib.artifact_integrity import (
    ArtifactSpec,
    derive_selected_artifact_keys,
    load_manifest,
    verify_selector_closure,
)


class ObjectStore(Protocol):
    def download_file(self, Bucket: str, Key: str, Filename: str) -> None: ...
    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None: ...


def object_key(artifact: ArtifactSpec) -> str:
    return f"ci-artifacts/sha256/{artifact.sha256}"


def check_bytes(path: Path, artifact: ArtifactSpec) -> None:
    if path.stat().st_size != artifact.size_bytes:
        raise ValueError(f"Model size mismatch: {artifact.serving_key}")
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != artifact.sha256:
        raise ValueError(f"Model digest mismatch: {artifact.serving_key}")


def transfer(
    *, store: ObjectStore, artifact: ArtifactSpec, root: Path, publish: bool, bucket: str
) -> None:
    if artifact.container_path is None:
        raise ValueError("Selected container artifact has no container_path")
    path = root / artifact.container_path
    if publish:
        check_bytes(path, artifact)
        store.upload_file(str(path), bucket, object_key(artifact))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".download")
        try:
            store.download_file(bucket, object_key(artifact), str(temporary))
            check_bytes(temporary, artifact)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--extra-manifest", type=Path)
    args = parser.parse_args()
    manifest = load_manifest()
    selected = derive_selected_artifact_keys()
    report = verify_selector_closure(manifest, selected)
    if report.status != "MATCH":
        raise ValueError("Production selectors are not covered by the artifact manifest")
    store = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    artifacts = [
        item
        for item in manifest.artifacts
        if item.system == "finish-position" and item.serving_key in selected
    ]
    if args.extra_manifest is not None:
        artifacts.extend(load_manifest(args.extra_manifest).artifacts)
    for artifact in artifacts:
        transfer(
            store=store,
            artifact=artifact,
            root=args.root,
            publish=args.publish,
            bucket=os.environ["PRODUCTION_ARTIFACT_BUCKET"],
        )
    print(
        f"Verified {'published' if args.publish else 'restored'} model artifacts: {len(artifacts)}"
    )


if __name__ == "__main__":
    main()
