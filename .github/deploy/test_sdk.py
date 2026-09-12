"""A failed SDK restore must preserve the previous verified installer."""

from pathlib import Path
from unittest.mock import Mock

import pytest

import sdk


def test_verified_installer_replaces_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sdk, "SDK_SIZE", 3)
    monkeypatch.setattr(
        sdk, "SDK_SHA256", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    store = Mock()
    store.download_file.side_effect = lambda bucket, key, filename: Path(filename).write_bytes(
        b"abc"
    )
    destination = tmp_path / "sdk" / "JVLinkSetup.exe"
    sdk.restore(store=store, bucket="private", destination=destination)
    assert destination.read_bytes() == b"abc"
    assert destination.with_suffix(".download").exists() is False


@pytest.mark.parametrize(("size", "message"), [(999, "size mismatch"), (3, "digest mismatch")])
def test_corrupt_installer_preserves_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: int, message: str
) -> None:
    monkeypatch.setattr(sdk, "SDK_SIZE", size)
    store = Mock()
    store.download_file.side_effect = lambda bucket, key, filename: Path(filename).write_bytes(
        b"bad"
    )
    destination = tmp_path / "JVLinkSetup.exe"
    destination.write_bytes(b"previous")
    with pytest.raises(ValueError, match=message):
        sdk.restore(store=store, bucket="private", destination=destination)
    assert destination.read_bytes() == b"previous"
    assert destination.with_suffix(".download").exists() is False


def test_unavailable_store_fails_closed(tmp_path: Path) -> None:
    store = Mock()
    store.download_file.side_effect = OSError("unavailable")
    with pytest.raises(OSError, match="unavailable"):
        sdk.restore(store=store, bucket="private", destination=tmp_path / "JVLinkSetup.exe")


def test_cli_restores_sdk_from_private_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://example.invalid")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("PRODUCTION_ARTIFACT_BUCKET", "private")
    client = Mock()
    monkeypatch.setattr(sdk.boto3, "client", Mock(return_value=client))
    restore = Mock()
    monkeypatch.setattr(sdk, "restore", restore)
    sdk.main()
    restore.assert_called_once_with(
        store=client,
        bucket="private",
        destination=Path("apps/jra-van-datalab-cloudflare-demo/sdk/JVLinkSetup.exe"),
    )
