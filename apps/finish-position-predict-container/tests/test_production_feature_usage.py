from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib import production_feature_usage
from predict_lib.production_feature_usage import (
    RELATIONSHIP_FEATURE_NAMES,
    relationship_layer_is_provably_unused,
)


def _write_json(root: Path, key: str, payload: object) -> None:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_relationship_layer_skips_only_when_every_selected_contract_excludes_fields(
    tmp_path: Path,
) -> None:
    base = "finish-position/jra/base"
    variant = "finish-position/jra/variant"
    keys = {
        f"{base}/model.json",
        f"{base}/metadata.json",
        f"{variant}/model.json",
        f"{variant}/metadata.json",
    }
    _write_json(tmp_path, f"{base}/metadata.json", {"feature_names": ["speed"]})
    _write_json(tmp_path, f"{variant}/metadata.json", {"feature_names": ["odds"]})

    assert relationship_layer_is_provably_unused("jra", artifact_root=tmp_path, selected_keys=keys)


def test_relationship_layer_runs_when_any_selected_contract_uses_a_field(
    tmp_path: Path,
) -> None:
    key = "finish-position/jra/base/metadata.json"
    _write_json(
        tmp_path,
        key,
        {"feature_names": ["speed", next(iter(RELATIONSHIP_FEATURE_NAMES))]},
    )

    assert not relationship_layer_is_provably_unused(
        "jra", artifact_root=tmp_path, selected_keys={key}
    )


def test_relationship_layer_checks_transformer_feature_order(tmp_path: Path) -> None:
    prefix = "finish-position/nar/transformer"
    keys = {f"{prefix}/norm.json", f"{prefix}/weights_s1.npz"}
    _write_json(tmp_path, f"{prefix}/norm.json", {"feature_order": ["speed"]})

    assert relationship_layer_is_provably_unused("nar", artifact_root=tmp_path, selected_keys=keys)

    _write_json(
        tmp_path,
        f"{prefix}/norm.json",
        {"feature_order": ["past_speed_volatility_5"]},
    )
    assert not relationship_layer_is_provably_unused(
        "nar", artifact_root=tmp_path, selected_keys=keys
    )


def test_relationship_layer_fails_closed_for_incomplete_or_unknown_contracts(
    tmp_path: Path,
) -> None:
    prefix = "finish-position/jra/base"
    cases = (
        set(),
        {f"{prefix}/model.json"},
        {f"{prefix}/manifest.json"},
        {f"{prefix}/metadata.json"},
        {f"{prefix}/weights_s1.npz"},
    )
    _write_json(tmp_path, f"{prefix}/metadata.json", {"other": ["speed"]})

    for keys in cases:
        assert not relationship_layer_is_provably_unused(
            "jra", artifact_root=tmp_path, selected_keys=keys
        )


def test_relationship_layer_fails_closed_for_missing_or_malformed_contract(
    tmp_path: Path,
) -> None:
    key = "finish-position/jra/base/metadata.json"
    assert not relationship_layer_is_provably_unused(
        "jra", artifact_root=tmp_path, selected_keys={key}
    )
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    path.write_text("{", encoding="utf-8")
    assert not relationship_layer_is_provably_unused(
        "jra", artifact_root=tmp_path, selected_keys={key}
    )


@pytest.mark.parametrize(
    "payload",
    (
        [],
        {"feature_names": "speed"},
        {"feature_names": []},
        {"feature_names": ["speed", 1]},
        {"feature_names": ["speed", ""]},
    ),
)
def test_relationship_layer_fails_closed_for_invalid_feature_names(
    tmp_path: Path, payload: object
) -> None:
    key = "finish-position/jra/base/metadata.json"
    _write_json(tmp_path, key, payload)

    assert not relationship_layer_is_provably_unused(
        "jra", artifact_root=tmp_path, selected_keys={key}
    )


def test_relationship_layer_fails_closed_when_selector_derivation_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_derivation() -> frozenset[str]:
        raise ValueError("bad selector")

    monkeypatch.setattr(production_feature_usage, "derive_selected_artifact_keys", fail_derivation)

    assert not relationship_layer_is_provably_unused("jra", artifact_root=tmp_path)


def test_tracked_selected_artifacts_exclude_relationship_features() -> None:
    root = Path(__file__).resolve().parent.parent
    models = root / "models"

    assert relationship_layer_is_provably_unused("jra", artifact_root=models)
    assert relationship_layer_is_provably_unused("nar", artifact_root=models)
