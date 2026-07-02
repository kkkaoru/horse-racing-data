"""Tests for the deployed NAR Set-Transformer scorer (numpy-only, predict_lib
coverage >=95 on all four metrics). File I/O is confined to tmp_path fixtures."""

from __future__ import annotations

import json
import sys
import zipfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

import numpy as np
import numpy.lib.format as npy_format
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import predict_lib.transformer_scorer as tsc

# ``_present_float`` is module-private (leading underscore); accessed via
# getattr with the attribute name held in a variable (not a string literal, so
# oxlint/ruff's "no getattr with constant attribute" check does not apply, and
# not a static ``tsc._present_float`` attribute expression, so strict
# pyright's reportPrivateUsage does not apply either) while still exercising
# the real implementation.
_PRESENT_FLOAT_ATTR: str = "_present_float"
_present_float: Callable[[object], float | None] = cast(
    Callable[[object], float | None], getattr(tsc, _PRESENT_FLOAT_ATTR)
)


def _savez(path: Path, arrays: Mapping[str, np.ndarray]) -> None:
    """Write ``arrays`` to an uncompressed ``.npz`` at ``path``.

    Equivalent to ``numpy.savez(path, **arrays)`` (a ``.npz`` is just a zip of
    per-array ``.npy`` entries), but avoids spreading a ``dict[str, ndarray]``
    into ``numpy.savez``'s ``**kwds: ArrayLike`` — which strict pyright flags
    because the same call also declares a named ``allow_pickle: bool``
    keyword any string key could theoretically collide with.
    """
    with zipfile.ZipFile(path, mode="w") as zf:
        for name, array in arrays.items():
            with zf.open(f"{name}.npy", mode="w") as fp:
                npy_format.write_array(fp, array)


# --------------------------------------------------------------------------
# _present_float — every branch
# --------------------------------------------------------------------------


def test_present_float_none_returns_none() -> None:
    assert _present_float(None) is None


def test_present_float_bool_true_returns_one() -> None:
    assert _present_float(True) == 1.0


def test_present_float_bool_false_returns_zero() -> None:
    assert _present_float(False) == 0.0


def test_present_float_int_returns_float() -> None:
    assert _present_float(7) == 7.0


def test_present_float_float_returns_float() -> None:
    assert _present_float(3.5) == 3.5


def test_present_float_nan_returns_none() -> None:
    assert _present_float(float("nan")) is None


def test_present_float_empty_string_returns_none() -> None:
    assert _present_float("") is None


def test_present_float_nan_string_returns_none() -> None:
    assert _present_float("nan") is None


def test_present_float_numeric_string_returns_float() -> None:
    assert _present_float("3.5") == 3.5


def test_present_float_bad_string_returns_none() -> None:
    assert _present_float("abc") is None


# --------------------------------------------------------------------------
# within_race_ordinal_rank
# --------------------------------------------------------------------------


def test_within_race_ordinal_rank_higher_score_is_rank_one() -> None:
    assert tsc.within_race_ordinal_rank([1.0, 3.0, 2.0], ["a", "b", "c"]) == [3, 1, 2]


def test_within_race_ordinal_rank_ties_break_on_ketto_ascending() -> None:
    assert tsc.within_race_ordinal_rank([5.0, 5.0], ["z9", "a1"]) == [2, 1]


# --------------------------------------------------------------------------
# within_race_zscore
# --------------------------------------------------------------------------


def test_within_race_zscore_std_greater_than_zero() -> None:
    # mean=2.0, population std=sqrt(2/3)~=0.8165 -> [-1.2247, 0.0, 1.2247]
    zs = tsc.within_race_zscore([1.0, 2.0, 3.0])
    assert zs == pytest.approx([-1.224744871, 0.0, 1.224744871])


def test_within_race_zscore_all_equal_is_all_zeros() -> None:
    assert tsc.within_race_zscore([4.0, 4.0, 4.0]) == [0.0, 0.0, 0.0]


def test_within_race_zscore_empty_returns_empty() -> None:
    assert tsc.within_race_zscore([]) == []


# --------------------------------------------------------------------------
# forward_rank_score — properties + masked-pad invariance + determinism
# --------------------------------------------------------------------------


def _tiny_weights(
    dim: int = 8, ff: int = 16, feats: int = 4, layers: int = 1
) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(0)
    w: dict[str, np.ndarray] = {
        "numeric_projection.weight": rng.standard_normal((dim, feats)),
        "numeric_projection.bias": rng.standard_normal(dim),
        "umaban_embedding.weight": rng.standard_normal((19, dim)),
        "input_layer_norm.weight": np.ones(dim),
        "input_layer_norm.bias": np.zeros(dim),
        "encoder.ln.weight": np.ones(dim),
        "encoder.ln.bias": np.zeros(dim),
        "rank_head.weight": rng.standard_normal((1, dim)),
        "rank_head.bias": rng.standard_normal(1),
    }
    for i in range(layers):
        p = f"encoder.layers.{i}"
        w[f"{p}.attention.query_proj.weight"] = rng.standard_normal((dim, dim))
        w[f"{p}.attention.key_proj.weight"] = rng.standard_normal((dim, dim))
        w[f"{p}.attention.value_proj.weight"] = rng.standard_normal((dim, dim))
        w[f"{p}.attention.out_proj.weight"] = rng.standard_normal((dim, dim))
        w[f"{p}.ln1.weight"] = np.ones(dim)
        w[f"{p}.ln1.bias"] = np.zeros(dim)
        w[f"{p}.ln2.weight"] = np.ones(dim)
        w[f"{p}.ln2.bias"] = np.zeros(dim)
        w[f"{p}.linear1.weight"] = rng.standard_normal((ff, dim))
        w[f"{p}.linear1.bias"] = rng.standard_normal(ff)
        w[f"{p}.linear2.weight"] = rng.standard_normal((dim, ff))
        w[f"{p}.linear2.bias"] = rng.standard_normal(dim)
    return w


def test_forward_rank_score_shape_and_finite() -> None:
    w = _tiny_weights()
    numeric = np.random.default_rng(1).standard_normal((2, 5, 4))
    umaban = np.array([[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]], dtype=np.int64)
    mask = np.ones((2, 5), dtype=bool)
    out = tsc.forward_rank_score(numeric, umaban, mask, w, num_layers=1, num_heads=2, eps=1e-5)
    assert out.shape == (2, 5)
    assert bool(np.isfinite(out).all())


def test_forward_rank_score_masked_pad_does_not_change_real_scores() -> None:
    w = _tiny_weights()
    rng = np.random.default_rng(2)
    real = rng.standard_normal((1, 3, 4))
    umaban = np.array([[1, 2, 3]], dtype=np.int64)
    mask3 = np.ones((1, 3), dtype=bool)
    base = tsc.forward_rank_score(real, umaban, mask3, w, num_layers=1, num_heads=2)
    padded = np.concatenate([real, rng.standard_normal((1, 2, 4))], axis=1)
    umaban5 = np.array([[1, 2, 3, 0, 0]], dtype=np.int64)
    mask5 = np.array([[True, True, True, False, False]])
    padded_out = tsc.forward_rank_score(padded, umaban5, mask5, w, num_layers=1, num_heads=2)
    assert float(np.abs(padded_out[0, :3] - base[0, :3]).max()) < 1e-9


def test_forward_rank_score_deterministic() -> None:
    w = _tiny_weights()
    numeric = np.random.default_rng(3).standard_normal((1, 4, 4))
    umaban = np.array([[1, 2, 3, 4]], dtype=np.int64)
    mask = np.ones((1, 4), dtype=bool)
    a = tsc.forward_rank_score(numeric, umaban, mask, w, num_layers=1, num_heads=2)
    b = tsc.forward_rank_score(numeric, umaban, mask, w, num_layers=1, num_heads=2)
    assert float(np.abs(a - b).max()) == 0.0


# --------------------------------------------------------------------------
# TransformerScorer / load_transformer / seed_rank_mean
# --------------------------------------------------------------------------


def _write_artifact(dir_path: Path, n_seeds: int, use_seed_files: bool) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    seed_files: list[str] = []
    for s in range(n_seeds):
        w = _tiny_weights()
        fname = f"weights_s{s + 1}.npz"
        _savez(dir_path / fname, {k: v.astype(np.float32) for k, v in w.items()})
        seed_files.append(fname)
    norm = {
        "arch": {
            "dim": 8,
            "num_layers": 1,
            "num_heads": 2,
            "eps": 1e-5,
            "umaban_vocab_size": 19,
            "max_runners": 18,
        },
        "feature_order": ["f0", "f1", "f2", "f3"],
        "mean": [0.0, 0.0, 0.0, 0.0],
        "std": [1.0, 1.0, 1.0, 1.0],
    }
    if use_seed_files:
        norm["seed_files"] = seed_files
    (dir_path / "norm.json").write_text(json.dumps(norm))


def test_load_transformer_with_seed_files(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=3, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    assert len(scorer.seeds) == 3


def test_load_transformer_glob_without_seed_files(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=2, use_seed_files=False)
    scorer = tsc.load_transformer(tmp_path)
    assert len(scorer.seeds) == 2


def test_load_transformer_single_weights_fallback(tmp_path: Path) -> None:
    w = _tiny_weights()
    _savez(tmp_path / "weights.npz", {k: v.astype(np.float32) for k, v in w.items()})
    (tmp_path / "norm.json").write_text(
        json.dumps(
            {
                "arch": {
                    "dim": 8,
                    "num_layers": 1,
                    "num_heads": 2,
                    "eps": 1e-5,
                    "umaban_vocab_size": 19,
                    "max_runners": 18,
                },
                "feature_order": ["f0", "f1", "f2", "f3"],
                "mean": [0.0, 0.0, 0.0, 0.0],
                "std": [1.0, 1.0, 1.0, 1.0],
            }
        )
    )
    scorer = tsc.load_transformer(tmp_path)
    assert len(scorer.seeds) == 1


def test_load_transformer_missing_seed_file_raises(tmp_path: Path) -> None:
    (tmp_path / "norm.json").write_text(
        json.dumps(
            {
                "arch": {
                    "dim": 8,
                    "num_layers": 1,
                    "num_heads": 2,
                    "eps": 1e-5,
                    "umaban_vocab_size": 19,
                    "max_runners": 18,
                },
                "feature_order": ["f0"],
                "mean": [0.0],
                "std": [1.0],
                "seed_files": ["weights_s1.npz"],
            }
        )
    )
    with pytest.raises(FileNotFoundError):
        tsc.load_transformer(tmp_path)


def test_load_transformer_raises_when_no_norm_manifest(tmp_path: Path) -> None:
    _savez(
        tmp_path / "weights.npz",
        {k: v.astype(np.float32) for k, v in _tiny_weights().items()},
    )
    with pytest.raises(FileNotFoundError):
        tsc.load_transformer(tmp_path)


def test_load_transformer_single_fallback_when_no_npz_and_no_seed_files(
    tmp_path: Path,
) -> None:
    (tmp_path / "norm.json").write_text(
        json.dumps(
            {
                "arch": {"num_layers": 1, "num_heads": 2, "eps": 1e-5, "max_runners": 18},
                "feature_order": ["f0"],
                "mean": [0.0],
                "std": [1.0],
            }
        )
    )
    with pytest.raises(FileNotFoundError):
        tsc.load_transformer(tmp_path)


def test_load_transformer_flat_schema_falls_back_to_alt_keys(tmp_path: Path) -> None:
    """A ``norm.json`` with NO nested ``arch`` (flat top-level keys) exercises the
    ``field()`` alt-key resolution: ``num_layers``/``num_heads``/``max_runners``
    are absent so the flat ``layers``/``heads``/``max_runners`` alt names are
    used instead, and ``eps`` (absent under either name) falls through to the
    hardcoded default."""
    w = _tiny_weights()
    _savez(tmp_path / "weights_s1.npz", {k: v.astype(np.float32) for k, v in w.items()})
    (tmp_path / "norm.json").write_text(
        json.dumps(
            {
                "feature_order": ["f0", "f1", "f2", "f3"],
                "mean": [0.0, 0.0, 0.0, 0.0],
                "std": [1.0, 1.0, 1.0, 1.0],
                "layers": 1,
                "heads": 2,
                "max_runners": 18,
                "seed_files": ["weights_s1.npz"],
            }
        )
    )
    scorer = tsc.load_transformer(tmp_path)
    assert scorer.num_layers == 1
    assert scorer.num_heads == 2
    assert scorer.eps == 1e-5
    assert scorer.max_runners == 18


def test_load_transformer_flat_schema_defaults_when_alt_keys_absent(tmp_path: Path) -> None:
    """Neither the nested ``arch`` key names NOR their flat alt names are
    present: every ``field()`` lookup falls through to its hardcoded default."""
    w = _tiny_weights()
    _savez(tmp_path / "weights_s1.npz", {k: v.astype(np.float32) for k, v in w.items()})
    (tmp_path / "norm.json").write_text(
        json.dumps(
            {
                "feature_order": ["f0", "f1", "f2", "f3"],
                "mean": [0.0, 0.0, 0.0, 0.0],
                "std": [1.0, 1.0, 1.0, 1.0],
                "seed_files": ["weights_s1.npz"],
            }
        )
    )
    scorer = tsc.load_transformer(tmp_path)
    assert scorer.num_layers == 3
    assert scorer.num_heads == 4
    assert scorer.max_runners == 18


def test_seed_rank_mean_two_seeds(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=2, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [
        {"f0": 1.0, "f1": 0.0, "f2": 0.0, "f3": 0.0, "umaban": 1},
        {"f0": 0.0, "f1": 1.0, "f2": 0.0, "f3": 0.0, "umaban": 2},
        {"f0": 0.0, "f1": 0.0, "f2": 1.0, "f3": 0.0, "umaban": 3},
    ]
    trm = scorer.seed_rank_mean(entries, ["a", "b", "c"])
    assert len(trm) == 3
    assert all(1.0 <= v <= 3.0 for v in trm)


def test_seed_scores_treats_absent_feature_as_zero(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    present = [
        {"f0": 0.0, "f1": 0.0, "f2": 0.0, "f3": 0.0, "umaban": 1},
        {"f0": 0.0, "f1": 0.0, "f2": 0.0, "f3": 0.0, "umaban": 2},
    ]
    absent = [{"umaban": 1}, {"umaban": 2}]  # every feature absent -> normalises to 0.0
    assert scorer.seed_scores(present) == scorer.seed_scores(absent)


def test_seed_rank_mean_handles_out_of_range_and_null_umaban(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [
        {"f0": 1.0, "f1": 0.0, "f2": 0.0, "f3": 0.0, "umaban": 99},
        {"f0": 0.0, "f1": 2.0, "f2": 0.0, "f3": 0.0, "umaban": None},
    ]
    trm = scorer.seed_rank_mean(entries, ["a", "b"])
    assert len(trm) == 2
    assert {round(v) for v in trm} == {1, 2}


def test_seed_score_mean_matches_hand_averaged_seed_scores(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=2, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [
        {"f0": 1.0, "f1": 0.0, "f2": 0.0, "f3": 0.0, "umaban": 1},
        {"f0": 0.0, "f1": 1.0, "f2": 0.0, "f3": 0.0, "umaban": 2},
        {"f0": 0.0, "f1": 0.0, "f2": 1.0, "f3": 0.0, "umaban": 3},
    ]
    per_seed = scorer.seed_scores(entries)
    mean = scorer.seed_score_mean(entries)
    assert len(mean) == 3
    assert mean == [sum(s[i] for s in per_seed) / len(per_seed) for i in range(len(entries))]


# --------------------------------------------------------------------------
# missing_feature_keys
# --------------------------------------------------------------------------


def test_missing_feature_keys_empty_entries_returns_empty_set(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    assert scorer.missing_feature_keys([]) == set()


def test_missing_feature_keys_empty_when_all_present(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [{"f0": 1.0, "f1": 2.0, "f2": 3.0, "f3": 4.0, "umaban": 1}]
    assert scorer.missing_feature_keys(entries) == set()


def test_missing_feature_keys_reports_absent_middle_feature(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [{"f0": 1.0, "f1": 2.0, "f3": 4.0, "umaban": 1}]  # f2 absent (middle)
    assert scorer.missing_feature_keys(entries) == {"f2"}


def test_missing_feature_keys_reports_absent_last_feature(tmp_path: Path) -> None:
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [{"f0": 1.0, "f1": 2.0, "f2": 3.0, "umaban": 1}]  # f3 absent (last)
    assert scorer.missing_feature_keys(entries) == {"f3"}


def test_missing_feature_keys_ignores_null_value_present_key(tmp_path: Path) -> None:
    """A present key with a null value is NOT missing -- only a structurally
    absent key is flagged."""
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [{"f0": 1.0, "f1": None, "f2": 3.0, "f3": 4.0, "umaban": 1}]
    assert scorer.missing_feature_keys(entries) == set()


def test_missing_feature_keys_only_checks_representative_first_entry(
    tmp_path: Path,
) -> None:
    """Only the first entry's keys gate the guard -- a later entry missing a
    key is not itself inspected (the race-level feature build is uniform)."""
    _write_artifact(tmp_path, n_seeds=1, use_seed_files=True)
    scorer = tsc.load_transformer(tmp_path)
    entries = [
        {"f0": 1.0, "f1": 2.0, "f2": 3.0, "f3": 4.0, "umaban": 1},
        {"f0": 1.0, "f1": 2.0, "umaban": 2},  # f2/f3 absent here, but not first
    ]
    assert scorer.missing_feature_keys(entries) == set()


# --------------------------------------------------------------------------
# fuse_ensemble_transformer
# --------------------------------------------------------------------------


def test_fuse_ensemble_transformer_blends_zscores() -> None:
    # z([2.0, 1.0]) pop std: mean=1.5, std=0.5 -> [+1.0, -1.0]
    # z([10.0, 1.0]) pop std: mean=5.5, std=4.5 -> [+1.0, -1.0]
    # fused = 0.5*[+1,-1] + 0.5*[+1,-1] = [1.0, -1.0]
    fused = tsc.fuse_ensemble_transformer([10.0, 1.0], [2.0, 1.0], 0.5)
    assert fused == pytest.approx([1.0, -1.0])


def test_fuse_ensemble_transformer_weight_zero_is_base_only() -> None:
    # weight_transformer=0.0 -> fused == z(base_scores) exactly (mean=5.5, std=4.5).
    fused = tsc.fuse_ensemble_transformer([10.0, 1.0], [1.0, 2.0], 0.0)
    assert fused == pytest.approx(tsc.within_race_zscore([10.0, 1.0]))
    assert fused == pytest.approx([1.0, -1.0])


def test_fuse_ensemble_transformer_default_weight_is_half() -> None:
    fused_default = tsc.fuse_ensemble_transformer([1.0, 10.0], [1.0, 2.0])
    fused_explicit = tsc.fuse_ensemble_transformer([1.0, 10.0], [1.0, 2.0], 0.5)
    assert fused_default == fused_explicit
