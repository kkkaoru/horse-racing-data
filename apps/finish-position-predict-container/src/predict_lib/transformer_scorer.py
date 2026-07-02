"""Deployed numpy Race Set Transformer scorer for NAR — predict_lib.transformer_scorer.

This module is the ONE deployable implementation of the transformer forward +
seed combination + rank fusion. It supersedes candidate-mlx-nar/numpy_forward.py:
the deploy-gate is re-run through THIS module so eval == serve by construction.
Target production model_version: iter40-nar-settransformer-blend-v1.

Dependency: numpy ONLY (already in the container via pandas/pyarrow). No MLX, no
Metal, no polars, no torch. np.load reads the MLX-written .npz directly.

Fidelity (see parity_results.json): the float64 forward is bit-exact to MLX
*eager* execution (max|diff| ~1e-6, 0 rank flips on real 2025 NAR). The historical
"0.998 corr" was MLX *graph-mode* lazy-fusion float32 noise — unbiased on the gate
metric, not reproducible across device/version. float64 gives one deterministic,
host-invariant score (Linux/x86 container vs Mac/ARM trainer).

Target artifact: 117-feature listwise Set Transformer, 3 seeds, all-history.
The 117 baseline is a verified subset of the container's iter12 NAR feature
build (0 missing), so it is feature-contract-safe (no retrain-on-192, no layer
additions needed).

Artifact layout (produced by export_artifact.py / mlx-nar):
  <dir>/norm.json     {feature_order[117], mean[117], std[117], arch{...},
                       seed_files:["weights_s1.npz","weights_s2.npz","weights_s3.npz"]}
  <dir>/weights_s*.npz   numpy float32 weights, keys = MLX param paths (per seed)

Gate-exact fusion (mirrors eval_prodbase.build_join, deploy variant ``score_z_55``):
  per seed i:  seed_score_i = per-horse rank_score (higher = better)
  tf_score_mean = mean_i(seed_score_i)               # mean of seed SCORES, not ranks
  base_score    = the NAR base (iter12 XGBoost) per-horse score
  fused = 0.5 * znorm(tf_score_mean) + 0.5 * znorm(base_score)  # znorm = within-race
          z-normalisation (population std, ddof=0); higher = better -> rank_within_race
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

import numpy as np

_NEG_INF: Final[float] = -1e9


# ---------------------------------------------------------------------------
# Core forward (batched, float64) — CANONICAL, == numpy_forward.forward_rank_score
# ---------------------------------------------------------------------------


def _ln(x: np.ndarray, w: np.ndarray, b: np.ndarray, eps: float) -> np.ndarray:
    mu = x.mean(-1, keepdims=True)
    var = x.var(-1, keepdims=True)  # population variance (ddof=0) == mx.fast.layer_norm
    return (x - mu) / np.sqrt(var + eps) * w + b


def _softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    x = x - x.max(axis=axis, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=axis, keepdims=True)


def _mha(
    x: np.ndarray, w: Mapping[str, np.ndarray], prefix: str, add_mask: np.ndarray, nheads: int
) -> np.ndarray:
    b, ll, d = x.shape
    hd = d // nheads
    q = x @ w[f"{prefix}.query_proj.weight"].T
    k = x @ w[f"{prefix}.key_proj.weight"].T
    v = x @ w[f"{prefix}.value_proj.weight"].T
    q = q.reshape(b, ll, nheads, hd).transpose(0, 2, 1, 3)
    k = k.reshape(b, ll, nheads, hd).transpose(0, 2, 1, 3)
    v = v.reshape(b, ll, nheads, hd).transpose(0, 2, 1, 3)
    scale = 1.0 / math.sqrt(hd)
    scores = (q * scale) @ k.transpose(0, 1, 3, 2)
    scores = scores + add_mask[:, None, :, :]
    attn = _softmax(scores, axis=-1)
    out = (attn @ v).transpose(0, 2, 1, 3).reshape(b, ll, d)
    return out @ w[f"{prefix}.out_proj.weight"].T


def forward_rank_score(
    numeric: np.ndarray,
    umaban: np.ndarray,
    mask: np.ndarray,
    weights: Mapping[str, np.ndarray],
    num_layers: int = 3,
    num_heads: int = 4,
    eps: float = 1e-5,
) -> np.ndarray:
    """Batched rank_score forward. Signature-compatible with
    numpy_forward.forward_rank_score so the deploy gate can call it directly.

    numeric (B,L,F) ALREADY-NORMALISED, umaban (B,L) int, mask (B,L) bool.
    Returns (B,L) rank_score (higher = better). Runs in the dtype of ``numeric``
    (pass float64 for the canonical deterministic path).
    """
    w = weights
    add_mask = np.where(mask, 0.0, _NEG_INF)[:, None, :].astype(numeric.dtype)
    x = numeric @ w["numeric_projection.weight"].T + w["numeric_projection.bias"]
    x = x + w["umaban_embedding.weight"][umaban]
    x = _ln(x, w["input_layer_norm.weight"], w["input_layer_norm.bias"], eps)
    for i in range(num_layers):
        p = f"encoder.layers.{i}"
        y = _ln(x, w[f"{p}.ln1.weight"], w[f"{p}.ln1.bias"], eps)
        y = _mha(y, w, f"{p}.attention", add_mask, num_heads)
        x = x + y
        y = _ln(x, w[f"{p}.ln2.weight"], w[f"{p}.ln2.bias"], eps)
        y = np.maximum(y @ w[f"{p}.linear1.weight"].T + w[f"{p}.linear1.bias"], 0.0)
        y = y @ w[f"{p}.linear2.weight"].T + w[f"{p}.linear2.bias"]
        x = x + y
    x = _ln(x, w["encoder.ln.weight"], w["encoder.ln.bias"], eps)
    return (x @ w["rank_head.weight"].T + w["rank_head.bias"]).squeeze(-1)


# ---------------------------------------------------------------------------
# Within-race ordinal rank (deterministic tie-break) — shared by base + seeds
# ---------------------------------------------------------------------------


def within_race_ordinal_rank(scores: Sequence[float], ketto: Sequence[str]) -> list[int]:
    """1-based within-race ordinal rank (higher score = rank 1). Ties break on
    ketto_toroku_bango ascending — matches predict_lib.rank._sort_key so serve
    ranking is deterministic and idempotent across re-runs."""
    order = sorted(range(len(scores)), key=lambda i: (-scores[i], ketto[i]))
    rank = [0] * len(scores)
    for r, idx in enumerate(order):
        rank[idx] = r + 1
    return rank


def within_race_zscore(scores: Sequence[float]) -> list[float]:
    """Within-race z-normalisation: ``(x - mean) / std`` (population std,
    ddof=0), or all-zeros when std == 0. Matches the score-fusion gate's
    ``znorm`` (polars ``std(ddof=0)``) so eval == serve. Scale-invariant, so the
    fused blend is robust to differing base-score magnitudes."""
    n = len(scores)
    if n == 0:
        return []
    arr = np.asarray(scores, dtype=np.float64)
    mean = float(arr.mean())
    std = float(arr.std())  # numpy default ddof=0 == polars std(ddof=0)
    if std > 0.0:
        return [float((value - mean) / std) for value in arr]
    return [0.0] * n


# ---------------------------------------------------------------------------
# Scorer (multi-seed)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransformerScorer:
    seeds: tuple[dict[str, np.ndarray], ...]  # per-seed float64 weight dicts
    feature_order: tuple[str, ...]
    mean: np.ndarray  # (F,) float64
    std: np.ndarray  # (F,) float64
    num_layers: int
    num_heads: int
    eps: float
    umaban_vocab_size: int
    max_runners: int

    def _normalise(self, entries: Sequence[Mapping[str, object]]) -> tuple[np.ndarray, np.ndarray]:
        n = len(entries)
        numeric = np.zeros((1, n, len(self.feature_order)), dtype=np.float64)
        umaban = np.zeros((1, n), dtype=np.int64)
        for h, entry in enumerate(entries):
            for f, name in enumerate(self.feature_order):
                v = _present_float(entry.get(name))
                if v is not None:
                    numeric[0, h, f] = (v - self.mean[f]) / self.std[f]
            u = _present_float(entry.get("umaban"))
            iu = int(u) if u is not None else 0
            umaban[0, h] = iu if 0 <= iu < self.umaban_vocab_size else 0
        return numeric, umaban

    def seed_scores(self, entries: Sequence[Mapping[str, object]]) -> list[list[float]]:
        """Per-seed per-horse rank_score for one race (list over seeds)."""
        numeric, umaban = self._normalise(entries)
        mask = np.ones((1, len(entries)), dtype=bool)
        out: list[list[float]] = []
        for w in self.seeds:
            s = forward_rank_score(
                numeric, umaban, mask, w, self.num_layers, self.num_heads, self.eps
            )
            out.append([float(x) for x in s[0]])
        return out

    def seed_rank_mean(
        self, entries: Sequence[Mapping[str, object]], ketto: Sequence[str]
    ) -> list[float]:
        """The gate's ``_trm``: mean over seeds of each seed's within-race
        ordinal rank. Lower = better (it is a rank). This was the transformer
        contribution consumed by the earlier rank-level fusion (superseded by
        :meth:`seed_score_mean` -> :func:`fuse_ensemble_transformer`)."""
        per_seed = self.seed_scores(entries)
        seed_ranks = [within_race_ordinal_rank(s, ketto) for s in per_seed]
        n = len(entries)
        return [sum(r[i] for r in seed_ranks) / len(seed_ranks) for i in range(n)]

    def seed_score_mean(self, entries: Sequence[Mapping[str, object]]) -> list[float]:
        """Mean over seeds of each seed's raw per-horse rank_score (the gate's
        ``tf_score_mean``). This is the transformer contribution consumed by the
        score-level :func:`fuse_ensemble_transformer` (z-normalised there)."""
        per_seed = self.seed_scores(entries)
        n = len(entries)
        return [sum(s[i] for s in per_seed) / len(per_seed) for i in range(n)]

    def missing_feature_keys(self, entries: Sequence[Mapping[str, object]]) -> set[str]:
        """Return the ``feature_order`` names absent as KEYS from the race's
        representative (first) entry — a fail-closed feature-contract guard.

        Present-but-null / NaN values are NOT flagged (they are informative
        absences the transformer normalises to 0.0, matching training). Only a
        structurally missing column (feature-layer drift) is reported so the
        caller can fall back to the ensemble-only ranking. An empty race returns
        an empty set (the field<2 guard handles it upstream)."""
        if not entries:
            return set()
        keys = set(entries[0].keys())
        return {name for name in self.feature_order if name not in keys}


def _present_float(value: object) -> float | None:
    """Float value, or None when absent / null / empty / NaN (-> normalises to 0)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return None if math.isnan(f) else f
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _read_norm(d: Path) -> dict[str, object]:
    """Read the normalisation manifest, accepting either the production
    ``norm_stats.json`` (mlx-nar serve artifact) or the ``norm.json`` schema."""
    for name in ("norm_stats.json", "norm.json"):
        p = d / name
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError(f"no norm_stats.json / norm.json in {d}")


def _seed_paths(d: Path, norm: Mapping[str, object]) -> list[Path]:
    """Resolve the seed weight files. Order: explicit ``seed_files`` -> sorted
    glob of *.npz (production ``c2nm_prod_s*.npz`` / ``weights_*.npz``) ->
    single ``weights.npz``."""
    seed_files = norm.get("seed_files")
    if isinstance(seed_files, list) and seed_files:
        return [d / str(f) for f in seed_files]
    globbed = sorted(p for p in d.glob("*.npz"))
    if globbed:
        return globbed
    return [d / "weights.npz"]


def load_transformer(artifact_dir: str | Path) -> TransformerScorer:
    """Load the norm manifest + all seed weight files into a multi-seed scorer.

    numpy-only. Accepts the production ``norm_stats.json`` (keys
    ``numeric_mean``/``numeric_std``/flat ``dim``/``layers``/``heads``/
    ``max_runners``) or the ``norm.json`` schema (``mean``/``std``/nested
    ``arch``). ``umaban_vocab_size`` and ``eps`` are derived from the weights /
    defaulted (1e-5) since the production manifest omits them. Raises when a
    seed weight file is missing (caller falls back to ensemble-only)."""
    d = Path(artifact_dir)
    norm = _read_norm(d)
    # nested (norm.json) or flat (norm_stats.json); cast documents the JSON
    # object contract (both branches are always a JSON object at this schema
    # level) without altering the runtime value.
    arch = cast("dict[str, object]", norm.get("arch", norm))

    def field(key: str, alt: str, default: float) -> float:
        if key in arch:
            return cast(float, arch[key])
        if alt in norm:
            return cast(float, norm[alt])
        return default

    seeds: list[dict[str, np.ndarray]] = []
    for p in _seed_paths(d, norm):
        if not p.exists():
            raise FileNotFoundError(f"transformer seed weights missing: {p}")
        npz = np.load(p)
        seeds.append({k: np.asarray(npz[k], dtype=np.float64) for k in npz.files})

    mean = norm.get("numeric_mean", norm.get("mean"))
    std = norm.get("numeric_std", norm.get("std"))
    # umaban vocab is authoritative from the embedding weight, not the manifest.
    umaban_vocab = int(seeds[0]["umaban_embedding.weight"].shape[0])
    return TransformerScorer(
        seeds=tuple(seeds),
        feature_order=tuple(cast("list[str]", norm["feature_order"])),
        mean=np.asarray(mean, dtype=np.float64),
        std=np.asarray(std, dtype=np.float64),
        num_layers=int(field("num_layers", "layers", 3)),
        num_heads=int(field("num_heads", "heads", 4)),
        eps=float(field("eps", "eps", 1e-5)),
        umaban_vocab_size=umaban_vocab,
        max_runners=int(field("max_runners", "max_runners", 18)),
    )


# ---------------------------------------------------------------------------
# Rank fusion — EXACT mirror of the deploy gate (eval_prodbase.build_join)
# ---------------------------------------------------------------------------


def fuse_ensemble_transformer(
    base_scores: Sequence[float],
    transformer_score_mean: Sequence[float],
    weight_transformer: float = 0.5,
) -> list[float]:
    """Score-level z-fusion of the NAR base score with the transformer's mean
    seed score (deploy variant ``score_z_55``).

    ``base_scores`` = the NAR base (iter12 XGBoost) per-horse score;
    ``transformer_score_mean`` = :meth:`TransformerScorer.seed_score_mean`
    output. Both are within-race z-normalised (scale-invariant) then blended:
        fused[i] = w * z(transformer_score_mean)[i] + (1-w) * z(base_scores)[i]
    Higher = better -> rank_within_race (ketto tie-break applied by the caller).
    Replaces the earlier rank-level fusion; 5-fold CONFIRMED over it: top1
    +0.253 [LB95 +0.120] / place2 +0.341 [+0.166] / place3 +0.230 [+0.051]."""
    transformer_z = within_race_zscore(transformer_score_mean)
    base_z = within_race_zscore(base_scores)
    return [
        weight_transformer * transformer_z[i] + (1.0 - weight_transformer) * base_z[i]
        for i in range(len(base_scores))
    ]
