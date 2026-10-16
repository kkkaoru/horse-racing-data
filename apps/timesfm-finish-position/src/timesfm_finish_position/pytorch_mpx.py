"""PyTorch MPS race-set ranker with a hard ban on win-odds features."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import torch
from torch import nn

MAX_RUNNERS = 18
DEFAULT_EMBEDDING_DIM = 64
DEFAULT_HEADS = 4
DEFAULT_LAYERS = 2
DEFAULT_DROPOUT = 0.1
ATTENTION_NEG_INF = -1e9
ODDS_FEATURE_MARKERS: tuple[str, ...] = (
    "odds",
    "tansho",
    "tanpuku",
    "ninki",
    "implied_prob",
    "market_share",
    "popularity",
)
CURRENT_RACE_OUTCOME_FEATURES: frozenset[str] = frozenset(
    {
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_2_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
    }
)
IDENTITY_COLUMNS: frozenset[str] = frozenset(
    {
        "race_id",
        "race_date",
        "race_year",
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "bamei",
        "kishumei_ryakusho",
        "chokyoshimei_ryakusho",
        "category",
        "umaban",
        "finish_position",
        "kyosomei_hondai",
        "kyosomei_fukudai",
        "kyosomei_kakkonai",
        "kyosomei_higo",
        "grade_code",
        "track_code",
        "kyoso_joken_code",
    }
)


def is_banned_odds_feature(name: str) -> bool:
    """Return True when a column is win-odds or a direct popularity proxy."""
    lowered = name.lower()
    return any(marker in lowered for marker in ODDS_FEATURE_MARKERS)


def is_current_race_outcome_feature(name: str) -> bool:
    """Return True for labels from the race being predicted."""
    return name in CURRENT_RACE_OUTCOME_FEATURES or name.startswith("target_")


def select_numeric_features(names: Sequence[str]) -> list[str]:
    """Keep numeric model inputs and drop identity, odds, and current-race labels."""
    selected: list[str] = []
    for name in names:
        if name in IDENTITY_COLUMNS or is_banned_odds_feature(name):
            continue
        if is_current_race_outcome_feature(name):
            continue
        selected.append(name)
    if not selected:
        raise ValueError("no odds-free numeric features remain")
    return selected


def resolve_mpx_device(preference: str = "auto") -> torch.device:
    """Prefer Apple MPS, then CUDA, then CPU."""
    if preference == "cpu":
        return torch.device("cpu")
    if preference == "mps":
        if not torch.backends.mps.is_available():
            raise RuntimeError("MPS training was requested but is unavailable")
        return torch.device("mps")
    if preference == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA training was requested but is unavailable")
        return torch.device("cuda")
    if preference != "auto":
        raise ValueError(f"unsupported torch training device: {preference}")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def graded_relevance(positions: np.ndarray) -> np.ndarray:
    """Convert finish positions into a race-normalized ListNet target."""
    relevance = np.zeros(len(positions), dtype=np.float64)
    for index, position in enumerate(positions):
        value = float(position)
        if np.isfinite(value) and value >= 1.0:
            relevance[index] = 1.0 / np.log2(1.0 + value)
    total = float(relevance.sum())
    if total <= 0.0:
        return relevance
    return relevance / total


def pad_races(
    features: Sequence[np.ndarray],
    targets: Sequence[np.ndarray],
    *,
    width: int,
    maximum_runners: int = MAX_RUNNERS,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Pad variable-size races into dense (batch, runners, features) tensors."""
    if len(features) != len(targets):
        raise ValueError("features and targets must contain the same races")
    batch = len(features)
    padded = torch.zeros((batch, maximum_runners, width), dtype=torch.float32)
    relevance = torch.zeros((batch, maximum_runners), dtype=torch.float32)
    mask = torch.zeros((batch, maximum_runners), dtype=torch.bool)
    for row, (values, target) in enumerate(zip(features, targets, strict=True)):
        matrix = np.asarray(values, dtype=np.float32)
        if matrix.ndim == 1:
            matrix = matrix.reshape(1, -1)
        if matrix.ndim != 2 or matrix.shape[1] != width:
            raise ValueError(
                "each race feature matrix must have shape (runners, width) "
                f"got {matrix.shape} width={width}"
            )
        target_vector = np.asarray(target, dtype=np.float32).reshape(-1)
        if matrix.shape[0] != target_vector.shape[0]:
            raise ValueError("runner count must match the relevance target")
        count = min(int(matrix.shape[0]), maximum_runners)
        padded[row, :count] = torch.from_numpy(matrix[:count])
        relevance[row, :count] = torch.from_numpy(target_vector[:count])
        mask[row, :count] = True
    return padded, relevance, mask


def listnet_loss(logits: torch.Tensor, targets: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    """Masked ListNet cross-entropy over a race."""
    masked_logits = logits.masked_fill(~mask, ATTENTION_NEG_INF)
    log_probability = torch.log_softmax(masked_logits, dim=-1)
    return -(targets * log_probability).sum(dim=-1).mean()


def winner_contained_hits(
    scores: Sequence[np.ndarray],
    positions: Sequence[np.ndarray],
) -> dict[str, int]:
    """Count races whose winner sits inside predicted Top1..Top5."""
    if len(scores) != len(positions):
        raise ValueError("scores and positions must contain the same races")
    hits = {f"top{rank}": 0 for rank in range(1, 6)}
    for score, race_positions in zip(scores, positions, strict=True):
        if len(score) == 0 or len(score) != len(race_positions):
            raise ValueError("each race must have aligned scores and positions")
        winner = int(np.argmin(race_positions))
        order = np.argsort(-np.asarray(score), kind="stable")
        rank = int(np.where(order == winner)[0][0]) + 1
        if rank <= 1:
            hits["top1"] += 1
        if rank <= 2:
            hits["top2"] += 1
        if rank <= 3:
            hits["top3"] += 1
        if rank <= 4:
            hits["top4"] += 1
        if rank <= 5:
            hits["top5"] += 1
    return hits


class RaceSetTransformer(nn.Module):
    """Listwise set transformer that scores every horse against the rest of the race."""

    def __init__(
        self,
        feature_count: int,
        *,
        embedding_dim: int = DEFAULT_EMBEDDING_DIM,
        heads: int = DEFAULT_HEADS,
        layers: int = DEFAULT_LAYERS,
        dropout: float = DEFAULT_DROPOUT,
    ) -> None:
        super().__init__()
        if feature_count < 1:
            raise ValueError("feature_count must be positive")
        if embedding_dim < 1 or heads < 1 or layers < 0:
            raise ValueError("transformer sizes must be positive")
        if layers > 0 and embedding_dim % heads != 0:
            raise ValueError("embedding_dim must be divisible by heads")
        if not 0.0 <= dropout < 1.0:
            raise ValueError("dropout must be between zero and one")
        self.input = nn.Linear(feature_count, embedding_dim)
        self.norm = nn.LayerNorm(embedding_dim)
        self.encoder: nn.TransformerEncoder | None = None
        if layers > 0:
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=embedding_dim,
                nhead=heads,
                dim_feedforward=embedding_dim * 4,
                dropout=dropout,
                activation="gelu",
                batch_first=True,
                norm_first=True,
            )
            self.encoder = nn.TransformerEncoder(
                encoder_layer,
                num_layers=layers,
                enable_nested_tensor=False,
            )
        self.head = nn.Linear(embedding_dim, 1)

    def forward(self, features: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        hidden = self.norm(self.input(features))
        encoded = hidden
        if self.encoder is not None:
            encoded = self.encoder(hidden, src_key_padding_mask=~mask)
        scores = self.head(encoded).squeeze(-1)
        return scores.masked_fill(~mask, ATTENTION_NEG_INF)
