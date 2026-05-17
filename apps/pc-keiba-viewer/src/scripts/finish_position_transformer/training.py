#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingTypeStubs=false, reportAttributeAccessIssue=false, reportUnknownParameterType=false, reportMissingParameterType=false
"""Multi-task training loop for the Race Set Transformer.

Loss = w_top1 * BCE(top1) + w_top3 * BCE(top3) + w_pair * pairwise ranking.
Optimizer = AdamW with linear warmup + cosine decay schedule.
Early stopping is driven by validation NDCG@3.
"""
from __future__ import annotations

from time import perf_counter
from typing import TypedDict

import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as opt
import numpy as np

from finish_position_transformer.dataset import (
    MAX_RUNNERS,
    RaceBatchArrays,
    iter_race_batches,
)
from finish_position_transformer.model import ModelOutput, RaceSetTransformer

DEFAULT_TOP1_WEIGHT = 1.0
DEFAULT_TOP3_WEIGHT = 1.0
DEFAULT_PAIRWISE_WEIGHT = 1.0
DEFAULT_LISTNET_WEIGHT = 0.0
TOP3_UPPER_BOUND = 3
LISTNET_RELEVANCE_TIER_BASE = 4
LISTNET_MASK_NEG_INF = -1e9
DEFAULT_LEARNING_RATE = 1e-3
DEFAULT_WEIGHT_DECAY = 1e-4
DEFAULT_WARMUP_STEPS = 500
DEFAULT_MAX_EPOCHS = 30
DEFAULT_EARLY_STOPPING_EPOCHS = 5
DEFAULT_BATCH_SIZE = 64
DEFAULT_SEED = 20260517
NDCG_K = 3
IDEAL_DCG_AT_3 = 3 / np.log(3) + 2 / np.log(4) + 1 / np.log(5)


class LossWeights(TypedDict):
    top1: float
    top3: float
    pairwise: float
    listnet: float


class TrainingConfig(TypedDict):
    batch_size: int
    early_stopping_epochs: int
    learning_rate: float
    loss_weights: LossWeights
    max_epochs: int
    seed: int
    warmup_steps: int
    weight_decay: float


class EpochMetrics(TypedDict):
    epoch: int
    train_loss: float
    valid_ndcg_at_3: float


class TrainingResult(TypedDict):
    best_epoch: int
    best_valid_ndcg_at_3: float
    elapsed_seconds: float
    history: list[EpochMetrics]


def default_training_config() -> TrainingConfig:
    return {
        "batch_size": DEFAULT_BATCH_SIZE,
        "early_stopping_epochs": DEFAULT_EARLY_STOPPING_EPOCHS,
        "learning_rate": DEFAULT_LEARNING_RATE,
        "loss_weights": {
            "top1": DEFAULT_TOP1_WEIGHT,
            "top3": DEFAULT_TOP3_WEIGHT,
            "pairwise": DEFAULT_PAIRWISE_WEIGHT,
            "listnet": DEFAULT_LISTNET_WEIGHT,
        },
        "max_epochs": DEFAULT_MAX_EPOCHS,
        "seed": DEFAULT_SEED,
        "warmup_steps": DEFAULT_WARMUP_STEPS,
        "weight_decay": DEFAULT_WEIGHT_DECAY,
    }


def _to_mx_batch(batch: RaceBatchArrays) -> dict[str, mx.array]:
    return {
        "numeric_features": mx.array(batch["numeric_features"]),
        "categorical_indices": mx.array(batch["categorical_indices"]),
        "race_categorical_indices": mx.array(batch["race_categorical_indices"]),
        "umaban": mx.array(batch["umaban"]),
        "finish_position": mx.array(batch["finish_position"]),
        "mask": mx.array(batch["mask"]),
    }


def _masked_bce(logit: mx.array, label: mx.array, mask: mx.array) -> mx.array:
    log_sigmoid_pos = -nn.losses.binary_cross_entropy(logit, label, reduction="none", with_logits=True)
    weighted = -log_sigmoid_pos * mask
    denom = mx.maximum(mask.sum(), mx.array(1.0))
    return weighted.sum() / denom


def _pairwise_ranking_loss(rank_score: mx.array, finish: mx.array, mask: mx.array) -> mx.array:
    score_diff = rank_score[:, :, None] - rank_score[:, None, :]
    finish_diff = finish[:, :, None] - finish[:, None, :]
    valid_pair = (mask[:, :, None] * mask[:, None, :]).astype(mx.float32)
    pair_label = (finish_diff < 0).astype(mx.float32) * valid_pair * (finish[:, :, None] > 0).astype(
        mx.float32
    ) * (finish[:, None, :] > 0).astype(mx.float32)
    per_pair = nn.losses.binary_cross_entropy(score_diff, pair_label, reduction="none", with_logits=True)
    weighted = per_pair * pair_label
    denom = mx.maximum(pair_label.sum(), mx.array(1.0))
    return weighted.sum() / denom


def _listnet_loss(rank_score: mx.array, finish: mx.array, mask: mx.array) -> mx.array:
    mask_float = mask.astype(mx.float32)
    has_finish = mx.greater(finish, mx.array(0.0)).astype(mx.float32)
    valid = mask_float * has_finish
    relevance = mx.maximum(
        mx.array(0.0),
        mx.array(float(LISTNET_RELEVANCE_TIER_BASE)) - finish,
    ) * valid
    target_logits = mx.where(valid > 0, relevance, mx.array(LISTNET_MASK_NEG_INF))
    target_probs = mx.softmax(target_logits, axis=-1)
    score_logits = mx.where(valid > 0, rank_score, mx.array(LISTNET_MASK_NEG_INF))
    log_score_probs = nn.log_softmax(score_logits, axis=-1)
    per_race = -mx.sum(target_probs * log_score_probs * (valid > 0).astype(mx.float32), axis=-1)
    race_valid = (mx.sum(valid, axis=-1) > 0).astype(mx.float32)
    denom = mx.maximum(race_valid.sum(), mx.array(1.0))
    return (per_race * race_valid).sum() / denom


def multitask_loss(
    output: ModelOutput,
    finish_position: mx.array,
    mask: mx.array,
    weights: LossWeights,
) -> mx.array:
    mask_float = mask.astype(mx.float32)
    top1_label = mx.equal(finish_position, mx.array(1.0)).astype(mx.float32)
    in_top3 = mx.logical_and(
        mx.greater_equal(finish_position, mx.array(1.0)),
        mx.less_equal(finish_position, mx.array(float(TOP3_UPPER_BOUND))),
    )
    top3_label = in_top3.astype(mx.float32)
    top1_term = _masked_bce(output["top1_logit"], top1_label, mask_float)
    top3_term = _masked_bce(output["top3_logit"], top3_label, mask_float)
    pair_term = _pairwise_ranking_loss(output["rank_score"], finish_position, mask)
    listnet_term = _listnet_loss(output["rank_score"], finish_position, mask)
    return (
        weights["top1"] * top1_term
        + weights["top3"] * top3_term
        + weights["pairwise"] * pair_term
        + weights["listnet"] * listnet_term
    )


def _make_learning_rate_schedule(
    base_lr: float, warmup_steps: int, total_steps: int
) -> opt.schedulers.Callable[[mx.array], mx.array]:
    warmup = opt.schedulers.linear_schedule(0.0, base_lr, warmup_steps)
    decay_steps = max(total_steps - warmup_steps, 1)
    cosine = opt.schedulers.cosine_decay(base_lr, decay_steps)
    return opt.schedulers.join_schedules([warmup, cosine], [warmup_steps])


def predict_rank_scores(
    model: RaceSetTransformer,
    arrays: RaceBatchArrays,
    batch_size: int,
) -> np.ndarray:
    scores = np.zeros((len(arrays["race_ids"]), MAX_RUNNERS), dtype=np.float32)
    cursor = 0
    for batch in iter_race_batches(arrays, batch_size=batch_size, shuffle=False):
        mx_batch = _to_mx_batch(batch)
        output = model(
            mx_batch["numeric_features"],
            mx_batch["categorical_indices"],
            mx_batch["race_categorical_indices"],
            mx_batch["umaban"],
            mx_batch["mask"],
        )
        batch_scores = np.asarray(output["rank_score"])
        scores[cursor : cursor + batch_scores.shape[0]] = batch_scores
        cursor += batch_scores.shape[0]
    return scores


def _ndcg_at_3_for_race(predicted_ranks: np.ndarray, finish: np.ndarray, mask: np.ndarray) -> float:
    dcg = 0.0
    real_indices = np.where(mask)[0]
    for slot in real_indices:
        rank = int(predicted_ranks[slot])
        if rank > NDCG_K:
            continue
        finish_pos = int(finish[slot])
        gain = max(0, 4 - finish_pos)
        dcg += gain / np.log(2 + rank)
    return dcg / IDEAL_DCG_AT_3


def evaluate_ndcg(
    model: RaceSetTransformer, arrays: RaceBatchArrays, batch_size: int
) -> float:
    scores = predict_rank_scores(model, arrays, batch_size=batch_size)
    finish = arrays["finish_position"]
    mask = arrays["mask"]
    num_races = scores.shape[0]
    if num_races == 0:
        return 0.0
    ndcgs = np.zeros(num_races, dtype=np.float64)
    for race_idx in range(num_races):
        race_mask = mask[race_idx]
        race_scores = scores[race_idx]
        masked_scores = np.where(race_mask, race_scores, -np.inf)
        order = np.argsort(-masked_scores, kind="stable")
        predicted_ranks = np.zeros(MAX_RUNNERS, dtype=np.int32)
        for slot_rank, position in enumerate(order):
            predicted_ranks[position] = slot_rank + 1
        ndcgs[race_idx] = _ndcg_at_3_for_race(predicted_ranks, finish[race_idx], race_mask)
    return float(ndcgs.mean())


def train_transformer(
    model: RaceSetTransformer,
    train_arrays: RaceBatchArrays,
    valid_arrays: RaceBatchArrays | None,
    config: TrainingConfig,
) -> TrainingResult:
    started = perf_counter()
    rng = np.random.default_rng(config["seed"])
    num_train_races = len(train_arrays["race_ids"])
    if num_train_races == 0:
        return _empty_result(started)
    steps_per_epoch = max(1, (num_train_races + config["batch_size"] - 1) // config["batch_size"])
    total_steps = steps_per_epoch * config["max_epochs"]
    schedule = _make_learning_rate_schedule(
        config["learning_rate"], config["warmup_steps"], total_steps
    )
    optimizer = opt.AdamW(learning_rate=schedule, weight_decay=config["weight_decay"])
    loss_fn = _make_loss_fn(config["loss_weights"])
    loss_and_grad = nn.value_and_grad(model, loss_fn)
    best_score = -float("inf")
    best_epoch = 0
    history: list[EpochMetrics] = []
    no_improve = 0
    for epoch in range(1, config["max_epochs"] + 1):
        train_loss = _run_epoch(model, optimizer, loss_and_grad, train_arrays, config["batch_size"], rng)
        valid_score = (
            evaluate_ndcg(model, valid_arrays, config["batch_size"])
            if valid_arrays is not None and len(valid_arrays["race_ids"]) > 0
            else 0.0
        )
        history.append({"epoch": epoch, "train_loss": train_loss, "valid_ndcg_at_3": valid_score})
        if valid_score > best_score:
            best_score = valid_score
            best_epoch = epoch
            no_improve = 0
        else:
            no_improve += 1
        if no_improve >= config["early_stopping_epochs"]:
            break
    return {
        "best_epoch": best_epoch,
        "best_valid_ndcg_at_3": float(best_score) if best_score > -float("inf") else 0.0,
        "elapsed_seconds": perf_counter() - started,
        "history": history,
    }


def _empty_result(started: float) -> TrainingResult:
    return {
        "best_epoch": 0,
        "best_valid_ndcg_at_3": 0.0,
        "elapsed_seconds": perf_counter() - started,
        "history": [],
    }


def _make_loss_fn(weights: LossWeights):
    def loss_fn(model: RaceSetTransformer, batch: dict[str, mx.array]) -> mx.array:
        output = model(
            batch["numeric_features"],
            batch["categorical_indices"],
            batch["race_categorical_indices"],
            batch["umaban"],
            batch["mask"],
        )
        return multitask_loss(output, batch["finish_position"], batch["mask"], weights)

    return loss_fn


def _run_epoch(
    model: RaceSetTransformer,
    optimizer: opt.AdamW,
    loss_and_grad,
    arrays: RaceBatchArrays,
    batch_size: int,
    rng: np.random.Generator,
) -> float:
    total_loss = 0.0
    num_batches = 0
    for batch in iter_race_batches(arrays, batch_size=batch_size, shuffle=True, rng=rng):
        mx_batch = _to_mx_batch(batch)
        loss, grads = loss_and_grad(model, mx_batch)
        optimizer.update(model, grads)
        mx.eval(model.parameters(), optimizer.state)
        total_loss += float(loss)
        num_batches += 1
    return total_loss / max(num_batches, 1)


__all__ = (
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_EARLY_STOPPING_EPOCHS",
    "DEFAULT_LEARNING_RATE",
    "DEFAULT_MAX_EPOCHS",
    "DEFAULT_SEED",
    "DEFAULT_WARMUP_STEPS",
    "DEFAULT_WEIGHT_DECAY",
    "EpochMetrics",
    "LossWeights",
    "TrainingConfig",
    "TrainingResult",
    "default_training_config",
    "evaluate_ndcg",
    "multitask_loss",
    "predict_rank_scores",
    "train_transformer",
)
