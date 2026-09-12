"""PIT-safe scalar horse-history windows and deterministic static microbatches."""

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt


@dataclass(frozen=True)
class HorseWindows:
    contexts: npt.NDArray[np.float32]
    targets: npt.NDArray[np.float32]
    source_indices: npt.NDArray[np.int64]
    history_counts: npt.NDArray[np.int64]


@dataclass(frozen=True)
class WindowConfig:
    start_date: str
    end_date: str
    context_length: int = 128
    minimum_history: int = 2
    require_finite_targets: bool = True

    def __post_init__(self) -> None:
        if self.start_date > self.end_date or self.context_length < 1 or self.minimum_history < 1:
            raise ValueError("Invalid window dates or history lengths")


def build_windows(
    *,
    horse_ids: npt.NDArray[np.str_],
    dates: npt.NDArray[np.str_],
    values: npt.NDArray[np.float32],
    config: WindowConfig,
) -> HorseWindows:
    """Preserve source-row identity; exclude every same-day/future outcome.

    All venues contribute history. The finite context is an explicit model-input
    cap, not a restriction on training eligibility or entrant scope. Sparse rows
    are absent from windows and must retain incumbent scores in race evaluation.
    """
    if horse_ids.ndim != 1 or dates.shape != horse_ids.shape or values.shape != horse_ids.shape:
        raise ValueError("Horse, date and scalar value columns must align")
    if np.any(dates[1:] < dates[:-1]):
        raise ValueError("Source rows must be chronological")
    candidate_mask = (dates >= config.start_date) & (dates <= config.end_date)
    if config.require_finite_targets:
        candidate_mask &= np.isfinite(values)
    candidates = np.flatnonzero(candidate_mask)
    contexts = np.full((len(candidates), config.context_length), np.nan, dtype=np.float32)
    targets = np.empty((len(candidates), 1), dtype=np.float32)
    indices = np.empty(len(candidates), dtype=np.int64)
    counts = np.empty(len(candidates), dtype=np.int64)
    by_horse: dict[str, list[int]] = {}
    for index, horse in enumerate(horse_ids):
        horse_rows = by_horse.setdefault(str(horse), [])
        if np.isfinite(values[index]):
            horse_rows.append(index)
    history = {horse: np.asarray(rows, dtype=np.int64) for horse, rows in by_horse.items()}
    written = 0
    for target in candidates:
        rows = history[str(horse_ids[target])]
        cutoff = int(np.searchsorted(dates[rows], dates[target], side="left"))
        if cutoff < config.minimum_history:
            continue
        prior = rows[max(0, cutoff - config.context_length) : cutoff]
        contexts[written, -len(prior) :] = values[prior]
        targets[written, 0] = values[target]
        indices[written] = target
        counts[written] = cutoff
        written += 1
    return HorseWindows(contexts[:written], targets[:written], indices[:written], counts[:written])


def filter_window_targets(
    windows: HorseWindows, target_rows: npt.NDArray[np.bool_]
) -> HorseWindows:
    """Filter labels without removing other venues from their input histories."""
    keep = target_rows[windows.source_indices]
    return HorseWindows(
        windows.contexts[keep],
        windows.targets[keep],
        windows.source_indices[keep],
        windows.history_counts[keep],
    )


class StaticWindowBatches:
    """Address batches by optimizer step for deterministic resumption.

    Drop the final incomplete effective batch each epoch. Validation should use
    the original windows, never this sampler, so no evaluation runner disappears.
    """

    def __init__(
        self, windows: HorseWindows, *, batch_size: int, accumulation: int = 1, seed: int = 42
    ) -> None:
        if batch_size < 1 or accumulation < 1:
            raise ValueError("Batch size and accumulation must be positive")
        self.windows = windows
        self.batch_size = batch_size
        self.accumulation = accumulation
        self.seed = seed
        self.effective_batch = batch_size * accumulation
        self.steps_per_epoch = len(windows.targets) // self.effective_batch
        if self.steps_per_epoch == 0:
            raise ValueError("Not enough windows for one static effective batch")
        self.epoch = -1
        self.order = np.empty(0, dtype=np.int64)

    def at_step(self, step: int) -> tuple[npt.NDArray[np.float32], npt.NDArray[np.float32]]:
        if step < 0:
            raise ValueError("Step must be nonnegative")
        epoch, within_epoch = divmod(step, self.steps_per_epoch)
        if self.epoch != epoch:
            self.order = np.random.default_rng(self.seed + epoch).permutation(
                len(self.windows.targets)
            )
            self.epoch = epoch
        start = within_epoch * self.effective_batch
        indices = self.order[start : start + self.effective_batch]
        return (
            self.windows.contexts[indices].reshape(self.accumulation, self.batch_size, -1),
            self.windows.targets[indices].reshape(self.accumulation, self.batch_size, -1),
        )
