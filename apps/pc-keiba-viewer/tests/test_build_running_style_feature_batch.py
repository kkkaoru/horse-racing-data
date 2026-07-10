from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "scripts"))

import build_running_style_feature_batch as subject


def test_select_per_horse_columns_excludes_current_targets_but_keeps_prior_corner_history() -> None:
    columns = [
        "source",
        "race_id",
        "target_corner_1_norm",
        "target_corner_2_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
        "past_corner_2_norm_avg_5",
        "last_race_corner_4_norm",
        "speed_index_avg_5",
        "self_nige_rate_minus_field_avg",
    ]
    assert subject.select_per_horse_columns(columns) == [
        "past_corner_2_norm_avg_5",
        "last_race_corner_4_norm",
        "speed_index_avg_5",
    ]
