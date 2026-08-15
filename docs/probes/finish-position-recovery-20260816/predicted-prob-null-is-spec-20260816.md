# predicted_top1/top3_prob NULL is spec (2026-08-16)

Read-only Neon + code. Not today's outage.

## Neon

| kaisai_tsukihi | rows | score non-null | top1 non-null | top3 non-null |
| -------------- | ---: | -------------: | ------------: | ------------: |
| 0816           |  940 |            940 |             0 |             0 |
| 0815           |  822 |            822 |             0 |             0 |
| 0814           |  337 |            337 |             0 |             0 |
| 0810           |  466 |            466 |             0 |             0 |

All 2026 domestic rows with a non-null `predicted_top1_prob`: **9 rows**,
`source=overseas`, `model_version=overseas-lgbm-fp-v1`, date `0725`.
JRA/NAR/Ban-ei production models never populate these columns.

## Code

`predict_lib/upcoming.py` `build_prediction_rows`:

> `predicted_top1_prob` / `predicted_top3_prob` /
> `predicted_finish_position` are left `None` — the v7-lineage rankers
> emit a relevance score + rank, not calibrated probabilities.

Walk-forward scorer writes the same nulls
(`score_finish_position_walk_forward.py`). Calibration
(`calibrate_finish_position.py`) is an offline job that _derives_ probs
from rank when the column is all-null.

## Viewer

Race-detail table sorts and displays `predictedRank` / `predictedScore`
spread. `predictedTop1Prob` exists only on the local-loader type
(`finish-prediction-dimensions.ts`), not on the production detail
table. Display does not depend on these Neon columns.

Not a 0816 generation drop.
