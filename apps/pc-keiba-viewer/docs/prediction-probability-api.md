# Model probabilities in the prediction API

## Root cause

The `2026/09/05/09/01` daily prediction response contains ranks, but its
`winProbability` and `showProbability` values are null. There are two separate
causes:

1. The domestic production rankers intentionally leave `predicted_top1_prob` and
   `predicted_top3_prob` null. They output relevance scores and ranks, not
   calibrated probabilities. See `predict_lib/upcoming.py` and
   `docs/probes/finish-position-recovery-20260816/predicted-prob-null-is-spec-20260816.md`.
2. The viewer's LambdaRank query and the cron KV publisher also discarded these
   columns when they **were** populated, replacing every probability with null.

The transport fix selects and preserves valid stored probabilities in both the
database and KV paths. It does not manufacture probabilities from ranks. The
race-detail UI's heuristic estimates are not calibrated model outputs and are
not substituted into the API's model probability fields.

## Response contract

Both `/api/finish-predictions/daily` (including a single-race request) and the MCP
`get_finish_prediction_summary` response preserve these fields per horse:

| Field                   | Meaning                                                     |
| ----------------------- | ----------------------------------------------------------- |
| `winProbability`        | Stored `predicted_top1_prob`, a number from 0 to 1, or null |
| `showProbability`       | Stored `predicted_top3_prob`, a number from 0 to 1, or null |
| `winProbabilityStatus`  | `available` or `not_provided`                               |
| `showProbabilityStatus` | `available` or `not_provided`                               |

Each daily race object and MCP summary also includes a single
`probabilityAvailability` object with `win` and `show` fields. Its values are
`available` (all returned horses), `partial` (some returned horses), or
`not_provided` (none). This is relative to the returned prediction rows, not a
claim that all declared runners have predictions. Race-level metadata avoids
repeating status strings per horse in size-limited MCP responses.

`available` includes a genuine zero. `not_provided` means that no usable model
probability was supplied; missing, blank, nonnumeric, nonfinite, and out-of-range
values are returned as null. Rank predictions remain available when probabilities
are absent. This is not an authentication error or a failed race lookup.

The status does not certify calibration quality. `showProbability` retains the
existing **top-three** meaning; it does not represent the top-two place-bet rule
for small fields. Consumers should not treat rank scores, confidence tiers, or
model-level historical accuracy as a horse's win probability.

## Deployment and remaining work

Deploy the viewer and `finish-position-cron` together. Existing KV and race-detail
cache entries can still contain discarded nulls until refreshed. Republish the
affected prediction caches through the existing publisher with `bustCacheApi`
enabled after deploying; an existing cache hit alone cannot recover a discarded
database value.

The reported domestic race will continue to return `not_provided` while its model
does not write probabilities. Producing numeric probabilities for that race
requires a separately trained and validated calibration model, writing its
outputs for the same prediction generation, and republishing the caches. Changing
transport code alone cannot establish calibrated probabilities.
