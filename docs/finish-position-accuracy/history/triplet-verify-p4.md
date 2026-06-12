# Triplet Verification P4 — Rank 4 & Rank 7 (2026-06-12)

## Context

Per-class probe on two triplets from `triplet-ideation-ranked.md`. Analysis is READ-ONLY on
the local PG mirror (postgresql://horse_racing:\*\*\*@127.0.0.1:15432/horse_racing). Holdout:
2023-01-01 to 2026-06-12. Gate: partial Spearman ρ ≥ 0.08 (any class, any feature variant).

Probe script: `tmp/probe_triplets_p4.py` (not tracked).

---

## Rank 4: `kohan_3f` × `time_sa` × `corner1_norm`

### Hypothesis

Late-kick efficiency (kohan_3f / margin_to_winner) split by whether the horse ran front vs.
off-pace tracks a maturation signal: a horse whose late-kick improves specifically when NOT
front-running is a genuine improving closer. The trajectory (slope over last 5 races) of this
split-conditional efficiency should predict future finish_norm beyond what unconditional
`kohan3f_avg_5` and `corner_pass_avg_5` capture.

### Column names confirmed

- `jvd_se.kohan_3f` (varchar 3) — last-3f sectional time in tenths of seconds (e.g. "377" = 37.7s). "000" = no data.
- `jvd_se.time_sa` (varchar 4) — margin to winner, stored as `+NNN` in tenths (e.g. "+017" = 1.7s). Winner has "+000".
- `jvd_se.corner_1` (varchar 2) — absolute first-corner position (e.g. "02"). Not pre-normalised; normalised as `corner_1::int / shusso_tosu::int`.
- Same columns exist in `nvd_se` with identical encoding.

`corner1_norm` is computed per-race as `corner_1::int / r.shusso_tosu::int`.
`time_sa_sec` = `CAST(REPLACE(time_sa, '+', '') AS numeric) / 10.0`.
`lke` (late-kick efficiency) = `kohan_3f / (time_sa_sec + 0.1)`.

### Feature construction (leak-free)

For each target race (2023+), look back at the horse's last ≤ 5 completed races with strictly
earlier `race_date_str`:

```sql
-- Late-kick efficiency per past race:
lke = kohan_3f / (time_sa_sec + 0.1)
style = CASE WHEN corner_1 <= 3 THEN 'front' ELSE 'offpace' END

-- Aggregates over last 5:
lke_front_slope    = REGR_SLOPE(lke, -rn) FILTER (WHERE style='front')
lke_offpace_slope  = REGR_SLOPE(lke, -rn) FILTER (WHERE style='offpace')
lke_front_avg      = AVG(lke) FILTER (WHERE style='front')
lke_offpace_avg    = AVG(lke) FILTER (WHERE style='offpace')
lke_split_diff     = lke_front_avg - lke_offpace_avg
```

`rn=1` is most recent past race; `REGR_SLOPE(lke, -rn)` returns positive slope when lke
improves over time (recent races have higher lke than older ones).

### Holdout results (2023–2026, within-race demeaned finish_norm)

Controls: `kohan3f_avg_5`, `corner_pass_avg_5` (JRA); `odds_score` (NAR — only control with
sufficient non-null coverage after R2-parquet migration).

| Class | N target rows | Feature             | partial ρ | Gate ≥ 0.08 |
| ----- | ------------- | ------------------- | --------- | ----------- |
| JRA   | 226 264       | `lke_front_slope`   | -0.0794   | FAIL        |
| JRA   | 226 264       | `lke_offpace_slope` | -0.0690   | FAIL        |
| JRA   | 226 264       | `lke_split_diff`    | +0.0055   | FAIL        |
| NAR   | 470 232       | `lke_front_slope`   | -0.0067   | FAIL        |
| NAR   | 470 232       | `lke_offpace_slope` | -0.0183   | FAIL        |
| NAR   | 470 232       | `lke_split_diff`    | +0.0242   | FAIL        |

Non-null counts: JRA `lke_front_slope` 122 669 / `lke_offpace_slope` 61 769 / `lke_split_diff` 72 311; NAR 237 800 / 264 141 / 217 750. Coverage is substantial so sparsity is not the failure cause.

### Interpretation

The sign on `lke_front_slope` and `lke_offpace_slope` is negative (lower partial ρ = better
rank = smaller `finish_norm`), which is the correct direction — improving late-kick efficiency
predicts better finish. However the magnitude (JRA best: |ρ| = 0.0794) falls just below the
gate threshold and collapses to near zero for NAR.

**Why JRA is near-gate but NAR is not:**
NAR `corner_1` is often 0 ("00") indicating horses that never pass a corner (straight tracks,
short distances). This inflates the `front` bucket with semantically meaningless entries and
dilutes the efficiency slope. The `time_sa` encoding for draw-dead finishes (`+000`) creates
another sparsity issue for the winner's efficiency calculation. Both effects degrade the
signal-to-noise ratio for NAR.

**Why the signal is marginal even for JRA:**
`lke_front_slope` captures the same information as `kohan3f_avg_5` plus positional context.
After partialling out `kohan3f_avg_5` the incremental contribution from the style-split slope
is small. The ideation doc noted this as a redundancy risk: the CONDITIONAL difference is
new but weak in practice.

### Redundancy check

`kohan3f_avg_5` is used as a control and already captures unconditional late-kick mean.
`last_3_avg_finish_norm` captures finish trajectory. `corner_pass_avg_5` captures positional
trajectory. The only genuinely new piece — the conditional split `lke_front` vs `lke_offpace`
— yields max |ρ| ≈ 0.024 (NAR `lke_split_diff`) to 0.079 (JRA `lke_front_slope`). No variant
clears the gate in either class.

### Verdict

**ABORT** — gate ρ ≥ 0.08 not met in any (class, feature) cell. Best observed: JRA
`lke_front_slope` ρ = -0.0794 (just below gate). The signal direction is correct but magnitude
is insufficient. The near-gate JRA result does not justify incremental-verify because NAR is at
noise level (|ρ| < 0.025) and JRA controls (kohan3f + corner_pass) already absorb most of the
variance. Adding three noisy features for a marginal JRA-only partial ρ would not survive model
selection.

---

## Rank 7: `babajotai_code` × `corner1_norm` × `finish_position`

### Hypothesis

A front-runner on firm ground may be a closer on soft going — the horse's style effectiveness
should depend on track condition. The 2×2 conditional average (front/off × firm/soft) over the
last 10 past races should reveal horses whose style preference systematically switches by going,
yielding a predictive feature beyond `past_nige_rate_self` × static `babajotai_code`.

### Column names confirmed

- `jvd_ra.babajotai_code_shiba` / `babajotai_code_dirt` — turf / dirt going ordinal (varchar 1: "1"=良, "2"=稍重, "3"=重, "4"=不良, "0"=not applicable).
- `nvd_ra.babajotai_code_dirt` / `babajotai_code_shiba` — same encoding for NAR.
- `jvd_se.corner_1` — same as rank 4.
- `jvd_se.kakutei_chakujun` — finish position (varchar 2).

Going bucket: firm = babajotai ∈ {1,2}, soft = babajotai ∈ {3,4}. Style bucket: front = corner_1 ≤ 3, off = corner_1 > 3.

### Feature construction (leak-free)

For each target race (2023+), look back at the horse's last ≤ 10 completed races with strictly
earlier `race_date_str`:

```sql
finish_norm_hist = (finish_pos - 1.0) / (shusso_tosu - 1.0)

-- 2×2 conditional averages:
fp_front_firm  = AVG(finish_norm_hist) FILTER (WHERE style='front' AND going='firm')
fp_front_soft  = AVG(finish_norm_hist) FILTER (WHERE style='front' AND going='soft')
fp_off_firm    = AVG(finish_norm_hist) FILTER (WHERE style='off'   AND going='firm')
fp_off_soft    = AVG(finish_norm_hist) FILTER (WHERE style='off'   AND going='soft')

-- Interaction features:
front_going_diff        = fp_front_soft - fp_front_firm
style_going_interaction = (fp_front_soft - fp_front_firm) - (fp_off_soft - fp_off_firm)
front_finish_slope_on_going = REGR_SLOPE(finish_norm_hist, babajotai) FILTER (WHERE style='front')
```

### Holdout results (2023–2026, within-race demeaned finish_norm)

Controls: `corner_pass_avg_5` (JRA); `odds_score` (NAR).

| Class | N target rows | Feature                       | partial ρ | Gate ≥ 0.08 |
| ----- | ------------- | ----------------------------- | --------- | ----------- |
| JRA   | 226 264       | `front_going_diff`            | -0.0098   | FAIL        |
| JRA   | 226 264       | `style_going_interaction`     | -0.0122   | FAIL        |
| JRA   | 226 264       | `front_finish_slope_on_going` | -0.0058   | FAIL        |
| NAR   | 470 232       | `front_going_diff`            | -0.0077   | FAIL        |
| NAR   | 470 232       | `style_going_interaction`     | +0.0018   | FAIL        |
| NAR   | 470 232       | `front_finish_slope_on_going` | -0.0048   | FAIL        |

Non-null coverage: JRA `front_going_diff` 72 471 / `style_going_interaction` 14 938; NAR 222 579 / 88 583. `style_going_interaction` is notably sparse (requires a horse to have runs in all four cells within 10 races) — this is the primary limiting factor.

### Interpretation

All |ρ| < 0.013. The sparsity issue is severe: `style_going_interaction` requires a horse to
have raced as front-runner on firm AND front-runner on soft AND off-pace on firm AND off-pace on
soft within 10 starts. Only ~7% of JRA target rows have this (14 938 / 226 264) and ~19% for
NAR (88 583 / 470 232). Even for `front_going_diff` (less constrained, ~32% JRA coverage), the
signal is at noise level.

**Root cause:** The going-conditioned style interaction is sparse by construction. The number of
runs needed to estimate a 2×2 conditional average with meaningful coverage far exceeds the
10-race window. In practice, most horses run primarily on one going type, making the
cross-going comparison ill-estimated. The model almost certainly already learns the static
`babajotai_code × corner1_norm` interaction implicitly.

### Redundancy check

`past_nige_rate_self` (if it exists in the feature set) captures style frequency history. The
current features include `corner_pass_avg_5` and `last_race_corner_pass_norm`. `track_condition_normalized` is a current-race static. The going × style cross-temporal signal is new but
unestimable at this data density — not a measurement failure, a structural one.

### Verdict

**ABORT** — gate ρ ≥ 0.08 not met in any (class, feature) cell. Best observed: JRA
`style_going_interaction` ρ = -0.0122. The signal is at noise level across all variants and
both classes. The primary barrier is structural: a 2×2 conditional split over 10 races yields
too-sparse cells for the majority of horses. This is not recoverable by extending the window
(stale going regimes) or relaxing the split (loses the interaction signal).

---

## Summary

| Rank | Triplet                                               | Best                     | ρ    | (class) | Gate ≥ 0.08 | Verdict |
| ---- | ----------------------------------------------------- | ------------------------ | ---- | ------- | ----------- | ------- |
| 4    | `kohan_3f` × `time_sa` × `corner1_norm`               | 0.0794 (JRA front slope) | FAIL | ABORT   |
| 7    | `babajotai_code` × `corner1_norm` × `finish_position` | 0.0122 (JRA interaction) | FAIL | ABORT   |

Both triplets are ABORT. Neither produces a feature variant that clears ρ ≥ 0.08 after
controlling for existing features and odds in any class.

**Cross-pair interpretation:** Rank 4 is structurally more promising than Rank 7 — JRA
`lke_front_slope` at 0.0794 shows the direction is correct and the information exists. The
signal collapses in NAR due to data encoding issues (straight tracks, zero-margin winners).
Rank 7 is fully noise-level in both classes due to 2×2 cell sparsity. The alternating-pair
ordering (HIGH 4 + LOW 7) was correctly calibrated: the HIGH triplet showed marginal activity,
the LOW was silent.

**Saturation note:** Both ABORTs are consistent with the documented frontier (`finish_position_frontier_2026_06_11.md`): horse-level conditional signals built from existing PG
columns continue to fall below the gate. The only unsaturated path remains v3 running-style
model extension (separate project).
