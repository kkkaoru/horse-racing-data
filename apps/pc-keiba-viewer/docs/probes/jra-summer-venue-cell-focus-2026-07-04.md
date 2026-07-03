# JRA Summer-Venue (札幌/函館/福島/小倉) Cell Accuracy Focus (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position, subgroup/cell diagnostics + feature hypothesis
- **Trigger**: user focus request — understand serve-realistic cell accuracy at the
  4 JRA summer/regional venues (札幌 Sapporo=`01`, 函館 Hakodate=`02`, 福島
  Fukushima=`03`, 小倉 Kokura=`10`), compare current predictions vs 2025 and 2026
  actuals, and hypothesis→verify→evaluate for summer-specific improvement.
- **Context**: this work runs on the just-verified CLEAN 250-feat baseline
  (`jra-cb-v9-sim-2013`, armB from `tmp/candidate-leak-clean-retrain/`) —
  the deployed model previously carried a within-race leak
  (`target_corner_1/3/4_norm` + `target_running_style_class`, NULL at serve).
  All numbers below are serve-realistic (leak-free) unless explicitly marked
  "raw actuals only". Coordinated with the parallel same-day track-bias /
  draw-affinity retest (`jra-masked-lever-clean-retest-2026-07-04.md`) to avoid
  duplicate work — that doc already re-verified same-day track bias and draw
  affinity on the clean baseline (both REJECT, DO-NOT-RETEST) and found only a
  single non-summer cell (Tokyo) with any positive-LB95 primary.

## Part 1 — serve-realistic cell accuracy, 2023-2025

**Method**: predict-only reload of the saved clean armB models
(`tmp/candidate-leak-clean-retrain/models_jra_v9sim/armB/fold-{2023,2024,2025}/model.json`,
250 feat, no leak cols) on the held-out validation slice per fold year
(`split_train_valid`, `TRAIN_START=20130101`), restricted to
`keibajo_code in ('01','02','03','10')`. Pooled JRA-wide baseline for
comparison (from `jra_v9sim_wf_report.json`): **top1 33.63% / place2 18.02% /
place3 14.13% / top3_box 9.45%** (n=10,365 races, 2023-2025 pooled). Cells
require ≥50 races; `meetingday` (開催日数, `kaisai_nichime`) and `waku`
(枠, `wakuban`) were joined in from Postgres (not present in the offline
store) following the same leak-free join pattern as
`tmp/candidate-leak-clean-retrain/meetingday-bias-handoff/build_meetingday_features.py`.
Script: `tmp/candidate-summer-venue-focus/summer_venue_focus.py`. Full cell
table: `tmp/candidate-summer-venue-focus/summer_cells_report.json`.

### Weakest cells (top1 delta vs 33.63% baseline; n≥50)

| Dim              | Group                  | n   | top1  | place2 | place3 | Δtop1 (pp)                                 |
| ---------------- | ---------------------- | --- | ----- | ------ | ------ | ------------------------------------------ |
| distance×venue   | long × fukushima       | 80  | 30.0% | 7.5%   | 7.5%   | **-3.6** (place2/3 collapse: -10.5/-6.6pp) |
| meetingday×venue | late(6+) × sapporo     | 144 | 24.3% | 17.4%  | 6.9%   | **-9.3**                                   |
| waku×venue       | inner(1-2) × sapporo   | 83  | 21.7% | 20.5%  | 8.4%   | **-11.9**                                  |
| waku×venue       | inner(1-2) × kokura    | 171 | 24.6% | 11.1%  | 14.6%  | **-9.1**                                   |
| class×venue      | E × sapporo            | 121 | 26.5% | 12.4%  | 9.9%   | **-7.2**                                   |
| distance×venue   | mile × kokura          | 198 | 25.8% | 12.6%  | 10.6%  | **-7.9**                                   |
| waku×venue       | inner(1-2) × fukushima | 170 | 27.1% | 14.7%  | 11.2%  | **-6.6**                                   |
| venue×year       | kokura × 2025          | 240 | 26.3% | 18.3%  | 12.1%  | **-7.4**                                   |
| venue×year       | kokura × 2023          | 264 | 28.0% | 11.7%  | 12.9%  | **-5.6**                                   |
| class×venue      | E × kokura             | 193 | 28.0% | 15.0%  | 12.4%  | **-5.7**                                   |

**Strongest** (for contrast): sprint×hakodate (+9.2pp top1, n=63),
mid-meeting-day(3-5)×hakodate waku-mid (+6.2pp, n=211), other-surface×kokura
(+2.0pp, n=78).

**Two structural patterns stand out** (not single-cell noise — each recurs
across ≥3 of the 4 venues):

1. **Inside-waku (1-2) overconfidence at Kokura/Sapporo/Fukushima.** When the
   model's #1 pick is drawn inside, that pick's actual win rate is 6.6-11.9pp
   _below_ the venue-pooled average at 3 of 4 summer venues (Hakodate is the
   exception, roughly flat). This is a genuine, recurring miscalibration, not
   a raw draw-bias absence — draw signal (wakuban, track_bias_inside,
   same-track win rates) is already in the 250-feat set, and the parallel
   same-day-track-bias / draw-affinity retest (masked-lever doc, same date)
   found no positive-LB95 lever at any cell except Tokyo. This looks more
   like **rank-1-pick overconfidence conditional on venue** than a missing
   feature — worth flagging to whoever owns draw-affinity/ablation
   (masked-lever tasks #7/#8) as a candidate cell to check once their WF
   lands, but out of scope to fix here (see Part 2 hypothesis instead, a
   different mechanism).
2. **Kokura and Sapporo are the fragile venues; Hakodate is consistently
   strong** (sprint +9.2, mid-meetingday +6.2, most distance bands positive).
   Fukushima is mixed (long-distance and inner-waku weak, everything else
   near baseline).

### Odds-divergence (upset) analysis

Upset = market favorite (`tansho_ninkijun==1`) did not finish 1st.
JRA-wide upset rate 66.4% (turf 67.2 / dirt 65.8). **Summer venues run
hotter**: Fukushima 69.7%, Kokura 69.0%, Hakodate 66.2%, Sapporo 64.2%
(sub-cell extremes: Kokura×mile 74.75% n=198, Fukushima×long 72.5% n=80,
Hakodate×sprint only 57.1% n=63 — noisy at this n but directionally
consistent with Hakodate being the "easiest" summer venue).

Model behavior on upset vs non-upset races, pooled across all 4 summer
venues: **non-upset top1 91.7%** (the model essentially just rides the
favorite when the favorite wins) vs **upset top1 3.5%, place2 13.3%, place3
12.1%**. This is largely a definitional consequence (model's #1 pick tracks
the market favorite closely; if the favorite loses, the model's #1 pick is
usually also wrong) rather than a summer-specific defect — but it confirms
the model carries **no differentiated signal that would catch a summer-venue
upset specifically**, and Fukushima/Kokura's structurally higher upset rate
means this "blind spot" bites more often there than at Tokyo/Nakayama-type
venues. Place2/place3 hold up much better than top1 in upset races (13/12%
vs 92% non-upset) — the model's _ranking_ still has some signal even when
its #1 pick is wrong, it's specifically top1 exactness that fails on upsets.

## Part 2 — 2026 actuals

**Store limitation**: the offline feature store (`tmp/candidate-eval-jra/augmented`)
has no 2026 rows at all (max race_year=2025), and the production served-prediction
log (`race_finish_position_model_predictions` in Neon) has only 5 races /
68 rows of genuine day-by-day serving for the current clean model version, all
from 2026-07-03 — not enough for a real serve-vs-actual comparison yet.
Reconstructing the full 250-feature store for 2026 summer races was out of
scope for this pass. Given this, Part 2 is **raw 2026 actuals only (Neon
`jvd_se`/`jvd_ra`), NOT model-scored** — descriptive context, not an accuracy
number:

| Venue     | Races (finished)                 | Avg field | Favorite win rate |
| --------- | -------------------------------- | --------- | ----------------- |
| Sapporo   | 0 (meet not started as of 07-04) | —         | —                 |
| Hakodate  | 84                               | 10.6      | 27.8% (n=72)      |
| Fukushima | 108                              | 12.9      | 24.0% (n=96)      |
| Kokura    | 181                              | 13.5      | 31.1% (n=167)     |

All three running venues' 2026 favorite-win-rates and field sizes are within
normal historical range (JRA-wide favorite win rate is typically ~33-34%
given the ~66% upset rate above) — no red flags, but also not something we
can yet attribute to the model since these are unscored actuals. **Follow-up
recommended**: once more days of genuine armB-clean serving accumulate in
`race_finish_position_model_predictions`, re-run this cell breakdown against
real 2026 served predictions instead of raw actuals.

## Part 3 — hypothesis: short-straight × closing-kick interaction

**Motivation**: the 4 summer venues have notably shorter home straights than
the main tracks (store's own `course_final_straight_m`: ~260-296m at
Sapporo/Hakodate/Fukushima/Kokura vs ~310-525m at Tokyo/Nakayama/Chukyo/
Hanshin/Kyoto/Niigata). A short straight structurally favors front-runners
(逃げ/先行) and penalizes closers (差し/追込), who have less room to make up
ground. The store already has the raw ingredients (`course_final_straight_m`,
`past_sashi_rate_self`, `past_oikomi_rate_self`, `past_nige_rate_self`,
`past_senkou_rate_self`, plus race-level `field_nige_pressure` /
`field_sashi_pressure` etc. — confirmed via schema grep that no existing
column already multiplies straight-length by running-style tendency) but
because summer venues are a **minority of the overall JRA population**
(~2,000/year vs ~48,000/year JRA-wide), a depth-8 CatBoost fit on the full
population may under-allocate splits to this niche interaction even though
the ingredients exist — the same argument that motivated other per-cell
lever retests in this campaign, but for a genuinely different physical
mechanism (closing room, not draw/rail position) than the same-day
track-bias or draw-affinity levers already retested and REJECTed.

**Candidate columns** (schema extension only, armB's 250 feats untouched):

```
closer_x_straight = (past_sashi_rate_self + past_oikomi_rate_self) * course_final_straight_m
front_x_straight  = (past_nige_rate_self  + past_senkou_rate_self) * course_final_straight_m
```

**Method**: armC = armB (250) + 2 new cols (252), CatBoost YetiRank
(iterations=300, depth=8, lr=0.05, l2=3.0), 3-fold WF (2023/2024/2025) ×
3 seeds per fold (armC retrained per seed; armB baseline models reused
unmodified from `tmp/candidate-leak-clean-retrain`), paired race-level
bootstrap (2000 iter) vs the reused armB baseline predictions on the same
held-out rows. Evaluated GLOBAL (all JRA, sanity/no-regression check) and
SUMMER-RESTRICTED (primary target, + distance-band breakdown within summer).
Script: `tmp/candidate-summer-pace-hypo/straight_closer_wf.py`. Report:
`tmp/candidate-summer-pace-hypo/straight_closer_report.json`.

### Result: REJECT (both GLOBAL and SUMMER-RESTRICTED)

<!-- RESULT_BLOCK -->

WF completed: armC (252 feat) retrained 3 folds (2023/2024/2025) × 3 seeds
(offsets 42/142/242), armB baseline predictions reused unmodified. Paired
race-level bootstrap (2000 iter, seed 42) vs armB on the same held-out rows.
Full data: `tmp/candidate-summer-pace-hypo/straight_closer_report.json`,
`tmp/candidate-summer-pace-hypo/venue_breakdown.json` (per-venue follow-up,
predict-only reuse of the already-trained armC models, no retraining).

**GLOBAL** (all JRA venues pooled, n=10,365 races, 3-seed avg vs armB):

| Metric   | Δ (pp) | LB95 avg | LB95 min |
| -------- | ------ | -------- | -------- |
| top1     | +0.193 | -0.119   | -0.251   |
| place2   | -0.010 | -0.380   | -0.502   |
| place3   | +0.016 | -0.379   | -0.540   |
| place4   | -0.093 | -0.450   | -0.531   |
| place5   | -0.077 | -0.405   | -0.521   |
| place6   | +0.351 | +0.010   | -0.029   |
| top3_box | +0.032 | -0.135   | -0.270   |

No primary metric (top1/place2/place3/top3_box) clears LB95>0 globally
(expected — this is a summer-restricted hypothesis). Two non-primary metrics
(place4 -0.093pp, place5 -0.077pp) fall below the -0.05pp no-regression sanity
threshold — a mild negative drift, though confined to secondary placings, not
the primary accept-gate metrics.

**SUMMER-RESTRICTED** (venues 01/02/03/10 pooled, n=2,448 races, 3-seed avg vs
armB — the target population for this hypothesis):

| Metric   | Δ (pp) | LB95 avg | LB95 min | Per-seed sign (42/142/242)   |
| -------- | ------ | -------- | -------- | ---------------------------- |
| top1     | +0.368 | -0.272   | -0.531   | +/+/+ (stable)               |
| place2   | -0.191 | -0.994   | -1.266   | -/+/- (unstable, net neg)    |
| place3   | +0.231 | -0.545   | -0.899   | +/+/- (unstable at seed 242) |
| place4   | -0.150 | -0.885   | -1.144   | +/-/-                        |
| place5   | -0.354 | -1.103   | -1.225   | -/-/- (stable negative)      |
| place6   | +0.654 | -0.054   | -0.327   | +/+/+ (stable, LB95 near 0)  |
| top3_box | +0.123 | -0.245   | -0.368   | +/+/+ (stable)               |

Gate for summer-conditional adoption (primary delta ≥ +0.08pp AND LB95>0,
multi-seed stable sign, plus global no-regression): **not met by any primary
metric.** top1 (+0.368pp) and place3 (+0.231pp) are directionally positive and
clear the +0.08pp delta bar, but both have LB95 firmly negative (-0.27 to
-0.90pp), and place3's per-seed sign flips negative at seed 242. place2 is net
negative. The point estimates are consistent with the physical hypothesis
(front-runners favored on short straights) but n=2,448 races is too small for
the paired bootstrap to distinguish this from noise.

**SUMMER-RESTRICTED by distance band** (top1/place3/top3_box Δpp, 3-seed avg):

| Band         | n   | top1 (LB95 avg) | place3 (LB95 avg) | top3_box (LB95 avg) |
| ------------ | --- | --------------- | ----------------- | ------------------- |
| sprint       | 305 | -0.437 (-2.514) | -0.437 (-2.514)   | 0.000 (0.000)       |
| mile         | 631 | +0.740 (-0.423) | -0.158 (-1.532)   | 0.000 (-0.475)      |
| intermediate | 950 | +0.526 (-0.632) | +0.632 (-0.668)   | +0.351 (-0.351)     |
| long         | 295 | +0.113 (-1.469) | +1.243 (-0.904)   | +0.791 (-0.226)     |
| extended     | 267 | +0.125 (-1.373) | -0.624 (-3.371)   | -0.999 (-2.372)     |

`intermediate` (950 races) is the only band with all-positive direction across
top1/place2/place3/top3_box simultaneously, but still no LB95>0. `sprint` and
`extended` are net negative — the opposite of what the short-straight/closer
hypothesis would predict if it held uniformly, suggesting no clean
distance-conditional sub-signal either.

**SUMMER-RESTRICTED by individual venue** (top1/place2/place3/top3_box Δpp,
3-seed avg; follow-up predict-only run, no retraining):

| Venue     | n   | top1   | place2 | place3 | top3_box |
| --------- | --- | ------ | ------ | ------ | -------- |
| Sapporo   | 504 | +0.794 | -0.728 | +1.190 | +0.728   |
| Hakodate  | 432 | +0.231 | +0.617 | +1.389 | +0.540   |
| Fukushima | 720 | +0.463 | -0.787 | -1.019 | -0.463   |
| Kokura    | 792 | +0.084 | +0.253 | +0.126 | +0.042   |

Hakodate is the only venue with all 4 primary metrics simultaneously positive
(consistent with it already being flagged in Part 1 as the "consistently
strong" summer venue), Sapporo is mixed (place2 negative), Kokura is
essentially flat/no-effect, and Fukushima is net negative across 3 of 4
primaries — the opposite direction from the hypothesis. None of the 4 venues
individually reach LB95>0 on any primary metric (per-venue n=432-792 is even
smaller than the pooled summer slice).

## Overall conclusion

<!-- CONCLUSION_BLOCK -->

**Verdict: REJECT. `closer_x_straight` / `front_x_straight` are DO-NOT-DEPLOY
— do not fold into a clean retrain of the production JRA model.**

Neither the GLOBAL sanity check nor the SUMMER-RESTRICTED target population
clears the accept gate. Point estimates lean the direction the physical
hypothesis predicts (short straights favor front-runners: summer-pooled top1
+0.368pp, place3 +0.231pp, all 3 seeds positive-signed for top1), but every
primary metric's 95% lower bound stays negative (top1 LB95 avg -0.272pp,
place3 -0.545pp), and place2 is net negative (-0.191pp) with an unstable
per-seed sign. GLOBAL also shows a mild negative drift on place4/place5
(-0.09/-0.08pp, below the -0.05pp no-regression sanity threshold), so this
isn't even a clean "no-op elsewhere" candidate.

The distance-band and per-venue cuts don't rescue it either: `intermediate`
distance (950 races) and Hakodate (432 races) are the only sub-slices with
all-positive primary-metric direction, but neither reaches LB95>0, and
Fukushima is net negative across 3 of 4 primaries — directly contradicting
the hypothesis at one of the four target venues. This is the same failure
mode as the other venue/cell-specific candidate levers already REJECTed in
this campaign ([[project_venue_cell_round2_2026_06_20]],
[[project_cell_campaign_2026_07_02]]): the underlying mechanism is physically
plausible, but the summer-venue population (~2,000-2,450 races/2023-2025,
~500-800 per individual venue) is too small for a depth-8 CatBoost fit +
paired bootstrap to resolve a niche interaction from noise, and per-venue
model routing/specialization has independently been shown structurally
infeasible for JRA in the same campaign.

**Recommendation**: do not deploy. Mark
`closer_x_straight`/`front_x_straight` DO-NOT-RETEST at the current sample
size and model architecture. If revisited, the only path with a chance of
clearing the gate would be pooling many more years of summer-venue history
(analogous to the NAR full-window finding vs JRA's 2013 cutoff) to shrink the
bootstrap CI — not a different feature engineering variant on the same
2023-2025 window.

## Artifacts

- Cell diagnostic script + report: `tmp/candidate-summer-venue-focus/summer_venue_focus.py`,
  `tmp/candidate-summer-venue-focus/summer_cells_report.json`
- Hypothesis WF script + report: `tmp/candidate-summer-pace-hypo/straight_closer_wf.py`,
  `tmp/candidate-summer-pace-hypo/straight_closer_report.json`
- Related: `jra-masked-lever-clean-retest-2026-07-04.md` (same-day track-bias /
  draw-affinity clean retest, both REJECT — coordinated to avoid duplication)
- Not re-verified in this pass: full 2026 serve-log comparison (blocked on
  more days of genuine armB-clean production serving accumulating), Ban-ei/NAR
  summer-equivalent venues (out of scope per user's explicit JRA-only focus)
