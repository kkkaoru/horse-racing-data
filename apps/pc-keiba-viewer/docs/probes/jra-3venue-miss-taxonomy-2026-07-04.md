# JRA 函館/福島/小倉 予測ミス・タクソノミー (2024-2026)

Task #24. Where and how did the finish-position model's rank-1 pick lose at
Hakodate (02) / Fukushima (03) / Kokura (10), and does the model's disagreement
with the market add value at these three venues? All race-day filters below use
the _venue's actual meeting calendar_ (Kokura races Jan-Mar + Jul-Aug; Fukushima
races Apr + Jul; Hakodate races Jun-Jul only) — no month restriction was applied
beyond `keibajo_code in (02,03,10)`.

Scripts: `tmp/candidate-3venue-miss-taxonomy/venue_miss_taxonomy.py` (2024/2025
reconstruction, armB clean models) and `tmp/candidate-3venue-miss-taxonomy/era_2026_reflection.py`
(2026, Neon predictions joined to local-PG actuals). Raw output:
`tmp/candidate-3venue-miss-taxonomy/venue_report.json`,
`tmp/candidate-3venue-miss-taxonomy/era_2026_report.json`.

## 0. Data provenance (read this before the numbers below)

Three fundamentally different kinds of "prediction" get mixed together if you
query `race_finish_position_model_predictions` naively, and keeping them apart
changed the shape of this report:

| Label used below          | What it actually is                                                                                                                                                                                                                                                                                          | Years      |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| **Reconstruction (armB)** | Predict-only reload of the leak-clean armB fold models (`tmp/candidate-leak-clean-retrain/models_jra_v9sim/armB/fold-2024`, `fold-2025`), scored on the offline feature store. No live serving ever existed for these dates — this is the best available serve-realistic proxy.                              | 2024, 2025 |
| **Backfill (iter14)**     | `model_version='iter14-jra-cb-pacestyle-course-v8'` rows, ALL generated in a **single batch on 2026-06-04**, covering Kokura's already-completed winter meet (0124-0301) and Fukushima's already-completed spring meet (0411-0426). **Likely leak-inflated — see §4.**                                       | 2026       |
| **Genuine live serve**    | Rows generated the evening before race day, incrementally, matching the real cadence a user would have seen. Only ONE such day exists at these 3 venues: race day **0704** (2026-07-04), predicted 2026-07-03, split `jra-cb-v9-sim-2013` (leaky, pre-cutover) vs `jra-cb-v9-sim-2013-clean` (post-cutover). | 2026       |

Everything else in the table for these venues/years (`jra-rs-lgbm-v1.0`,
`lambdarank-jra-v1*`, `jra-trans-lgbm-ensemble-*`, `jra-cb-v5-single`,
`jra-cb-v6-stacked`, `jra-cb-v7-lineage`, `jra-merged-20yr-ensemble-v1.0`,
`win5-xgb-v7-lineage-v1-rs-overlay-*`) is either a different product (WIN5
overlay) or a one-off research/backfill snapshot (1-4 races only) — excluded
throughout.

**Market-favorite baseline** below is always computed on the _same_ race set as
the model row it's compared to (venue-scoped, not the JRA-wide pooled figure),
so deltas are apples-to-apples.

## 1. Accuracy tables: 2024/2025 reconstruction (armB, leak-clean)

Pooled JRA baseline (all venues, same armB lineage): top1 33.63% / place2 18.02%
/ place3 14.13% / top3_box 9.45%.

### venue × year (rank1 / place2 / place3 / place4 / place5 / place6 / top3_box, %)

| venue     | year | races | top1  | place2 | place3 | place4 | place5 | place6 | top3_box | mkt-fav top1\*       |
| --------- | ---- | ----- | ----- | ------ | ------ | ------ | ------ | ------ | -------- | -------------------- |
| hakodate  | 2024 | 144   | 34.72 | 18.06  | 13.19  | 13.89  | 11.81  | 14.58  | 12.50    | 37.41 (pooled 24+25) |
| hakodate  | 2025 | 144   | 39.58 | 15.28  | 9.72   | 11.11  | 13.89  | 9.72   | 9.03     | 37.41                |
| fukushima | 2024 | 240   | 31.25 | 13.75  | 14.58  | 9.58   | 9.58   | 7.08   | 7.92     | 30.06                |
| fukushima | 2025 | 240   | 29.17 | 15.00  | 16.25  | 14.17  | 10.42  | 9.17   | 9.17     | 30.06                |
| kokura    | 2024 | 288   | 37.50 | 15.97  | 12.85  | 14.24  | 7.64   | 10.76  | 9.38     | 31.88                |
| kokura    | 2025 | 240   | 26.25 | 18.33  | 12.08  | 9.17   | 9.17   | 7.92   | 5.83     | 31.88                |

\* market-favorite top1 hit-rate, venue-scoped, pooled across both years (n:
hakodate 286, fukushima 479, kokura 527 races).

Read: Hakodate is the model's best venue both years (top1 34.7/39.6%, clearly
above its own 37.4% market-favorite baseline in 2025, roughly at parity in
2024). Kokura swings hard between years (37.5% in 2024, only 26.25% in 2025 —
below its 31.9% market-favorite baseline), the largest single-venue year-over-year
swing of the three. Fukushima is flat and modestly below the pooled JRA
baseline both years.

### class × venue (grade_code; only "unknown" and "E" clear ≥30-race threshold)

| grade   | venue     | races | top1  | place2 | place3 | top3_box |
| ------- | --------- | ----- | ----- | ------ | ------ | -------- |
| unknown | hakodate  | 214   | 41.12 | 15.89  | 12.15  | 13.55    |
| unknown | fukushima | 358   | 29.33 | 13.41  | 15.64  | 9.22     |
| unknown | kokura    | 390   | 31.79 | 17.44  | 11.79  | 7.69     |
| E       | hakodate  | 66    | 27.27 | 21.21  | 9.09   | 3.03     |
| E       | fukushima | 109   | 34.86 | 18.35  | 15.60  | 6.42     |
| E       | kokura    | 128   | 33.59 | 16.41  | 15.62  | 8.59     |

("unknown" = non-graded conditions races, the bulk of the local circuit
calendar at these 3 venues; "E" = the lowest graded class band actually run
here in volume.)

### distance band × venue (top1 / top3_box, %; races ≥30)

| band         | hakodate              | fukushima             | kokura               |
| ------------ | --------------------- | --------------------- | -------------------- |
| sprint       | 40.48 / 11.90 (n=42)  | 31.08 / 8.11 (n=74)   | 27.78 / 5.56 (n=54)  |
| mile         | 35.63 / 8.05 (n=87)   | 31.53 / 14.41 (n=111) | 27.48 / 6.11 (n=131) |
| intermediate | 41.23 / 12.28 (n=114) | 29.61 / 6.70 (n=179)  | 32.82 / 7.18 (n=195) |
| long         | — (n<30)              | 26.79 / 3.57 (n=56)   | 36.11 / 9.72 (n=72)  |
| extended     | — (n<30)              | 31.67 / 8.33 (n=60)   | 39.47 / 11.84 (n=76) |

Kokura's longer bands (long/extended) clearly outperform its own shorter
bands — the opposite pattern from Hakodate/Fukushima, where sprint/intermediate
are strongest. This is a real, if modest, venue×distance interaction, but note
distance-band routing is architecturally the same "cell fragmentation" problem
already closed out in `project_venue_cell_round2_2026_06_20` — see §5.

### surface × venue (top1 / top3_box, %; races ≥30)

| surface | hakodate              | fukushima            | kokura               |
| ------- | --------------------- | -------------------- | -------------------- |
| turf    | 38.24 / 9.41 (n=170)  | 28.91 / 9.38 (n=256) | 32.33 / 8.67 (n=300) |
| dirt    | 35.59 / 12.71 (n=118) | 30.73 / 7.81 (n=192) | 31.25 / 4.55 (n=176) |

### waku (post position, keyed on model's rank-1 pick) × venue (top1 / top3_box, %; races ≥30)

| waku band   | hakodate              | fukushima             | kokura                |
| ----------- | --------------------- | --------------------- | --------------------- |
| inner (1-2) | 35.82 / 11.94 (n=67)  | 28.33 / 10.00 (n=120) | 27.59 / 12.07 (n=116) |
| mid (3-6)   | 38.97 / 11.03 (n=136) | 31.60 / 6.49 (n=231)  | 32.34 / 7.43 (n=269)  |
| outer (7-8) | 35.29 / 9.41 (n=85)   | 29.46 / 10.85 (n=129) | 36.36 / 4.90 (n=143)  |

Kokura's outer posts (7-8) show its best top1 rate (36.36%, above its own
mid-band 32.34%) but worst top3_box (4.90%) — a single-slot win, weak depth
pattern. Not large enough or consistent enough across venues to argue for a
standalone post-position lever (this overlaps `feature_draw_ablation` /
"meetingday×waku" territory already REJECTed — see §5).

## 2. Miss taxonomy: when the model's rank-1 pick lost, who won instead?

n = miss races (model's predicted-#1 horse did not finish 1st), 2024+2025
pooled, armB reconstruction: **fukushima 335, kokura 359, hakodate 181** (roughly
60-65% of all races at each venue — consistent with a ~35-40% top1 rate).

### Winner's profile in miss races vs the model's own rank-1 pick (contrast), %

| dimension                                             | fukushima (winner / model-pick)                                                          | hakodate (winner / model-pick)                                                         | kokura (winner / model-pick)                                                            |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| model-rank bucket (winner only, no model-pick analog) | rank2 32.8 · rank4-6 28.1 · rank7+ 22.1 · rank3 17.0                                     | rank2 30.4 · rank4-6 24.9 · rank3 22.7 · rank7+ 22.1                                   | rank4-6 32.6 · rank2 26.7 · rank3 20.6 · rank7+ 20.1                                    |
| ninkijun bucket                                       | 2-3位 46.3 / 90.7 · 4-6位 27.8 / 9.0 · 7+位 23.0 / 0.3 · 1番人気 3.0 / —                 | 2-3位 47.0 / 88.4 · 4-6位 26.0 / — · 7+位 22.7 / — · 1番人気 4.4 / 11.6                | 2-3位 44.6 / 85.7 · 4-6位 31.2 / — · 7+位 20.6 / — · 1番人気 3.6 / 14.3                 |
| waku bucket                                           | mid 47.5 / 47.2 · outer 25.7 / 27.2 · inner 26.9 / 25.7                                  | mid 54.7 / 45.9 · outer 29.3 / 30.4 · inner 16.0 / 23.8                                | mid 48.5 / 51.0 · outer 27.0 / 25.5 · inner 24.5 / 23.5                                 |
| running style (target, descriptive)                   | senkou 22.7/27.8 · sashi 17.6/20.9 · nige 11.3/6.3 · oikomi 10.4/7.2 · unknown 37.9/37.9 | senkou 22.1/15.5 · sashi 14.9/27.1 · nige 9.4/7.2 · oikomi 8.8/5.5 · unknown 44.8/44.8 | senkou 23.7/28.9 · sashi 20.6/22.4 · nige 11.7/5.6 · oikomi 6.7/5.6 · unknown 37.3/37.5 |
| class transition                                      | same 76.7/79.1 · up 11.3/7.5 · no-prior 9.3/8.1 · down 2.7/5.4                           | same 74.6/71.8 · down 9.4/9.4 · no-prior 8.8/8.3 · up 7.2/10.5                         | same 78.3/76.5 · up 10.6/12.6 · no-prior 7.0/6.4 · down 4.2/4.5                         |
| layoff band                                           | ≤30d 29.9/44.2 · 61-90d 22.7/16.4 · 31-60d 22.4/18.2 · >90d 15.8/13.1                    | ≤30d 47.0/51.4 · 61-90d 15.5/13.3 · 31-60d 15.5/16.0 · >90d 13.3/11.0                  | ≤30d 40.7/57.4 · 61-90d 17.5/13.2 · 31-60d 23.1/15.1 · >90d 15.8/7.8                    |

### Top miss archetypes (share of that venue's miss races)

**Fukushima** (n=335): (1) model's rank-2, a 2-3位 pick, wins instead — 29.3%
(98 races); (2) a genuine mid-pack upset, model rank4-6 = ninki 4-6 — 23.3% (78);
(3) a true longshot upset, model rank7+ = ninki 7+ — 20.6% (69); (4) model's
rank-3, still a 2-3位 pick, wins — 14.6% (49).

**Hakodate** (n=181): same ordering — rank2/ninki2-3 26.5% (48), rank4-6/ninki4-6
22.1% (40), rank7+/ninki7+ 21.0% (38), rank3/ninki2-3 19.3% (35).

**Kokura** (n=359): ordering flips — mid-pack upset (rank4-6/ninki4-6) is the
single largest archetype at 27.9% (100), ahead of rank2/ninki2-3 at 23.1% (83);
rank3/ninki2-3 18.7% (67); true longshot rank7+/ninki7+ 18.4% (66).

**Cross-venue read**: at all three venues, when the model is wrong it is
overwhelmingly wrong about a **close call among the top few market picks**, not
a blind spot for deep longshots — "true upset" (winner ninki 7+) is only
~18-23% of misses, versus ~45-47% where the winner was already ninki 2-3 (a
horse the market itself rated as a live contender). Kokura is the one outlier:
its plurality miss pattern is the mid-pack (ninki 4-6) upset rather than the
close photo-finish miss, consistent with Kokura's larger year-over-year top1
swing in §1. Class-transition and layoff bands show **no differential signal**
in misses (winner distribution tracks the model-pick distribution almost
exactly on both dimensions at all 3 venues) — this is descriptive
confirmation, not new evidence, for the existing class-ordinal and
layoff/summer-slot REJECTs (§5).

## 3. Asymmetry vs. the market: does the model's disagreement add value?

Races where the model's rank-1 horse ≠ the market favorite horse, 2024+2025
pooled:

| venue     | divergence races | divergence rate | model-pick win% | market-fav win% | edge (model − market, pp) |
| --------- | ---------------- | --------------- | --------------- | --------------- | ------------------------- |
| kokura    | 66               | 12.5%           | 24.24           | 19.70           | **+4.55**                 |
| fukushima | 41               | 8.6%            | 26.83           | 24.39           | **+2.44**                 |
| hakodate  | 27 (small n)     | 9.4%            | 25.93           | 29.63           | **−3.70**                 |

Kokura and Fukushima both show a positive edge for the model when it disagrees
with the crowd — modest, and on samples too thin (41-66 races) to claim
statistical significance, but directionally consistent with the model adding
value exactly in the situation the user cares about (オッズ乖離). Hakodate
shows the opposite sign, but n=27 is well below a reliable threshold — treat as
noise, not a real venue effect, until more divergence races accumulate.

## 4. 2026 reflection: what did the degraded-serve era actually cost here?

Two findings reshape what can honestly be said about 2026 at these venues.

### 4a. A ~23% prediction-serving blackout, not (only) an accuracy problem

Cross-referencing the real 2026 race calendar (local PG `jvd_ra`) against
`race_finish_position_model_predictions` (any model_version) shows **84 of 372
already-run races (22.6%) at these 3 venues got zero finish-position
prediction rows of any kind**, between the iter14 backfill (2026-06-04, itself
only covering already-completed meets) and the clean-model cutover
(2026-07-03 evening). Concretely: Hakodate's entire early summer meet
(0613/0614/0620/0621, 48 races) plus 0628 (12 races) — 5 of its first 6 race
days, 60 of 72 races — has no logged finish-position prediction at all.
Fukushima and Kokura each lost one summer-meet day (0628, 12 races each) the
same way. Only race day 0627 has a row at all in that window, and it's the
WIN5 overlay product, not finish-position. This is an **operational gap**,
independent of model quality — worth an infra follow-up (is the
Queues→Container trigger actually firing for these 3 venues' summer meets?),
not a modeling lever.

### 4b. The iter14 backfill numbers are very likely leak-inflated — don't use them as "before"

The iter14 batch (Kokura winter + Fukushima spring, generated 2026-06-04) shows
top1 38.89% (fukushima) / 44.76% (kokura), edges of **+19.4pp / +14.7pp** over
its own venue-scoped market-favorite baseline — far larger than anything in
the leak-clean armB reconstruction (§1: recomputing top1 minus venue-scoped
market-fav baseline per venue×year, the largest edge there is kokura-2024 at
+5.62pp — an order of magnitude smaller). The model
name itself ("pacestyle-course") and its 2026-06-04 vintage place it squarely
in the pre-clean-retrain lineage flagged in `project_target_corner_leak_2026_07_04`:
`target_corner_1_norm/3/4` and `target_running_style_class` are NULL at true
serve time (the race hasn't happened yet) but were **fully populated with the
real outcome** when this batch was computed retroactively over
already-finished races. That would mechanically inflate exactly these
numbers. This wasn't independently re-verified against iter14's exact stored
feature list (not retained on local disk), so it's flagged as a
well-supported hypothesis, not a confirmed fact — but on this evidence, the
iter14 backfill numbers should **not** be read as "what iter14 achieved in
production" and should not be cited as a pre-clean-deploy baseline anywhere
else in this campaign (flag for task #25's sibling sweep too).

### 4c. The one genuine live day (0704) has no result yet

Race day 0704 is the only 2026 date at these 3 venues with true incremental
live-serve rows for both `jra-cb-v9-sim-2013` (leaky, 5 races logged across the
3 venues) and `jra-cb-v9-sim-2013-clean` (36 races logged). **As of this
analysis, `jvd_se.kakutei_chakujun='00'` for all of them — today's races have
not finished/been confirmed yet.** The leaky-vs-clean before/after comparison
the team asked for cannot be computed right now; re-running
`era_2026_reflection.py` after today's results post (likely within hours) will
give the first-ever genuine live read on the clean deploy at these venues. The
honest 2026 baseline available today is: the armB 2025 reconstruction levels
in §1 (26.25-39.58% top1 depending on venue) are the best estimate of what a
leak-free model should show once live data accumulates.

## 5. Hypothesis candidates for sibling sweep agents

Cross-checked against the DO-NOT-RETEST catalogue (same-day bias, draw
affinity/ablation, straight×closer, meetingday×waku, jockey static+momentum
rates, pedigree sire/line, class-ordinal, market×tokubetsu, style-rivalry,
meet-repeat, weather, barei, blinker, odds-entropy field difficulty,
hokkaido-turf-first-timer, layoff/prior-finish summer slots).

- **No new feature-engineering lever survives this cut.** Every
  venue-differentiated pattern found here (waku/post-position by venue,
  distance-band by venue, class-transition, layoff) either matches an existing
  REJECT directly (waku↔"meetingday×waku"; class-transition↔"class-ordinal";
  layoff↔"layoff/prior-finish summer slots") or is architecturally the same
  fragmentation problem as `project_venue_cell_round2_2026_06_20` /
  `project_jra_rs_cell_routing_reject_2026_07_03` (distance-band×venue,
  waku×venue) — **do not re-propose these as fresh probes.**
- **Rank-2 "close-call" swap, venue-scoped** — the single largest miss
  archetype at all 3 venues is the model's own rank-2 pick winning instead
  (23-33% of misses). This is conceptually adjacent to the retired
  `project_etop2_place_preserving_win_2026_06_18` (E-top2 override, superseded
  by sim v9) and the REJECTed `project_score_additive_draw_speed_reject`
  (rank-1-only override family) — flag as **low expected incremental value,
  not a fresh idea**, but note it hasn't been isolated-tested restricted to
  just these 3 venues specifically; if pursued, treat it as a variant of
  already-closed work, not a new hypothesis.
- **Market-divergence edge at Kokura/Fukushima (+4.55pp/+2.44pp, §3)** is the
  one genuinely positive-looking signal, but on 41-66 races it's far below the
  sample sizes needed anywhere else in this campaign (`feedback_eval_class_subgroup_mandatory`
  implies cell-level deltas need much larger n before being trusted) and a
  venue-specific "trust the model more when it diverges" rule falls into the
  same venue-routing fragmentation trap as item 1. **Not actionable as a
  standalone lever without materially more divergence races accumulating.**
- **Operational, not modeling**: (a) investigate the Hakodate/Fukushima/Kokura
  summer-meet prediction blackout (§4a) — this is infra, hand to whoever owns
  the Queues/Container trigger; (b) do not cite iter14 2026 backfill numbers
  (§4b) as a pre-clean-deploy baseline in any other analysis this campaign
  produces.

## 反省点まとめ (日本語)

- 函館・福島・小倉の3場では、モデルのrank-1が外れたレースの **75-80%以上が
  「大外れ」ではなく「僅差の誤答」** だった。当てられなかった時に実際に勝った馬は
  ほとんどの場合、モデル自身のrank-2/3位予想馬か、市場の2-3番人気馬であり、
  7番人気以下の純粋な大穴が勝つケースはミスの2割程度にとどまる。過去のE-top2
  型オーバーライド系の施策がここでも近い発想になるが、既にsim v9系へ
  superseded/REJECT済みの領域と重なるため、新規性は低い。
- オッズ乖離 (モデルが市場人気馬と異なる馬を1位予想する) は小倉・福島では
  プラスに効いている (小倉+4.55pp、福島+2.44pp)。函館は逆符号だがサンプル数
  (27レース) が小さすぎて雑音の可能性が高い。方向性としては
  「モデルの乖離は価値を持つ」という仮説を支持するが、標本数不足でこのまま
  施策化はできない。
- 2026年の「劣化サーブ期間」を定量化しようとしたところ、想定外の事実が2つ
  見つかった。(1) 6/4のiter14一括バックフィル後から7/3のclean切替まで、
  この3場の実レース372走中84走 (22.6%) が着順予測を一切生成されていない
  「空白期間」だった — 函館は夏開催の最初の6日中5日 (60/72レース) が対象。
  これはモデル精度の問題ではなくサーブ経路自体の欠落であり、別途インフラ調査が
  必要。(2) 6/4のiter14バックフィル数値 (福島top1 38.89%・小倉44.76%、市場人気
  比+15~19pp) はarmB再構築 (leak-free) と比べて異常に高く、target_corner/
  running_style leak (7/4修正済) が「レース終了後に一括生成」という条件下で
  逆に本物の結果を特徴量に混入させた可能性が高い。したがってこのbackfill数値を
  「clean化前の本番精度」として引用してはならない。
- 唯一の真の本番ライブ日 (7/4、predicted 7/3) は、leaky/clean両方のログが
  揃っているにもかかわらず、分析時点でまだ全レースが未確定 (`kakutei_chakujun='00'`)
  だったため、leaky-vs-clean比較は今回できなかった。結果確定後の再実行が必要。
