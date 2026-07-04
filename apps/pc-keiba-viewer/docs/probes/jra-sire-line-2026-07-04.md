# JRA Sire-Line (系統) Aggregate Pedigree Features — Clean Baseline WF (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **USER condition**: D — 競馬場×class×距離×血統(父/母父/父父)の勝率 (venue x class x
  distance x bloodline win-rate), with special focus on the 4 summer venues
  (`keibajo_code` 01=Sapporo, 02=Hakodate, 03=Fukushima, 10=Kokura). Sapporo +
  Hakodate turf is 洋芝 (the only JRA venues with it).
- **Hypothesis under test**: individual-sire EB rates were REJECTed earlier today
  (`docs/probes/jra-pedigree-winrate-clean-2026-07-04.md` — flagship
  `sire_yoshiba_top3` failed pooled/summer/yoshiba-only, first-crop sires NULL,
  coverage-thin cells). LINE-level aggregation (父系統, e.g. サンデーサイレンス系/
  ミスタープロスペクター系/ノーザンダンサー系) pools 10-100x more offspring per group
  than an individual sire — this probe tests whether that pooling produces a
  _stable_ effect precisely where the individual-sire construction lacked n. This
  is a materially different construction from the REJECTed one (grouped by
  ancestor-line, not by individual sire), not a retest.

## STEP 1 — Data verification (done BEFORE building any feature, per the NAR

`nvd_um`/`nvd_nu` lesson: verify, don't assume)

Checked local Postgres (port 15432) for an explicit sire→系統 mapping table:

- **`jvd_hn`** (JRA 繁殖馬マスタ / breeding-horse master, 161,439 rows): columns are
  `record_id, data_kubun, data_sakusei_nengappi, hanshoku_toroku_bango, yobi_1,
ketto_toroku_bango, yobi_2, bamei, bamei_hankaku_kana, bamei_eur, seinen,
seibetsu_code, hinshu_code, moshoku_code, mochikomi_kubun, yunyu_nen, sanchimei,
ketto_joho_01a, ketto_joho_02a` — **no keito/系統 column at all** (only a
  single-generation sire/dam code pair).
- **`jvd_bt`** DOES have `keito_id` / `keito_mei` / `keito_setsumei` columns — this
  looked like exactly the mapping table the task hypothesized. Inspected it fully:
  it is a **92-row glossary** of named branch-representative stallions (e.g.
  `keito_mei` = "サンデーサイレンス", "テスコボーイ", "トニービン", "クロフネ",
  "ジャングルポケット", each with a hierarchical `keito_id` code like
  `010108020101020104` and a prose `keito_setsumei` description lifted from a stud-book
  glossary), keyed by the `hanshoku_toroku_bango` of that _one_ representative horse.
  It has **no per-runner or per-arbitrary-sire join key** — only 92 of the 1,222
  distinct sires actually siring 2013-2025 JRA runners appear in it directly, and
  using it would require a multi-generation ancestor-trace recursion (walk each
  horse's direct paternal line back through `jvd_um`'s `ketto_joho` chain until
  hitting one of the 92 named branch points) which is out of scope for this probe's
  time budget. **Not used as a direct per-horse lookup.**
- No other table in the 115-table local PG schema has a `keito`/`系統`-shaped column
  (checked via `duckdb_columns()` search across all attached tables).

**Path taken**: the pre-approved 2-generation PROXY. `jvd_um` (213,286 rows) carries
a full 3-generation, 14-ancestor pedigree (`ketto_joho_01`..`14`), so both a
sire-line and a damsire-line proxy are directly available with no extra joins or
recursion:

- **`sire_line`** = `ketto_joho_03b` (父父, "FF" = paternal grandsire — the sire's
  own sire). This is the direct 系統-tree axis: e.g. for a horse sired by Kizuna,
  FF = Deep Impact; for a horse sired by Deep Impact, FF = Sunday Silence.
- **`damsire_line`** = `ketto_joho_11b` (母父父, "MFF" = the damsire's own sire).
  Analogous 2-gen proxy for the damsire axis.

Verified group structure on the 2013-2025 JRA runner-row population
(`jvd_se` join `jvd_ra` join `jvd_um`, n=660,834 horse-runs) **before** writing any
feature SQL:

| Axis                          | Distinct groups | Coverage | p10 | p25 | median | p75   | p90     | Top group (n)                                |
| ----------------------------- | --------------- | -------- | --- | --- | ------ | ----- | ------- | -------------------------------------------- |
| individual sire (prior probe) | 1,222           | ~100%    | —   | —   | —      | —     | —       | サンデーサイレンス (largest individual sire) |
| **sire_line (FF)**            | 413             | 99.99%   | 5.2 | 19  | 82     | 755   | 3,307   | サンデーサイレンス n=142,619                 |
| **damsire_line (MFF)**        | 576             | 99.99%   | 5.0 | 18  | 90.5   | 423.5 | 1,781.5 | サンデーサイレンス n=139,708                 |

FF's top group (サンデーサイレンス, 142,619 runner-rows) is **~17x** the size any
single individual sire could reach in the same population, and the top-15 lists for
both axes are dominated by exactly the lines racing lore would predict (Sunday
Silence, Mr. Prospector, Northern Dancer, Halo, Kingmambo, Storm Cat, Roberto,
Danzig families) — confirms FF/MFF are a coherent line proxy, not noise, and deliver
the "10-100x pooling" the hypothesis required. (Note: at the 3-gen level,
`ketto_joho_07b`/FFF, the same lines reappear but sometimes double-counted under two
spellings, e.g. "サンデーサイレンス" and "Sunday Silence" as separate string values in
different records — the 2-gen FF/MFF axis used here did not show this spelling
split in its top-20, so it was kept at 2 generations as instructed rather than
extended to 3.)

## STEP 2 — Candidate columns

Built by `tmp/candidate-jra-sire-line/build_sire_line.py` from local Postgres
`jvd_se`/`jvd_ra`/`jvd_um` (port 15432). All strictly prior (expanding window,
`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`, ordered by
`(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)`). Empirical-Bayes
shrinkage `k=30` (lighter than the individual-sire probe's `k=50` — lines have far
more n per cell so a lighter shrink is appropriate) toward the **line's own**
running overall top3 rate. Race-level pre-aggregation + DuckDB window functions
ONLY — no horse-level self-join (the memory-safety pattern from
`build_draw_ablation.py` / `build_pedigree_winrate.py`). Built in 7.3s, no memory
incident (`memory_pressure -Q` stayed >= 37% free throughout; peaked around 37%
during the WF harness's CatBoost training, never approached the stop threshold).

| Column                      | Definition                                                                                                                                                                        | Coverage (2013-2025 store) |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `sire_line_surface_top3`    | sire-line (FF) offspring prior top3 rate by surface (turf/dirt)                                                                                                                   | 99.906%                    |
| `sire_line_dist_top3`       | sire-line (FF) offspring prior top3 rate at this row's dist-band                                                                                                                  | 99.869%                    |
| `sire_line_yoshiba_top3`    | **flagship** — sire-line (FF) offspring prior top3 rate specifically on Sapporo/Hakodate-turf races, via a `FILTER (WHERE ...) OVER (...)` cumulative window visible on every row | 98.966%                    |
| `sire_line_summer_top3`     | sire-line (FF) offspring prior top3 rate on the 4 summer venues pooled (01/02/03/10), same FILTER-window pattern                                                                  | 99.772%                    |
| `damsire_line_surface_top3` | damsire-line (MFF) offspring prior top3 rate by surface — direct line-level analogue of the REJECTed individual `damsire_surface_top3`                                            | 99.892%                    |

All well above the 40% coverage floor; no columns dropped for coverage.

## Method

- **Harness**: `tmp/candidate-jra-sire-line/wf_sire_line.py` — self-contained copy
  of `tmp/candidate-jra-pedigree-winrate-clean/wf_pedigree.py`'s harness pattern
  (itself copied from `tmp/candidate-masked-lever-retest/retest_wf.py`), including
  the race_id-sort-before-mask bug fix documented there. The **control-arm models
  are reused read-only** from `tmp/candidate-masked-lever-retest/models/base/`
  (identical spec/seeds/folds, already trained by the sibling masked-lever/pedigree
  campaigns) — only the +5-col treatment models are trained fresh here.
- **Baseline (control)**: CLEAN `armB`, 250 feat, no leak cols.
- **Treatment**: control + 5 sire-line candidate columns (additive).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`, no
  early-stop, `cat_indices=[]` (all-numeric) — matches deployed
  `jra-cb-v9-sim-2013` exactly.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds before
  the paired bootstrap.
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box`, `fukusho_2p`.
  Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment − control`.
- **Accept gate**: >=2/3 primaries `delta_pp >= +0.08` AND `LB95 > 0`; AND >=1 of
  `{place2, place3}` passes; AND no metric regresses below `-0.05pp`.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` / `kyori_band` /
  `season_band` / `current_baba_condition`, `n >= 200`. PLUS a summer-restricted
  subset (`keibajo_code in {01,02,03,10}`, `n >= 100`) and a Sapporo+Hakodate-only
  (洋芝-eligible) subset, given the flagship candidate targets exactly that cell.
  **PRIMARY TARGET**: the summer + 洋芝 (Sapporo+Hakodate turf) cell needs
  `delta_pp >= +0.08` AND `LB95 > 0` on that cell's primaries, multi-seed stable,
  with no global regression.

## Result: REJECT (pooled, summer-restricted, AND yoshiba-only)

Harness: `tmp/candidate-jra-sire-line/wf_sire_line.py`, completed in 710.1s (11.8min).
Full report: `tmp/candidate-jra-sire-line/reports/sire_line.json`.

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.816 | +0.019     | -0.158 |
| place2     | 18.119 | 18.209 | +0.090     | -0.122 |
| place3     | 14.163 | 14.092 | -0.071     | -0.312 |
| place4     | 12.166 | 12.163 | -0.003     | -0.225 |
| place5     | 11.076 | 10.957 | -0.119     | -0.322 |
| place6     | 10.417 | 10.532 | +0.116     | -0.093 |
| top3_box   | 9.410  | 9.326  | -0.084     | -0.203 |
| fukusho_2p | 74.912 | 74.900 | -0.013     | -0.180 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.119` (place5) —
this **exceeds** the `-0.05` no-reg bound (unlike the individual-sire probe's pooled
worst of `-0.074`, which stayed within it) → **ACCEPT_strict_gate=false**, and this
line-pooled construction is measurably _more_ harmful pooled than the individual-sire
version was.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.154[-0.453] | -0.212[-0.598] | -0.106[-0.540] |
| 2024 | +0.241[-0.049] | +0.357[-0.048] | +0.251[-0.154] |
| 2025 | -0.029[-0.357] | +0.125[-0.222] | -0.357[-0.791] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.222[-0.087] | +0.193[-0.183] | -0.154[-0.521] |
| 101  | -0.183[-0.492] | -0.068[-0.463] | +0.039[-0.328] |
| 2026 | +0.019[-0.299] | +0.145[-0.232] | -0.097[-0.521] |

Same sign-instability pattern as the individual-sire probe: every primary flips sign
across at least 2 of 3 seeds and 2 of 3 folds (seed42 top1/place2 positive but
place3 negative; seed101 top1/place2 negative, place3 positive; 2023 all three
negative; 2024 all three positive; 2025 top1/place3 negative, place2 positive) —
consistent with noise around a near-zero mean, not a stable effect. Line pooling did
not remove the instability the individual-sire construction showed.

### Summer-restricted (`keibajo_code` in {01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura}, n=2,448)

| Metric | Base   | Cand   | Delta (pp) | LB95   |
| ------ | ------ | ------ | ---------- | ------ |
| top1   | 32.108 | 32.067 | -0.041     | -0.422 |
| place2 | 16.217 | 16.258 | +0.041     | -0.395 |
| place3 | 13.508 | 13.222 | -0.286     | -0.790 |

Gate: `0/3 primaries`, worst_delta -0.2859 (place3) → **REJECT**, and worse than the
individual-sire probe's summer-restricted worst (-0.1225, top1) — no near-miss here
at all (individual-sire had a place2 near-miss at +0.42[-0.055]; this line-pooled
version's place2 is only +0.04).

### Yoshiba-only (Sapporo+Hakodate, n=936) — the flagship candidate's own target cell, and the direct pooling-vs-individual comparison

| Metric | Base   | Cand   | Delta (pp) | LB95       |
| ------ | ------ | ------ | ---------- | ---------- |
| top1   | 34.046 | 34.473 | +0.427     | -0.143     |
| place2 | 17.201 | 17.058 | -0.143     | -0.891     |
| place3 | 12.429 | 12.180 | -0.249     | -1.068     |
| place5 | 13.533 | 12.821 | **-0.712** | **-1.531** |

Gate: `0/3 primaries LB95>0`, worst_delta **-0.7123** (place5 regression, exceeds the
`-0.05` no-reg bound by a wide margin) → **REJECT**.

**Direct comparison against the individual-sire version's yoshiba-only result**
(`docs/probes/jra-pedigree-winrate-clean-2026-07-04.md`: top1 `+0.392[-0.321]`,
place2 `+0.356[-0.570]`, place3 `+0.463[-0.285]`, place5 `-0.499[-1.247]`):

| Primary           | Individual-sire delta[LB95] | Sire-line delta[LB95] | Interval tighter?                 |
| ----------------- | --------------------------- | --------------------- | --------------------------------- |
| top1              | +0.392 [-0.321]             | +0.427 [-0.143]       | **yes**, marginally               |
| place2            | +0.356 [-0.570]             | -0.143 [-0.891]       | **no** — flipped sign, wider      |
| place3            | +0.463 [-0.285]             | -0.249 [-1.068]       | **no** — flipped sign, much wider |
| place5 regression | -0.499 [-1.247]             | -0.712 [-1.531]       | **no** — worse regression         |

Line-pooling tightened the LB95 interval on exactly one of three primaries (top1,
marginally: -0.143 vs -0.321) but **flipped place2 and place3 from positive to
negative** and made the place5 regression meaningfully worse (-0.712 vs -0.499, both
well past the no-reg bound). This directly refutes the "pooling stabilizes the
signal" hypothesis: at n=936 races, going from ~1,222 individual-sire groups to 413
line groups did not tighten the confidence interval where it mattered (place2/place3)
— it made the point estimate direction _less_ consistent across primaries and
increased the collateral regression on place5. The most likely explanation: a
"sire line" (all Sunday-Silence-descended sires pooled together, for example) is too
coarse an axis for turf/dirt aptitude — individual sires within the same line differ
substantially in their own offspring's 洋芝 suitability (that's exactly why breeders
distinguish specific stallions, not just lines, when assessing turf aptitude), so
pooling across the line averages away real sire-specific signal rather than
reducing noise around a shared line-level truth.

### Per-venue breakdown within summer (seed-avg, primaries delta[LB95], n)

| Venue                | top1           | place2         | place3         |
| -------------------- | -------------- | -------------- | -------------- |
| 01 Sapporo (n=504)   | +0.397[-0.399] | +0.331[-0.728] | -0.595[-1.786] |
| 02 Hakodate (n=432)  | +0.463[-0.232] | -0.694[-1.698] | +0.154[-1.080] |
| 03 Fukushima (n=720) | -0.602[-1.343] | -0.185[-1.065] | -0.046[-1.065] |
| 10 Kokura (n=792)    | -0.084[-0.673] | +0.463[-0.253] | -0.547[-1.347] |

No venue clears LB95>0 on any primary. The two 洋芝 venues disagree in sign on
place3 (Sapporo -0.595 vs Hakodate +0.154) just as the individual-sire probe's two
洋芝 venues disagreed on place2 — the "shared 洋芝 aptitude" mechanism still doesn't
hold up at either the individual-sire or the line level.

### Cell scan (`keibajo_code` / `kyori_band` / `season_band` / `current_baba_condition`, n>=200, seed-avg)

22 cells x 3 primaries = 66 tests; 3 had `LB95>0` on a primary
(`keibajo_code=06` Nakayama `place3 +0.600[+0.022]` n=1500; `kyori_band=3` (2200+)
`place3 +0.958[+0.124]` n=800; `season_band=2` `top1 +0.454[+0.067]` n=2495) — a
~4.5% hit rate at nominal alpha=0.05 with no multiple-comparison correction, i.e.
consistent with chance (slightly below the individual-sire probe's ~6% hit rate on
the same cell grid). None of the 3 hits are in the target venue/summer dimension,
and none replicate across the other two related primaries at the same cell.

## Overall conclusion: REJECT — DO-NOT-RETEST

None of the 5 sire-line/damsire-line candidates (`sire_line_surface_top3`,
`sire_line_dist_top3`, `sire_line_yoshiba_top3`, `sire_line_summer_top3`,
`damsire_line_surface_top3`) clear the accept gate pooled, summer-restricted, or on
the flagship 洋芝-only cell. The pooled effect is near-zero and sign-unstable across
seeds/folds, and (unlike the individual-sire probe) actually breaches the -0.05
no-regression bound pooled (place5 -0.119pp). The 洋芝-only cell — the strongest a
priori hypothesis and the whole point of testing line-level pooling — is _worse_
than the individual-sire version: place2/place3 flip from positive to negative and
the place5 regression deepens from -0.499pp to -0.712pp. **The core hypothesis under
test (line-pooling trades individual-sire noise for line-level stability) is
refuted by the data**, not just unconfirmed: more pooling produced a less coherent,
not more coherent, cross-primary signal on the exact cell it was supposed to help
most. Combined with the individual-sire probe's REJECT from earlier today, this
closes out condition D's sire/damsire/系統 axis for JRA at both the individual and
line granularity: CatBoost depth=8 trained on the 250-feature clean armB spec
already captures whatever sire/venue/surface/distance bloodline signal exists via
tree interactions between the many existing sire/damsire win-rate columns
(`sire_distance_win_rate`, `sire_track_win_rate`, `dam_sire_distance_win_rate`,
`sire_baba_win_rate`, `sim_sire_win_rate`, `sim_damsire_win_rate`, etc.) and
`keibajo_code`/`track_code` — neither adding explicit per-sire nor explicit
per-line venue/yoshiba/surface-rate columns supplies information the model doesn't
already have access to. **DO-NOT-RETEST** this exact candidate set (FF/MFF 2-gen
proxy) on this baseline. A materially different construction (e.g. the true
`jvd_bt`-glossary-anchored 系統 via multi-generation ancestor-trace recursion to one
of the 92 named branch points, rather than the FF/MFF 2-gen proxy) would be a new
hypothesis with a real cost (recursive lineage walk, likely thin coverage past the
92 glossary anchors) and is not recommended given today's REJECT pattern at both
granularities already tested.

## Artifacts

- Feature build: `tmp/candidate-jra-sire-line/build_sire_line.py` ->
  `tmp/candidate-jra-sire-line/sire_line_features.parquet` (660,834 rows, 2013-2025)
- Harness: `tmp/candidate-jra-sire-line/wf_sire_line.py` (includes the race_id-sort
  fix for cell/summer/yoshiba masks, inherited from the sibling probes)
- Report: `tmp/candidate-jra-sire-line/reports/sire_line.json`
- Logs: `tmp/candidate-jra-sire-line/build.log`, `tmp/candidate-jra-sire-line/wf.log`
- Reused (read-only) control-arm models:
  `tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`
  (trained by the sibling masked-lever-retest campaign, identical spec/seeds/folds,
  also reused by today's `jra-pedigree-winrate-clean` probe)
- Trained (this probe) treatment models:
  `tmp/candidate-jra-sire-line/models/sire_line/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`
