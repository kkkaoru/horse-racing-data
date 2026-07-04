# Summer Competitive Arc: 3yo vs 古馬 / Sex / Weight (odds-free check, 2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA descriptive tendency map (NOT a model-feature ablation) — task #29, angle 1 of the
  4-angle odds-independent summer tendency sweep (siblings: #30 baba physics, #31 rotation, #32 pedigree
  seasonality).
- **Trigger**: user wants racing-domain "laws" that hold independent of the betting market — the within-summer
  competitive arc between 3-year-olds and older horses, sex, and weight/handicap structure.
- **Scope**: JRA flat races (`kyoso_shubetsu_code in (11,12,13,14)`), 2015-2026 (2026 = partial year, Jan-Jun
  only as of this date), summer venues 札幌(01)/函館(02)/福島(03)/小倉(10), summer months Jun-Sep, contrasted
  against (a) the same 4 venues in non-summer months and (b) main venues 東京(05)/中山(06)/京都(08)/阪神(09)
  in the same Jun-Sep window. Era-replication buckets: 2015-19 / 2020-23 / 2024-26 (last bucket has only
  2024+2025 as full summers; 2026 contributes June only).
- **Method discipline**: every candidate tendency gets (i) a **band-conditional check** — does the gap persist
  among horses the market rates the same way (`tansho_ninkijun` bands A=1-3 / B=4-6 / C=7+)? and (ii) a
  **partial-correlation check** — a race-clustered-bootstrap logistic regression of the binary win outcome on
  `log(tansho_odds)` + ninkijun-band dummies + the candidate variable, 150-200 bootstrap draws, reporting the
  candidate coefficient's point estimate / LB95 / UB95. A tendency whose coefficient survives this control
  (CI excludes 0, same direction as the raw effect) in a band and/or era is **ODDS-FREE** there; if the raw
  effect is large but the controlled coefficient collapses to ~0, it is **FULLY-PRICED**; in between is
  **PARTIALLY-PRICED**. Era-verdict: **LAW** = same sign in all 3 eras (≥0.1pp), **PARTIAL** = 2/3, **NOISE**
  = mixed/inconsistent. n≥30 per cell minimum, most cells n in the hundreds-to-thousands.
- **Data**: local PG 15432, `jvd_se` (barei/seibetsu_code/futan_juryo/kishu_minarai_code/tansho_odds/
  tansho_ninkijun/kakutei_chakujun) joined to `jvd_ra` (juryo_shubetsu_code/kyoso_shubetsu_code/grade_code) via
  DuckDB `postgres_scanner`, read-only. Base pull: 171,964 rows (75,233 summer-venue/summer-month + 44,093
  summer-venue/other-month + 52,638 main-venue/summer-month) plus a supplementary 443,772-row national
  full-year pull for angle 1's calendar-arc check. Scripts + full JSON results in
  `tmp/candidate-summer-oddsfree-arc/` (`arc_lib.py`, `angle1_age_arc.py`, `angle1b_national_arc.py`,
  `angle2_sex.py`, `angle3_weight.py`, `angle4_interaction.py`, `national_month_check.py`).
- **Column notes confirmed by probe**: `seibetsu_code` 1=牡/2=牝/3=セン; `juryo_shubetsu_code`
  1=ハンデ/2=別定/3=馬齢/4=定量(fixed); `kishu_minarai_code` 0=none, 1-4=apprentice allowance tiers (more
  weight relief as code rises), 9=special; `futan_juryo` is text tenths-of-kg (÷10 for kg, same convention as
  `[[reference-banei-data-location]]` but JRA/NAR not Ban-ei here); `barei` uses the post-2001 foaled-year
  convention (2yo debut, so "3yo" = `barei=3`).

## Part 1 — 3yo vs 古馬(4+): NOT a summer law, but a real calendar-wide age-catch-up law

**Headline correction to the framing**: the classic "3歳馬は夏を越えて力をつける" lore, tested strictly as a
within-summer (Jun→Sep) trajectory, does **not** hold up. But a much stronger and cleaner pattern exists at
the **national, full-year** level that summer happens to sit inside the rising half of.

### 1a. Within-summer weekly arc — NOISE (does not replicate as an intra-summer trend)

Linear-regression slope of (3yo win rate − old(4+) win rate) against week-of-summer, weighted by cell n, per
era (summer venues, `tmp/candidate-summer-oddsfree-arc/angle1_result.json` → `trend_table`):

| Era     | Slope (pp per week) |
| ------- | ------------------- |
| 2015-19 | -0.034              |
| 2020-23 | -0.027              |
| 2024-26 | +0.072              |

Two of three eras trend slightly _down_ across the summer, one trends up — **NOISE**, not LAW. Month-bucket
detail confirms: in 2015-19 the 3yo-old delta actually peaks in July (+1.20pp) and _declines_ into September
(+0.41pp); only 2024-26 shows a monotonic June→August rise (+3.36→+3.77→+4.47pp) before a small-n September
drop (n=245+139, noisy). **Verdict: NOISE — no reliable within-summer arc.**

### 1b. National full-year calendar arc — LAW, and it is NOT summer-specific

Aggregating nationally (all 10 JRA venues, all 12 months, `national_month_check.py`, SQL-side aggregation,
no row materialization needed) reveals the real shape, identical in all 3 eras:

| Era     | Jan   | Feb   | Mar   | Apr   | May   | Jun   | Jul   | Aug   | Sep   | Oct   | Nov   | Dec   |
| ------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| 2015-19 | -0.19 | -0.28 | -0.36 | -0.44 | -0.27 | +0.74 | +1.28 | +1.10 | +1.09 | +2.44 | +2.59 | +3.47 |
| 2020-23 | -0.08 | -0.31 | -0.37 | -0.71 | -0.61 | +2.87 | +3.16 | +3.29 | +4.67 | +6.13 | +6.85 | +6.35 |
| 2024-26 | -0.13 | -0.03 | -0.24 | -0.60 | -0.58 | +3.39 | +3.28 | +4.02 | +4.26 | +6.24 | +5.05 | +7.01 |

(cells: win-rate delta in pp, 3yo minus 4yo+, n in the thousands per cell)

Every era shows the identical shape: 3yo are **behind** older horses Jan-May, cross over to **ahead** in June,
and the gap **keeps growing through year-end** (peak is Nov/Dec, not Aug/Sep). Summer (Jun-Sep) is just the
crossover/early-ramp segment of a longer national arc that continues rising into autumn/winter. The magnitude
is also growing era-over-era (Dec delta: 3.47 → 6.35 → 7.01pp) — a secular trend layered on top of the
seasonal shape. **Verdict: LAW (national, calendar-wide), but explicitly NOT a "summer" phenomenon** — the
team-lead framing of "3yo strengthen through summer" should be corrected to "3yo cross from disadvantage to
advantage in June and keep gaining through the rest of the racing year."

### 1c. Odds-independence check — ODDS-FREE, but concentrated entirely in market favorites

Band-conditional persistence (`angle1b_national_arc.py`, national full-year, split Jan-May vs Jun-Dec):

| Era     | Phase   | Band A(fav,1-3) | Band B(4-6) | Band C(7+) |
| ------- | ------- | --------------- | ----------- | ---------- |
| 2015-19 | Jan-May | **+2.19**       | -0.85       | -0.60      |
| 2015-19 | Jun-Dec | **+1.65**       | +0.19       | +0.04      |
| 2020-23 | Jan-May | **+1.46**       | -0.21       | -0.66      |
| 2020-23 | Jun-Dec | **+5.82**       | +0.68       | +0.17      |
| 2024-26 | Jan-May | **+1.85**       | -0.37       | -0.64      |
| 2024-26 | Jun-Dec | **+7.24**       | +0.68       | -0.04      |

Among market favorites (band A) specifically, a 3yo favorite beats an old-horse favorite in **every single
era×phase cell**, including in Jan-May when 3yo look worse in the raw aggregate — the aggregate negative
delta in Jan-May is a Simpson's-paradox artifact of bands B/C (where most volume sits and old horses hold a
real edge). The favorite-band edge for 3yo also grows sharply from Jan-May to Jun-Dec and across eras (+2.19
→ +7.24pp at its extreme). The controlling logistic regression (`win ~ log_odds + band + is_old`) confirms
this survives odds control: `is_old` coefficient is negative in all 6 era×phase cells, and excludes zero in 5
of 6 (only 2015-19/Jun-Dec is borderline, LB95=-0.076/UB95=+0.003, same_sign_frac=0.96).

**Verdict: ODDS-FREE, but only within ninkijun band A (favorites)** — bands B/C show FULLY-PRICED (no
reliable residual signal). This is a genuine, era-replicated market inefficiency specifically about
_favorite-vs-favorite_ comparisons: when the market makes a 3yo its top pick, that 3yo is a better bet than an
equally-favored older horse, and this gap has been widening for a decade.

**Serve-feasibility**: **LOW, DO-NOT-RETEST overlap** — `[[project_barei_reject_2026_06_24]]` already ran a
654-cell exhaustive ablation of raw age as a JRA model feature (all LB95<0, "career proxy redundant"); that
sweep's mandatory cell dimensions (`[[feedback_eval_class_subgroup_mandatory]]`: category×class×subgroup×
racetrack×season×surface) would have included ninkijun-band-conditional cells. The market-level mispricing
found here is real, but a GBDT with odds/ninkijun and correlated form/class features already in its feature
set (263 feat in `jra-cb-v9-sim-2013`) evidently captures whatever incremental signal age carries through
those other channels — consistent with, not contradicting, the prior REJECT. This is a betting-market
curiosity, not an accuracy lever.

## Part 2 — Sex: real structural female deficit, narrows (not reverses) in summer, weak 3yo-filly signal

### 2a. Raw win-rate delta (female − male/gelding), by age group

| Dataset                     | Era     | 2yo   | 3yo   | 4-6yo | 7yo+  |
| --------------------------- | ------- | ----- | ----- | ----- | ----- |
| summer venue × summer month | 2015-19 | +0.29 | -1.45 | -0.52 | -1.45 |
| summer venue × summer month | 2020-23 | -0.56 | +0.33 | -0.17 | -1.24 |
| summer venue × summer month | 2024-26 | -0.50 | -0.73 | +0.62 | n/a   |
| main venue × summer month   | 2015-19 | -0.24 | -1.55 | -0.67 | -1.73 |
| main venue × summer month   | 2020-23 | -1.41 | -0.25 | -0.66 | +0.68 |
| main venue × summer month   | 2024-26 | -1.84 | -2.38 | -0.91 | n/a   |
| summer venue × OTHER month  | 2015-19 | -1.94 | -1.91 | -2.53 | -0.56 |
| summer venue × OTHER month  | 2020-23 | -3.31 | -2.66 | -2.29 | -0.71 |
| summer venue × OTHER month  | 2024-26 | -1.28 | -2.49 | -2.16 | -1.05 |

The "夏は牝馬" lore is directionally supported **only as a relative narrowing**: females carry a consistent,
sizeable win-rate deficit everywhere (main venues, off-season, all ages), but that deficit is **smallest (or
briefly reversed) during summer months at summer venues** compared to the same venues' off-season months
(e.g. 3yo: summer -1.45/+0.33/-0.73 vs off-season -1.91/-2.66/-2.49 — off-season deficit is consistently
~1.5-2pp larger). There is no era where females outright dominate; summer is better described as "the female
deficit shrinks" not "females excel." **Verdict: PARTIAL law** (directionally consistent narrowing, but never
a clean reversal in any era).

### 2b. Odds-independence — mostly FULLY/PARTIALLY-PRICED, one weak PARTIAL exception for 3yo fillies

Band-conditional deltas for 3yo (summer venues/months) are noisy across bands and eras with no consistent
sign pattern (`angle2_result.json` → `band_table`). The controlling-logistic bootstrap for `is_female` (3yo,
summer, controlling log_odds+band):

| Era     | Coefficient (point) | LB95   | UB95   |
| ------- | ------------------- | ------ | ------ |
| 2015-19 | +0.059              | -0.045 | +0.159 |
| 2020-23 | +0.200              | +0.080 | +0.303 |
| 2024-26 | +0.140              | -0.014 | +0.256 |

Only 2020-23 clears the zero bound; 2015-19 and 2024-26 are directionally positive but not significant. 4-6yo
and 7yo+ groups never clear zero in any era. **Verdict: PARTIAL/emerging, not a clean LAW** — there may be a
genuine small, odds-underpriced summer edge for 3yo fillies specifically, growing since 2020, but it fails the
strict 3-era LAW bar. This is directly consistent with (does not contradict)
`[[project_season_sex_weight_probe_2026_06_20]]`'s prior REJECT of a serve-time 夏牝馬 rank-swap/bonus
(`tmp/layer2_summer_mare_sim.py`, all bootstrap CIs straddling 0) — the underlying effect is real but too
small and era-inconsistent to move model accuracy, matching that prior finding almost exactly.

**Serve-feasibility**: none — already tested as a model intervention and rejected 2026-06-20; this probe adds
the _descriptive_ confirmation that the reason it was rejected is a genuinely weak, era-inconsistent effect,
not a measurement artifact. DO-NOT-RETEST.

## Part 3 — Weight/handicap structure

### 3a. Weight relative to field mean — ODDS-FREE LAW, but already a deployed feature

Bucketing `futan_juryo - race_mean` into light(≤-1kg) / mid / heavy(≥+1kg):

| Dataset                     | Era     | light-vs-heavy Δwin(pp) |
| --------------------------- | ------- | ----------------------- |
| summer venue × summer month | 2015-19 | -0.73                   |
| summer venue × summer month | 2020-23 | +1.91                   |
| summer venue × summer month | 2024-26 | +2.05                   |
| main venue × summer month   | 2015-19 | -1.29                   |
| main venue × summer month   | 2020-23 | +2.33                   |
| main venue × summer month   | 2024-26 | +1.15                   |
| summer venue × OTHER month  | 2015-19 | -2.08                   |
| summer venue × OTHER month  | 2020-23 | -2.37                   |
| summer venue × OTHER month  | 2024-26 | -3.24                   |

The raw sign flips 2015-19→2020-23 at both summer and main venues (so **not summer-specific**, and NOT a
clean 3-era LAW at the "light wins" framing) — but decomposing by weight-assignment type explains the flip:

| juryo_type              | 2015-19 | 2020-23 | 2024-26 |
| ----------------------- | ------- | ------- | ------- |
| 馬齢 (barei_scale)      | -3.81   | -2.77   | -3.94   |
| 定量 (teiryo_fixed)     | +1.92   | +7.00   | +8.49   |
| ハンデ (handicap)       | -2.71   | -5.99   | +2.29   |
| 別定 (betsutei, thin n) | -4.43   | +4.80   | -2.31   |

馬齢 races show a **consistent LAW**: horses lighter than the field mean (mechanically the younger horses in
a mixed-age 馬齢 field) _underperform_ — a selection effect (a 3yo racing openly against proven older horses
under scale weight is disproportionately a still-unproven horse), not a weight-carrying effect per se. 定量
(fixed-weight) races show the opposite and _growing_ LAW: "light" here is mechanically female (2kg allowance)
or apprentice-ridden, so this largely re-expresses Part 2's sex effect and the (fully-priced) apprentice
effect, not an independent weight law. ハンデ races show **no consistent direction** (-2.71/-5.99/+2.29) —
which is actually reassuring: it means JRA's official handicapper is doing a roughly unbiased job (no
persistent exploitable edge from being assigned less weight than the field in a race explicitly designed to
equalize weight). Absolute ≤52kg-vs-rest is also NOISE (-0.83/+0.66/-1.68) — an absolute cutoff mixes horses
across very different field-weight contexts and washes out the field-relative signal.

**The one genuinely clean signal**: the continuous `weight_rel` (futan − race mean) coefficient in the
controlling logistic regression (`win ~ log_odds + band + weight_rel`), summer venues/months:

| Era     | Coefficient | LB95    | UB95    |
| ------- | ----------- | ------- | ------- |
| 2015-19 | -0.0295     | -0.0543 | -0.0017 |
| 2020-23 | -0.0648     | -0.0975 | -0.0342 |
| 2020-26 | -0.0808     | -0.1223 | -0.0356 |

Negative and excludes zero in **all 3 eras**, and the magnitude is growing (roughly tripling 2015-19→2024-26).
**Verdict: ODDS-FREE LAW** — carrying more weight than the field average predicts a lower win probability
beyond what the market already prices in, and this has strengthened over the decade. Band-conditional check
also shows this concentrated in favorites (band A: -0.50/+5.80/+7.35 growing; bands B/C flat near 0) — the
same favorites-only pattern as Part 1.

**Serve-feasibility**: **none — already deployed.** `tmp/models/jra-cb-v9-sim/metadata.json`'s 263-feature
list already includes `futan_juryo_diff_from_race_avg` and `futan_juryo_rank_in_race` (plus `weight_diff_from_
avg`, a body-weight analog). This probe is a clean validation that the deployed feature is tracking a real,
strengthening, odds-independent signal — not a new lever. One structural observation worth flagging (not
actionable, `[[project_jra_window_ablation_2026_06_24]]` already fixed the 2013+ training window as optimal
and DO-NOT-RETEST): since the effect is monotonically strengthening era-over-era, a model trained on the full
2013-2024 window necessarily averages over a period when the effect was weaker, so recent-year predictions
may be very slightly under-weighting this feature relative to its _current_ true strength — but this is a
sub-lever of window/recency-weighting, already a closed frontier.

### 3b. Apprentice (減量) jockeys — FULLY-PRICED, not summer-specific

Raw deltas (apprentice − regular win rate) are large, negative, and consistent everywhere, not just summer:

| Dataset                     | 2015-19 | 2020-23 | 2024-26 |
| --------------------------- | ------- | ------- | ------- |
| summer venue × summer month | -3.23   | -2.59   | -2.82   |
| main venue × summer month   | -3.95   | -4.07   | -4.12   |
| summer venue × OTHER month  | -0.41   | -0.90   | -1.72   |

If anything, the apprentice deficit is **smaller** at summer venues than main venues (and smaller still
off-season) — the opposite of an "apprentices thrive more in summer" story. Band-conditional deltas are noisy
with no consistent sign, and the controlling-logit coefficient for `is_apprentice` never clears zero in any
era (2015-19 point +0.042 CI[-0.114,+0.144]; 2020-23 +0.110 CI[-0.002,+0.218]; 2024-26 +0.113
CI[-0.083,+0.258]). **Verdict: FULLY-PRICED, NOISE-leaning, not a summer law** — the market already discounts
apprentice-ridden horses correctly; there is no residual signal after controlling for odds.

## Part 4 — Volunteered: 3yo×sex×week interaction (null) + a cross-angle meta-pattern

**3yo female-vs-male gap across summer week-thirds** (`angle4_interaction.py`): no consistent widening or
narrowing trend within any era (2015-19: -1.57/-1.45/-1.37pp early/mid/late; 2020-23: +0.37/-0.34/+1.18;
2024-26: -0.98/-0.73/+0.20) — flat-to-noisy. **No interaction effect; NULL result, DO-NOT-RETEST this
specific interaction.**

**Cross-angle meta-observation**: three _independent_ signals in this probe — the 3yo-favorite edge (Part
1c), the 3yo-filly summer edge (Part 2b), and the weight*rel coefficient (Part 3a) — all show the \_same*
temporal shape: weak/inconsistent in 2015-19, clearly emergent in 2020-23, and strongest in 2024-26. This
recurring "post-2020 strengthening" pattern across otherwise-unrelated variables suggests a common structural
driver around 2020 (COVID-era spectator-less racing, a scale-of-weight or programming change, or a genuine
shift in 3yo cohort quality/development) rather than three coincidental trends. Worth a dedicated probe if a
future task wants to chase the root cause, but out of scope here (no specific JRA rule-change data source
identified in the time available).

## まとめ (日本語)

- **3歳 vs 古馬**: 「夏を越えて3歳が力をつける」という通説は、**夏の中での週次推移としては再現しなかった**
  (2/3年代でむしろ横ばい〜微減、NOISE判定)。実際に存在するのは**通年カレンダー全体の法則**で、1-5月は
  3歳が劣勢(-0.1〜-0.7pp)、6月に逆転して年末まで一貫して拡大する(11-12月に+2.6〜+7.0pp、全3年代で同じ
  形、LAW判定)。夏はこの上昇局面の入り口に過ぎない。さらに人気帯で条件付けると、**人気馬同士(1-3番人気)
  に限れば3歳が古馬に勝る傾向は年間を通じてどのフェーズでも存在し**、oddsで統制しても消えない
  (ODDS-FREE、ただし1-3番人気に限定、4番人気以下ではFULLY-PRICED)。ただし既存の馬齢(barei)特徴量は
  654セル全REJECT確定済みのため、モデル改善レバーとしては不採用(DO-NOT-RETEST)。
- **性別**: 牝馬は年間を通じ恒常的に勝率が低いが、**夏開催場の夏季はその劣後幅が最小**(オフシーズンより
  1.5〜2pp縮小)——「夏は牝馬」通説は"逆転"ではなく"劣後の縮小"として部分的に支持される(PARTIAL)。
  3歳牝馬限定でoddsを統制すると2020-23年代のみ有意な残存効果(ODDS-FREE寄り)があるが3年代通しでは
  再現せず、2026-06-20の夏牝馬serve-swap REJECTと整合する弱い効果。
- **斤量**: 馬齢戦・定量戦・ハンデ戦で符号が逆転するため「軽ければ勝つ」という単純な話ではなく、
  ハンデ戦は符号不安定=ハンデ委員の裁定がほぼ偏りなく機能している証拠。唯一クリーンなのは
  「馬体重ではなく斤量のレース平均差」を連続変数としてoddsで統制した回帰係数で、全3年代で負・
  有意・年代を追うごとに拡大(ODDS-FREE LAW)——ただしこれは既に本番モデル
  (`futan_juryo_diff_from_race_avg`等)に搭載済みの特徴量であり、新規レバーではない。
  減量騎手(見習い)は夏・オフシーズン問わず一貫して勝率が低く、oddsで統制すると残存効果なし
  (FULLY-PRICED)——「夏は減量騎手が活躍する」という傾向は存在しない。
