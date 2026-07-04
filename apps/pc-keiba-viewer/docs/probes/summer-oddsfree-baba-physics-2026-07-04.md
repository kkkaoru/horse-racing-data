# Track/Going Physics — Odds-Free Summer Racing Laws (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA descriptive physics (turf type / meet-progression wear /
  class-adjusted time / rain recovery), odds-free at the race level, task #30.
- **Trigger**: user focus on odds-INDEPENDENT summer-racing tendencies —
  laws about the racing surface and its evolution, separate from the
  betting market. Race-level physics (times, pace, style outcomes) are
  inherently odds-free; the one sub-question implying a horse-level edge
  (does a style/turf-type advantage survive odds control?) gets an explicit
  odds-independence check (within-ninkijun-band persistence + partial
  correlation controlling for odds/`tansho_odds`).
- **Scope**: Sapporo(`01`)/Hakodate(`02`) — 洋芝 (cool-season turf) — vs
  Fukushima(`03`)/Kokura(`10`) — regional 野芝 (native turf) — plus
  Tokyo(`05`)/Nakayama(`06`) as a same-period major-venue contrast, 2015-2026,
  turf races only (`track_code in ('11','17','18','21')`). Era replication
  bands: 2015-19 / 2020-23 / 2024-26 throughout. All analysis is pure
  descriptive statistics + bootstrap CIs on actual outcomes — no model
  training.
- **Coordination**: extends, doesn't duplicate, today's 3-venue sweep
  (`jra-3venue-data-sweep-2026-07-04.md` — pace-shape×distance and within-day
  baba drift at 02/03/10, 2023-2026) by going back to 2015, adding Sapporo,
  and asking different questions (turf biology, meet-wear, class-adjusted
  time, recovery physics rather than pace-shape). Cross-checked against
  `jra-summer-venue-cell-focus-2026-07-04.md` and
  `jra-masked-lever-clean-retest-2026-07-04.md` (same-day bias, draw
  affinity/ablation — all REJECT, DO-NOT-RETEST) before proposing anything
  draw/waku-adjacent.

## Data quality finding (methodological, applies to any future work with `soha_time`)

`jvd_se.soha_time` is **not** raw tenths-of-a-second — it is JRA-VAN's packed
`M-SS-t` integer (one minute digit, two-digit seconds, one tenths digit),
e.g. raw `1080` = 1:08.0 = 68.0s. A naive `raw / 10.0` (used correctly
elsewhere in this codebase only for sub-2:00 comparisons that don't cross a
minute boundary) silently adds **+40s per whole minute** once the minutes
digit increments — invisible for 1200m/1800m/2600m turf times (which never
straddle a minute boundary in practice) but catastrophic for 2000m, whose
natural time distribution (116-135s) straddles the 2:00 mark almost exactly
in the middle. An early pass of this probe's own angle-1 script computed
naive `soha_time/10.0` and got a spurious bimodal ~157-160s/~200-208s split
at 2000m (stddev 14-22s) that looked like "yoshiba 2000m is 9 seconds
slower" — entirely an artifact of that parsing bug, not a real effect.
Fixed formula (`tmp/candidate-summer-baba-physics/common.py::soha_time_sec_sql`):
`M*60 + SS + t/10.0`. **This fix should be applied wherever `soha_time` is
converted to seconds outside this probe** — the existing 3-venue-sweep
scripts only ever used `zenhan_3f`/`kohan_3f` (already correctly stored as
raw tenths, no minutes digit) and never converted `soha_time` itself, so
they were not affected, but any future script that does must use this
formula.

## Angle 1 — 洋芝 (Sapporo/Hakodate) vs 野芝 (Fukushima/Kokura) physics

Method: `tmp/candidate-summer-baba-physics/angle1_turf_type.py`. Summer
window restricted to months 06-08 for BOTH groups (isolates turf-type from a
generic season confound — Fukushima/Kokura's non-summer meets excluded).
Common distances present at all 4 venues: 1200/1800/2000/2600m.

### Winning time (sec), yoshiba − noshiba, pooled 2015-2026

| kyori | delta (yoshiba−noshiba) | LB95/UB95     | era-replication                                     |
| ----- | ----------------------- | ------------- | --------------------------------------------------- |
| 1200  | +0.45s (n=709/628)      | +0.25 / +0.64 | PARTIAL (flips sign 2024-26: +0.65,+0.88,**−0.64**) |
| 1800  | +0.94s (n=423/381)      | +0.49 / +1.40 | PARTIAL (flips sign 2024-26: +1.42,+1.38,**−0.85**) |
| 2000  | **−6.44s** (n=353/278)  | −8.66 / −4.35 | PARTIAL, large and unstable (−7.36,−9.30,**+0.63**) |
| 2600  | +1.23s (n=151/65)       | +0.16 / +2.29 | **REPLICATES** (+1.04,+1.97,+0.29 — all positive)   |

**2000m caveat (important):** the −6.44s gap is large, well-powered, and
_survives class-stratification_ (checked within each `kyoso_joken_code`
bucket — yoshiba faster in 5/6 class cells, by 2.9-12.4s each, so it is not
a class-quality-mix artifact). But it does **not** era-replicate (flips sign
entirely in 2024-26) and shows up _only_ at 2000m — 1200/1800/2600m show
sub-2-second, mostly era-flipping deltas. A grass-biology effect (洋芝 vs 野芝)
has no obvious reason to matter enormously at exactly 2000m and negligibly
elsewhere; the much more likely explanation is that "2000m" is not a
comparable _course_ across these 4 venues even though `track_code` is
identical (`17` at all 4) — number of turns, start-to-first-turn distance,
and home-straight length differ by venue at a given nominal trip, and this
probe's raw extraction has no course-geometry field to control for it. **Do
not read this as a 洋芝/野芝 turf-type finding** — flagging it as a
venue×distance course-geometry artifact requiring dedicated course-layout
data (out of scope here), separate from the turf-type question this angle
was designed to answer.

### Agari (last-3f, `kohan_3f_race`) — the "洋芝=stamina, slower kick" lore

| kyori | yoshiba agari | noshiba agari | delta  | LB95/UB95         |
| ----- | ------------- | ------------- | ------ | ----------------- |
| 1200  | 35.31s        | 35.28s        | +0.03s | −0.05 / +0.10     |
| 1800  | 35.77s        | 35.88s        | −0.11s | −0.23 / **−0.01** |
| 2000  | 36.19s        | 36.13s        | +0.05s | −0.09 / +0.20     |
| 2600  | 36.40s        | 36.14s        | +0.26s | −0.02 / +0.54     |

All four deltas are under 0.3 seconds and only one (1800m, tiny and
negative — the _opposite_ direction from the lore) has an LB95 that clears
zero, barely. **Closing-kick speed is statistically indistinguishable
between 洋芝 and 野芝 at matched distances.** This is a clean, direct refutation
of the specific "洋芝 requires more stamina / blunts the sprint" claim, at
least as measured by raw last-3f time.

### Style win-share (front=逃げ/先行 vs closer=差し/追込)

| kyori | yoshiba front% | noshiba front% | delta      | LB95/UB95        |
| ----- | -------------- | -------------- | ---------- | ---------------- |
| 1200  | 69.8           | 73.6           | −3.8pp     | −8.6 / +1.5      |
| 1800  | 78.5           | 71.4           | **+7.1pp** | **+1.4** / +13.5 |
| 2000  | 70.5           | 70.1           | +0.4pp     | −6.9 / +7.6      |
| 2600  | 76.8           | 70.8           | +6.1pp     | −6.5 / +20.1     |

Pooled-by-era (all common kyori): 2015-19 delta −0.7pp, 2020-23 +1.5pp,
2024-26 +1.5pp → **PARTIAL** (sign flips once, magnitude small throughout).
Only the 1800m cell clears LB95>0, and it's one cell among four — weak,
inconsistent evidence for a turf-type style effect.

### Odds-independence check on the style effect

Horse-level partial correlation of `is_front` vs `is_win`, controlling for
`tansho_odds` (all finishers, not just winners):

| turf_type | n      | raw corr | partial corr (odds-controlled) |
| --------- | ------ | -------- | ------------------------------ |
| yoshiba   | 20,570 | +0.2154  | +0.1831                        |
| noshiba   | 18,092 | +0.2190  | +0.1796                        |

Within-ninkijun-band persistence (front-runner win rate minus closer win
rate, by favorite-rank band):

| ninki_band   | yoshiba (front−closer)  | noshiba (front−closer)  |
| ------------ | ----------------------- | ----------------------- |
| 1 (favorite) | 40.6% − 20.9% = +19.7pp | 38.0% − 18.9% = +19.1pp |
| 2-3          | 21.4% − 10.6% = +10.8pp | 21.7% − 9.9% = +11.8pp  |
| 4-6          | 11.3% − 3.6% = +7.7pp   | 10.8% − 4.4% = +6.4pp   |
| 7+           | 5.2% − 0.9% = +4.3pp    | 4.5% − 1.0% = +3.5pp    |

A real, well-known, odds-surviving front-runner advantage exists everywhere
(it doesn't vanish when controlling for odds, at any favorite-rank band) —
but it is **essentially identical in magnitude at yoshiba and noshiba**,
band by band. **Verdict: no turf-type-specific style edge, odds-independent
or otherwise.** Whatever front-runner advantage exists is a general JRA
phenomenon already reflected equally in both turf types' market prices.

**Angle 1 bottom line**: agari-lore REFUTED (no measurable difference);
style-win-share and style-edge-vs-odds: REJECT turf-type-specific effect
(flat/inconsistent); raw winning time: small (<1.3s), mostly non-replicating
effects at 1200/1800m, one clean-replicating small effect at 2600m
(+0.29 to +1.97s, yoshiba slower), and one large-but-untrustworthy 2000m
anomaly attributed to course geometry, not turf type.

## Angle 2 — meet-progression clock drift (course wear vs weather)

Method: `tmp/candidate-summer-baba-physics/angle2_meet_progression.py`. Time
index (`time_z`) = winner's time minus that **same venue's own**
(kyori × era) baseline mean, so cross-venue course-geometry differences
(the Angle-1 2000m problem) can't contaminate this — every comparison is
within-venue. All months (meet-day physics isn't summer-specific), 6 venues
(4 regional + Tokyo/Nakayama contrast).

### Late-meet slowdown (day 6+ minus day 1-2, `time_z` sec), firm-baba-only (`baba_code='1'`, isolates course wear from weather), per era

| venue               | 2015-19 | 2020-23 | 2024-26 | verdict                                                                      |
| ------------------- | ------- | ------- | ------- | ---------------------------------------------------------------------------- |
| 01 Sapporo (洋芝)   | +1.37   | −0.34   | −0.64   | PARTIAL                                                                      |
| 02 Hakodate (洋芝)  | +1.19   | +1.51   | +1.83   | **REPLICATES** — track slows down as the meet progresses                     |
| 03 Fukushima (野芝) | −0.01   | +1.18   | −0.46   | PARTIAL                                                                      |
| 05 Tokyo            | −0.64   | +0.37   | −1.30   | PARTIAL                                                                      |
| 06 Nakayama         | −0.33   | +0.65   | +0.74   | PARTIAL                                                                      |
| 10 Kokura (野芝)    | −0.91   | −1.70   | −0.24   | **REPLICATES** — track speeds up as the meet progresses (opposite direction) |

Two of six venues show a clean, all-3-eras-same-sign effect, and they go in
**opposite directions**: Hakodate genuinely wears down/slows over its meet
(consistent with 洋芝's known poor wear-recovery under repeated racing — this
is the one place in this probe where the 洋芝 stamina-course lore shows real,
replicating support, even though it's Hakodate-specific, not shared by
Sapporo), while Kokura gets _faster_ as its meet progresses (plausibly
track-management/rolling effects consolidating the surface, or a
program-structure confound — better-class races scheduled later in Kokura's
meet — not distinguished here). Sapporo (the other 洋芝 venue) does **not**
replicate the Hakodate pattern, so this is not a clean universal "洋芝 wears
down" law — it's venue-specific, 2 of 6 venues, opposite-signed.

### Does meet-day shift style/waku outcomes? (horse-level, odds-controlled)

| nichime_band    | front-runner partial corr (odds-controlled) | waku partial corr (odds-controlled) |
| --------------- | ------------------------------------------- | ----------------------------------- |
| d1-2 (n=35,550) | +0.1544                                     | −0.0146                             |
| d3-5 (n=54,270) | +0.1449                                     | +0.0014                             |
| d6+ (n=56,145)  | +0.1426                                     | +0.0010                             |

Both are essentially flat across the meet (front-runner edge drifts by
<0.02 in partial-corr units; waku edge stays within ±0.015 of zero
throughout, consistent with the already-REJECTED `draw_ablation`/
`sameday_bias` findings). **Verdict: REJECT** — meet-day progression does
not meaningfully move either the style or the draw edge, despite the two
venues' real time-index drift above. The two venue-specific wear effects,
where they exist, don't propagate into a detectable outcome-structure shift
at the sample sizes available.

## Angle 3 — 時計傾向×class: does the summer program run slower/faster than the same class elsewhere?

Method: `tmp/candidate-summer-baba-physics/angle3_class_time.py`. Compares
winning time, **holding `kyoso_joken_code` (class condition) fixed**,
between the 4 summer/regional venues in their own summer months (06-08) and
Tokyo/Nakayama in any month (Tokyo/Nakayama don't race in June-August at
all, confirmed by the month-distribution check — so this is necessarily an
across-period comparison, unlike Angle 1's same-period design).

### Era-replication (all classes pooled per kyori — coarser cut, see caveat)

| kyori | 2015-19                                    | 2020-23 | 2024-26    | verdict                                                            |
| ----- | ------------------------------------------ | ------- | ---------- | ------------------------------------------------------------------ |
| 1200  | +0.93s                                     | +0.45s  | +1.70s     | **REPLICATES** — summer venues slower, all 3 eras                  |
| 1800  | +0.55s                                     | +1.41s  | +2.17s     | **REPLICATES** — summer venues slower, all 3 eras                  |
| 2000  | +0.59s                                     | +0.18s  | **−4.05s** | PARTIAL/untrustworthy — same 2000m course-geometry flag as Angle 1 |
| 2600  | n/a (Tokyo/Nakayama don't race 2600m turf) | —       | —          | NO_DATA                                                            |

### Class-controlled cross-check (pooled 2015-2026, per kyori × class cell)

At 1200m and 1800m, the "summer venue slower" direction holds in every
class bucket tested (1-win/2-win/3-win/maiden/special-graded), e.g. 1200m
special/graded +0.96s [LB95 +0.50], 1800m maiden-mid +0.85s [LB95 +0.44] —
so the era-replicating pooled result above is not purely a class-mix
artifact, though the specific **widening-over-time** shape (roughly +0.5-0.9s
in 2015-19 growing to +1.7-2.2s in 2024-26) is read off the class-POOLED
per-era cut, not a class-held-fixed per-era cut (too few races per
class×venue×era cell to do that split reliably) — treat the widening-trend
shape as suggestive, not confirmed class-controlled.

**Interpretation**: at the same official class label, a horse racing at
Kokura/Fukushima/Sapporo/Hakodate in summer runs to a systematically slower
standard time than the same class at Tokyo/Nakayama (small, ~0.5-2s, but
real and replicating at 1200/1800m specifically) — most plausibly a
within-class quality-tier gap (regional/minor-venue conditions horses being
weaker than major-venue conditions horses of the nominally same class label)
rather than a going/course effect, since Angle 1 already showed these same
venues' raw going/agari physics are close to indistinguishable from
Tokyo/Nakayama-tier tracks. **Serve-feasibility**: `keibajo_code` and
`kyoso_joken_code` are both already existing pre-race categorical features
in the deployed model — this is exactly the "ingredients already exist,
should be tree-discoverable via a `venue×class` split" pattern flagged
elsewhere in today's sweep (`jra-3venue-data-sweep-2026-07-04.md` #1,
`jra-summer-venue-cell-focus-2026-07-04.md` Part 3) that has repeatedly
turned out to be already-saturated when tested (draw_ablation,
sameday_bias, straight×closer — all REJECT per
`jra-masked-lever-clean-retest-2026-07-04.md`). Not recommending a fresh WF
test without a specific reason to think this particular interaction is
under-split by the existing depth-8 trees.

## Angle 4 — rain/moisture recovery physics (within-meet 稍重→良 transitions)

Method: `tmp/candidate-summer-baba-physics/angle4_rain_recovery.py`.
Day-level modal `babajotai_code_shiba` per (venue, kaisai_kai, nichime),
day-over-day transition analysis, 4 regional venues.

### Recovery speed (day-over-day, degraded-start days only, pooled 2015-2026)

| venue        | n degraded-start days | next-day improves | stays same | worsens further |
| ------------ | --------------------- | ----------------- | ---------- | --------------- |
| 01 Sapporo   | 28                    | 71.4%             | 25.0%      | 3.6%            |
| 02 Hakodate  | 28                    | 75.0%             | 14.3%      | 10.7%           |
| 03 Fukushima | 31                    | 71.0%             | 22.6%      | 6.5%            |
| 10 Kokura    | 64                    | 67.2%             | 20.3%      | 12.5%           |

`P(full recovery to code=1 next day)` per era is noisy at this n (7-35 per
venue×era cell) and shows **no consistent venue ranking**: Sapporo
41.7%→66.7%, Hakodate 58.3%→66.7%, Fukushima 58.3%→58.8% (flat), Kokura
53.8%→42.9%→50.0% (no trend). **Verdict: NOISE — the "drainage physics
differ per track" hypothesis is not supported**; all 4 venues recover from
a degraded going at a statistically indistinguishable ~67-75% next-day rate,
and the era breakdown shows no venue holds a stable rank across eras.

### Outcome structure during recovery vs stable conditions

Pooled front-win% by day-level recovery state (all 4 venues):

| state                                                   | front-win% | n     |
| ------------------------------------------------------- | ---------- | ----- |
| stable_good (良 for ≥2 consecutive days)                | 69.8%      | 2,867 |
| just_recovered (returned to 良 today)                   | 70.4%      | 564   |
| recovering_not_done (improving but still worse than 良) | 69.9%      | 166   |
| stable_bad (same degraded code ≥2 days)                 | 69.8%      | 212   |
| worsening (getting worse day-over-day)                  | 69.6%      | 713   |

A spread of under 1 percentage point across all five trajectory states.
**Verdict: REJECT** — the _direction of change_ in track condition (actively
drying out vs actively worsening vs stable) carries no outcome-structure
signal beyond whatever the _current_ `baba_code` level already captures.
This is a clean confirmation, via a genuinely new angle (trajectory, not
level), of the standing conclusion from the masked-lever campaign that
CatBoost's existing baba/track features already capture what there is to
capture from track-condition information.

## Summary for prioritization

| #   | Hypothesis                                              | Grade                                              | Odds-independence                                                | Serve-feasible                                                         | DO-NOT-RETEST overlap                                         | Recommendation                                                                                                                        |
| --- | ------------------------------------------------------- | -------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 1a  | 洋芝 vs 野芝 winning time (1200/1800m)                  | PARTIAL, sign flips 2024-26                        | n/a (race-level)                                                 | yes, trivial (`keibajo_code`)                                          | none new                                                      | do not pursue — not stable                                                                                                            |
| 1a' | 洋芝 vs 野芝 at 2000m                                   | large but PARTIAL, likely course-geometry artifact | n/a                                                              | —                                                                      | none new                                                      | **flag as data caveat, not a turf-type finding**; exclude 2000m from any future yoshiba/noshiba time claim without course-layout data |
| 1b  | 洋芝 vs 野芝 agari (stamina lore)                       | REFUTED (no diff)                                  | n/a                                                              | n/a                                                                    | none new                                                      | lore refuted, log and close                                                                                                           |
| 1c  | 洋芝 vs 野芝 style win-share                            | PARTIAL, weak                                      | REJECT (odds-independent check shows no turf-type-specific edge) | n/a                                                                    | overlaps style-rivalry (task #16, REJECTED)                   | close, no new lever                                                                                                                   |
| 2a  | Hakodate late-meet slowdown                             | **REPLICATES** (venue-specific)                    | n/a (race-level)                                                 | yes (`keibajo_code`×`kaisai_nichime`, both pre-race known)             | adjacent to meetingday-waku (REJECTED)                        | descriptive only; too small/venue-specific to WF-test on its own                                                                      |
| 2b  | Kokura late-meet speedup                                | **REPLICATES** (opposite direction)                | n/a                                                              | yes                                                                    | same as above                                                 | same — mechanism undetermined (track mgmt vs program confound)                                                                        |
| 2c  | Meet-day shift in style/waku edge                       | REJECT (flat)                                      | REJECT (odds-controlled partial corr flat across meet-day)       | —                                                                      | confirms `jra-meet-momentum-2026-07-04.md` REJECT             | closed                                                                                                                                |
| 3   | Summer-venue vs major-venue class-time gap (1200/1800m) | **REPLICATES**, small (0.5-2s)                     | n/a (race-level, program-structure)                              | yes, but ingredients already exist (`keibajo_code`×`kyoso_joken_code`) | pattern matches saturated draw/pace levers (masked-lever doc) | descriptive finding; not recommending fresh WF test                                                                                   |
| 3'  | Same, at 2000m                                          | untrustworthy                                      | —                                                                | —                                                                      | —                                                             | excluded, same course-geometry flag as 1a'                                                                                            |
| 4a  | Recovery speed differs by venue (drainage)              | REJECT (noise, no consistent ranking)              | n/a                                                              | —                                                                      | none new                                                      | closed                                                                                                                                |
| 4b  | Outcome shift during recovery trajectory                | REJECT (front-win% flat <1pp across all states)    | REJECT implicitly (no signal to even test against odds)          | —                                                                      | confirms masked-lever baba findings                           | closed                                                                                                                                |

**Bottom line**: of the four angles, the two cleanest, most-replicating,
genuinely new findings are (i) the agari/stamina-lore refutation (1b — a
clean negative result worth documenting so it isn't re-investigated), and
(ii) the two-venue-specific, opposite-signed meet-progression drift at
Hakodate/Kokura (2a/2b — real but too small/venue-idiosyncratic to act on).
The class-adjusted summer-vs-major time gap (3) is the most
actionable-looking descriptive law, but its ingredients (`keibajo_code`,
`kyoso_joken_code`) are already in the model, and the extensive campaign
today (`jra-masked-lever-clean-retest-2026-07-04.md`,
`jra-summer-venue-cell-focus-2026-07-04.md` Part 3) found repeatedly that
this "ingredients exist, should be tree-discoverable" reasoning fails when
actually WF-tested — so it is reported as a descriptive law, not proposed
as a new lever. The 2000m distance is flagged as unreliable for any
future cross-venue time comparison at these 4 venues without dedicated
course-layout (turn-count/straight-length) data. Everything else (turf-type
style effects, meet-day style/waku shift, rain-recovery venue differences
and outcome-structure shift) is a clean REJECT/NOISE verdict, consistent
with and reinforcing the broader campaign's finding that CatBoost's
existing track/venue/class/draw feature set already captures what these raw
descriptive angles can see.

## 日本語要約

- **soha_time のパース bug を発見・修正**: `M-SS-t` 形式(分1桁+秒2桁+コンマ1桁)を
  単純に `/10` すると分の桁が繰り上がるたびに+40秒ずれる。2000mの走破タイムが
  ちょうど2分の境界をまたぐため、この bug が「洋芝は2000mで9秒遅い」という
  見せかけの結果を生んでいた。修正後、2000m の差は依然大きい(class 補正後も
  残る)が、年代別に符号反転し、他の距離(1200/1800/2600m)では見られない
  極端さのため、芝質ではなくコース形状(周回・直線長)由来の可能性が高いと
  結論。今後 soha_time を秒に変換する際は必ず修正式を使うこと。
- **洋芝(札幌/函館) vs 野芝(福島/小倉)物理**: 上がり3ハロン(agari)はほぼ完全に
  同じ(差<0.3秒、ほぼ有意差なし)→「洋芝はスタミナ寄りで上がりが遅い」という
  俗説は否定。勝ち時計は1200/1800mで年代により符号反転(不安定)、2600mのみ
  綺麗に再現(洋芝がわずかに遅い、+0.3〜+2.0秒)。脚質別勝率シェアも弱く不安定。
  逃げ・先行の"オッズ非依存"優位性はodds制御後も存在するが、洋芝でも野芝でも
  ほぼ同じ大きさ→芝質特有の脚質エッジは無し。
  - **参考(パース bug関連)**: [[project_target_corner_leak_2026_07_04]] と同様、
    「見かけの大きな効果が実はデータ処理由来」というパターン。
- **開催進行(馬場のヘタリ)**: 函館は開催が進むほど時計が遅くなる傾向が3年代
  すべてで再現(洋芝特有の摩耗ロア支持)。一方、小倉は逆に開催後半ほど時計が
  速くなる傾向が再現(メカニズム不明、コース整備 or 番組編成の交絡の可能性)。
  ただし脚質・枠のオッズ制御済み優位性は開催日数が進んでも全くフラット
  (前開催日 vs 後開催日で partial correlation の変化 <0.02)→ 既存の
  meet-momentum(REJECT済)/meetingday-waku(REJECT済)の結論を再確認。
- **夏開催 vs 主場の同クラス時計比較**: 同じ条件クラスで比較しても、
  夏季地方開催(札幌/函館/福島/小倉)はTokyo/中山より1200/1800mで
  一貫して(3年代とも)0.5〜2秋遅い → 番組構造・馬質の地域差を示唆する
  descriptive な発見。ただし `keibajo_code`×`kyoso_joken_code` は既存
  feature であり、同種の「既存featureで木が学習できるはず」という推論は
  本日の他の lever(draw_ablation, sameday_bias 等)ですべて REJECT
  だったため、新規 WF test は推奨しない。
- **雨後回復(稍重→良)物理**: 4場とも翌日改善率67〜75%で統計的に区別不能
  (「排水性が場によって違う」仮説は不支持=NOISE)。回復途中/悪化中/安定
  いずれの状態でも逃げ・先行勝率は69.6〜70.4%とほぼ完全にフラット →
  馬場状態の"軌道"(良くなっているか悪くなっているか)は現在の baba_code
  水準以上の情報を持たない、という明確な REJECT。

## Artifacts

- `tmp/candidate-summer-baba-physics/build_analysis_table.py` — extraction
  (PG → parquet, read-only), 301,406 horse-rows / 21,292 races, 6 venues
  (01/02/03/05/06/10), 2015-2026.
- `tmp/candidate-summer-baba-physics/analysis_table.parquet`
- `tmp/candidate-summer-baba-physics/common.py` — shared bootstrap CI /
  partial-correlation / era-replication-verdict helpers, and the corrected
  `soha_time_sec_sql()` parser (see data-quality finding above).
- `tmp/candidate-summer-baba-physics/angle1_turf_type.py` —
  洋芝/野芝 time/agari/style/odds-independence.
- `tmp/candidate-summer-baba-physics/angle2_meet_progression.py` —
  meet-day time drift + style/waku odds-controlled check.
- `tmp/candidate-summer-baba-physics/angle3_class_time.py` — class-held-fixed
  summer-vs-major time comparison.
- `tmp/candidate-summer-baba-physics/angle4_rain_recovery.py` — day-level
  baba transition + outcome-structure check.
- Not investigated further (out of scope for this pass): course-geometry
  data (turn count, straight length per venue×distance) that would let the
  2000m anomaly be properly attributed; a class-held-fixed version of
  Angle 3's era-replication (blocked on small per-cell n).
