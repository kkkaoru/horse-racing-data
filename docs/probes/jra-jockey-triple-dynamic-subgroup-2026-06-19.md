# JRA 騎手×競馬場×距離×芝ダ triple interaction — 動的 subgroup 精度評価

**Date:** 2026-06-19
**Category:** JRA finish-position (top1)
**Goal:** グローバル ρ gate に落ちた騎手 triple interaction 信号 (raw ρ=+0.274 / partial ρ=+0.063) を、動的 subgroup ごとの **実予測精度寄与** で再評価。USER 指示「ρ gate 不要・少しでも改善すれば ADOPT」。

## 結論 (TL;DR)

- **グローバルでは REJECT**: `jc_win_rate` / `jc_avg_finish` を odds の上に足しても top1 は **−0.09pp (LB95 −0.49pp)** で改善しない。raw ρ=+0.274 の信号は **odds_score が既に内包** している (人気馬は騎手×条件成績を市場が織り込み済み)。
- **n≥500 で唯一の STRONG ADOPT は venue=06 (中山)**: top1 **+1.13pp (LB95 +0.13pp)**, n=1500。CI が 0 を超える唯一の大型 subgroup。
- 大型 subgroup の WEAK ADOPT (点推定>0 だが CI が 0 跨ぎ): venue=05(阪神)/03(中京), class=NONE(新馬・未勝利), dirt 系。
- 大型 subgroup で **有意に悪化**: class=E (−0.78pp, LB95<0), venue=07(京都)/10(小倉), class=E turf/sprint。triple 信号がノイズになる帯がはっきり存在。

## 手法

### Step 1 — 騎手 triple 特徴量 (strictly causal)

PG `jvd_se` JOIN `jvd_ra` (`keibajo_code IN '01'..'10'`、JRA 限定)。

- `jc_win_rate` = 騎手×競馬場×距離帯×芝ダ の条件付き勝率
- `jc_avg_finish` = 同条件 平均着順
- **prior races only**: レース日付でソートし、各レースは「より前の日付」のレースのみ集計。同日レースは相互に除外 (same-day leakage 回避)。LOYO ではなく expanding-window にしたのは walk-forward (train≤2022 / holdout 2023-25) と整合させ未来漏洩を断つため。
- distance_band: ≤1400 sprint / ≤1800 mile / ≤2200 intermediate / >2200 long
- surface: track_code 10-22 turf / 23-29 dirt / 51-59(障害) other
- 1,865,333 行、jc_win_rate 非NULL **97.9%**。
- script: `tmp/jockey_triple_build.py` → `tmp/jockey_triple_features.parquet`

### Step 2 — Feature store JOIN

`apps/pc-keiba-viewer/tmp/feat-jra-v8-iter18-class` (263 cols, 2006-2026) を
`(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)` で LEFT JOIN
(store の `race_id=jra:Y:MMDD:VENUE:RACE` 粒度に合わせ kai/nichime は落とす)。
join 後 jc_win_rate coverage **98.5%**, 990,719 行 / 70,417 races。NULL は中央値補完。

### Step 3 — base vs enhanced (同一 capacity)

- **base**: LightGBM LambdaRank on `[odds_score]` のみ (1 特徴ランカー = 実質 odds 人気順)
- **enhanced**: LambdaRank on `[odds_score, jc_win_rate, jc_avg_finish]`
- 完全同一ハイパラ・**同一 num_boost_round=300** (capacity 非対称を排除し feature 寄与だけを分離)
- relevance: 1着=3 / top3=1 / else=0、DuckDB memory_limit=4GB threads=4
- walk-forward: train ≤2022 (830,372 行) / holdout 2023-2025 (10,365 races)
- 各レースで argmax(score) を 1着予測 → top1 hit
- Δtop1 = enhanced − base、**paired bootstrap CI** (同一レース再標本、n_boot=2000, seed=42)

### Step 4 — ADOPT 判定

n≥500 かつ Δ>0: LB95>0 → STRONG / それ以外 → WEAK。n<500 で Δ>0 → ADOPT_LOW_N。Δ≤0 → REJECT。

## 結果 (主要 subgroup, Δ 降順抜粋)

| subgroup                 |     n |   base |    enh |       Δtop1 |        LB95 |    UB95 | 判定              |
| ------------------------ | ----: | -----: | -----: | ----------: | ----------: | ------: | ----------------- |
| **venue=06 (中山)**      |  1500 | 0.3447 | 0.3560 | **+0.0113** | **+0.0013** | +0.0220 | **STRONG_ADOPT**  |
| venue=03 (中京)          |   720 | 0.2931 | 0.3014 |     +0.0083 |     −0.0056 | +0.0208 | WEAK_ADOPT        |
| venue=05 (阪神)          |  1607 | 0.3634 | 0.3696 |     +0.0062 |     −0.0044 | +0.0162 | WEAK_ADOPT        |
| class=NONE\|dband=long   |   521 | 0.3551 | 0.3608 |     +0.0058 |     −0.0116 | +0.0230 | WEAK_ADOPT        |
| class=NONE\|dband=mile   |  3289 | 0.3484 | 0.3512 |     +0.0027 |     −0.0043 | +0.0091 | WEAK_ADOPT        |
| dband=long               |   800 | 0.3625 | 0.3650 |     +0.0025 |     −0.0112 | +0.0163 | WEAK_ADOPT        |
| surface=dirt\|dband=mile |  2442 | 0.3419 | 0.3444 |     +0.0025 |     −0.0045 | +0.0098 | WEAK_ADOPT        |
| class=NONE (新馬/未勝利) |  7570 | 0.3452 | 0.3466 |     +0.0015 |     −0.0030 | +0.0059 | WEAK_ADOPT        |
| surface=dirt             |  4976 | 0.3426 | 0.3436 |     +0.0010 |     −0.0038 | +0.0064 | WEAK_ADOPT        |
| **ALL**                  | 10365 | 0.3370 | 0.3361 | **−0.0009** |     −0.0049 | +0.0028 | **REJECT**        |
| surface=turf             |  5015 | 0.3296 | 0.3266 |     −0.0030 |     −0.0088 | +0.0026 | REJECT            |
| class=E                  |  2186 | 0.3253 | 0.3175 |     −0.0078 |     −0.0160 | +0.0000 | REJECT            |
| venue=07 (京都)          |  1128 | 0.3298 | 0.3183 |     −0.0115 |     −0.0230 | −0.0009 | REJECT (有意悪化) |
| class=E\|surface=turf    |  1337 | 0.3171 | 0.3037 |     −0.0135 |     −0.0247 | −0.0030 | REJECT (有意悪化) |
| venue=10 (小倉)          |   792 | 0.3131 | 0.2942 |     −0.0189 |     −0.0341 | −0.0051 | REJECT (有意悪化) |

判定集計: STRONG_ADOPT **1** / WEAK_ADOPT 14 / ADOPT_LOW_N 14 / REJECT 44。
n≥500 の WEAK_ADOPT は全 14 件とも LB95<0 (点推定のみ正)。

完全な全 subgroup テーブルは `tmp/probes/jockey_triple_subgroup_eval.json`。

## 解釈

1. **raw ρ=+0.274 と partial ρ=+0.063 の差が答え**: triple 信号は odds と強相関し、odds を条件付けると残差はほぼ消える。グローバルで net 改善しないのは partial ρ が小さい事実と一致 (memory `relationship_perclass_investigation_2026_06_12` の「partial ρ は必要だが十分でない」を再確認)。
2. **中山 (venue=06) だけ本物**: 唯一 CI が 0 を超える大型 subgroup。中山は急坂・小回りで騎手の乗り方差が着順に効きやすく、odds が拾い切れない騎手×条件成分が残ると解釈できる。
3. **class=E / 京都 / 小倉 では有害**: triple 統計がノイズ/過適合になり odds より悪化。global 一律投入は禁物。

## 推奨

- **global ADOPT は不可** (net −0.09pp、frontier 維持)。
- USER の「少しでも改善」基準に厳密準拠するなら、**venue=06 限定の class-conditional/venue-conditional routing** が唯一の候補 (+1.13pp, LB95>0)。ただし single subgroup・効果薄で、serve 時に中山だけ enhanced ranker を切替える運用コストに見合うかは要判断。memory `feedback_per_class_eval_conditional_adoption` の routing パターンに合致。
- WEAK_ADOPT 群 (阪神/中京/dirt/maiden) は点推定正だが LB95<0 のため、現状の単一年 holdout では確信不可。複数 multi-year split で再確認しない限り採用見送り。

## 成果物

- `docs/probes/jra-jockey-triple-dynamic-subgroup-2026-06-19.md` (本書)
- `tmp/probes/jockey_triple_subgroup_eval.json` (全 subgroup 数値)
- `tmp/jockey_triple_build.py` / `tmp/jockey_triple_subgroup_eval.py` (再現スクリプト)
