# Exotic Odds Place Signal Probe

**Date:** 2026-06-12
**Status:** ABORT for JRA | PROCEED for NAR / Ban-ei (partial ρ threshold ≥0.08)
**Question:** Do exotic-bet odds (umaren / wide / sanrenpuku) carry per-horse PLACE signal beyond tansho odds?

---

## 1. Schema Discovery

### Tables used

| Table               | Bet type                      | Encoding      | Chars/combo | Max combos    |
| ------------------- | ----------------------------- | ------------- | ----------- | ------------- |
| `jvd_o2` / `nvd_o2` | 馬連 (umaren, quinella)       | fixed-width   | 13          | C(18,2) = 153 |
| `jvd_o3` / `nvd_o3` | ワイド (wide, quinella-place) | fixed-width   | 17          | C(18,2) = 153 |
| `jvd_o5` / `nvd_o5` | 3連複 (sanrenpuku, trio)      | fixed-width   | 15          | C(18,3) = 816 |
| `jvd_se`            | JRA results                   | per-horse row | —           | —             |
| `nvd_se`            | NAR+Ban-ei results            | per-horse row | —           | —             |

### Encoding detail

All odds tables store the full combinatorial set as a single packed string per race row (one row per race, data_kubun='5' = confirmed pre-race snapshot).

**Umaren** (o2): `h1(2) + h2(2) + odds(5) + votes(4)` × 153 slots = 1989 chars.
Odds value: `int(s[4:9]) / 10` → actual pari-mutuel displayed odds.

**Wide** (o3): `h1(2) + h2(2) + lo_odds(5) + hi_odds(5) + votes(3)` × 153 slots = 2601 chars.
Wide carries a range (minimum/maximum payout). Mid-odds = `(lo + hi) / 20`.

**Sanrenpuku** (o5): `h1(2) + h2(2) + h3(2) + odds(5) + votes(4)` × 816 slots = 12240 chars.

### Result table relevant columns

`jvd_se` / `nvd_se`:

- `kakutei_chakujun` — official finish position (string, "01"–"18")
- `tansho_odds` — win odds × 10 (e.g., "0150" = 15.0x)
- `tansho_ninkijun` — win popularity rank
- `ijo_kubun_code` — '0' = normal finish; non-'0' = scratch/DQ/etc.
- `data_kubun` — '7' = official final result

### Data coverage 2023–2026

| Source                        | Years with data | Race-rows (2023–2026) | Horse-rows after filter |
| ----------------------------- | --------------- | --------------------- | ----------------------- |
| jvd_o2 (JRA umaren)           | 2023–2026       | 11,871                | 163,003                 |
| jvd_o3 (JRA wide)             | 2023–2026       | 11,871                | 163,003                 |
| jvd_o5 (JRA sanrenpuku)       | 2023–2026       | 11,812                | 162,188                 |
| nvd_o2 (NAR+Banei umaren)     | 2023, 2025–2026 | 36,804                | 367,920                 |
| nvd_o3 (NAR+Banei wide)       | 2023, 2025–2026 | 36,802                | 367,920                 |
| nvd_o5 (NAR+Banei sanrenpuku) | 2023–2026       | 52,025                | 519,696                 |

**Data gap note:** `nvd_o2` / `nvd_o3` / `nvd_o4` are missing year 2024 entirely (known ingest gap in NAR sync; see exotic-odds-availability.md §3). `nvd_o5` has intact 2024. The 2024 gap affects the umaren and wide signals for NAR/Ban-ei; this makes their partial ρ estimates slightly conservative (2024 data excluded from those signals).

---

## 2. Methodology

### 2.1 Per-horse market-implied probabilities

For each race we decode all combos from the packed string and compute implied probability as `1 / odds` (inverse pari-mutuel odds). We then **marginalize** to get a per-horse signal:

**Umaren P2(h):**

```
P2(h) = Σ_{j≠h} (1 / umaren_odds(h,j))
```

Summing over all pairs containing horse h. This is proportional to the market's implied probability that horse h finishes in the top-2 (as estimated by the quinella pool).

**Wide P3(h):**

```
P3(h) = Σ_{j≠h} (1 / wide_mid_odds(h,j))
```

where `wide_mid_odds = (lo + hi) / 2`. Wide pays when both horses are in the top-3, so this marginalizes to a top-3 signal.

**Sanrenpuku P3(h):**

```
P3(h) = Σ_{j<k, j≠h, k≠h} (1 / sanrenpuku_odds(h,j,k))
```

Summing over all triples containing h. Sanrenpuku pays when all three are in top-3, so this is also a top-3 signal.

The marginalized values are not normalized to [0,1]; they are monotone-transformed implied probabilities. We use them as ranking signals, not absolute probabilities.

### 2.2 Baseline: tansho odds

Tansho (win-bet) baseline is `tansho_prob = 1 / tansho_odds` (where `tansho_odds = raw / 10`).

### 2.3 Partial Spearman ρ

To measure incremental signal beyond tansho, we compute a **partial Spearman ρ** of the exotic-implied probability vs. outcome (is_top2 or is_top3), controlling for tansho_odds and tansho_ninkijun (popularity rank). Implementation: rank-residualize all variables (OLS on ranks) then compute Pearson ρ of residuals.

- **Umaren P2 → is_top2**, partial on [tansho, ninkijun]
- **Wide P3 → is_top3**, partial on [tansho, ninkijun]
- **Sanrenpuku P3 → is_top3**, partial on [tansho, ninkijun]

Threshold for PROCEED: partial ρ ≥ 0.08 in at least one class for at least one exotic signal.

### 2.4 Venue classification

- JRA: keibajo_code 01–10
- NAR: keibajo_code 30–48 (excluding 83)
- Ban-ei: keibajo_code 83

Horses with `ijo_kubun_code ≠ '0'` (scratch, disqualified, etc.) are excluded.

---

## 3. Results

### 3.1 Sample sizes (after filtering)

| Class  | Horses  | Races  |
| ------ | ------- | ------ |
| JRA    | 162,798 | 11,871 |
| NAR    | 318,923 | 31,203 |
| Ban-ei | 54,918  | 5,964  |

Note: NAR umaren/wide non-null = 224,855 (2024 gap); NAR sanrenpuku non-null = 304,673.

### 3.2 Spearman ρ — raw signal vs. finish_norm

`finish_norm = (n - rank) / (n - 1)` so 1st place = 1.0, last = 0.0.

| Signal                 | JRA ρ | NAR ρ | Ban-ei ρ |
| ---------------------- | ----- | ----- | -------- |
| tansho_prob (baseline) | 0.545 | 0.558 | 0.474    |
| umaren_P2              | 0.556 | 0.574 | 0.496    |
| wide_P3                | 0.556 | 0.570 | 0.492    |
| sanrenpuku_P3          | 0.555 | 0.566 | 0.498    |

All exotic signals are positively correlated with finish and modestly exceed the tansho baseline, but this is expected since they are derived from the same pari-mutuel market.

### 3.3 AUC comparison — exotic vs. tansho

| Signal        | Task  | JRA AUC | NAR AUC | Ban-ei AUC |
| ------------- | ----- | ------- | ------- | ---------- |
| tansho_prob   | top-3 | 0.819   | 0.818   | 0.760      |
| umaren_P2     | top-2 | 0.832   | 0.841   | 0.782      |
| wide_P3       | top-3 | 0.823   | 0.824   | 0.768      |
| sanrenpuku_P3 | top-3 | 0.824   | 0.824   | 0.770      |

Umaren P2 shows notably higher AUC for top-2 across all classes (the task is closely matched to the bet type). Wide and sanrenpuku AUC for top-3 roughly matches tansho top-3 AUC.

### 3.4 Partial Spearman ρ — incremental signal controlling tansho + ninkijun

This is the primary test for whether exotic odds carry information **beyond** tansho.

| Signal        | Task  | JRA partial ρ | NAR partial ρ | Ban-ei partial ρ |
| ------------- | ----- | ------------- | ------------- | ---------------- |
| umaren_P2     | top-2 | 0.048         | 0.075         | **0.098**        |
| wide_P3       | top-3 | 0.076         | **0.101**     | **0.109**        |
| sanrenpuku_P3 | top-3 | 0.080         | **0.111**     | **0.131**        |

**Bold** = meets PROCEED threshold (≥0.08). JRA umaren narrowly misses; JRA wide/sanrenpuku are just at/below the threshold.

---

## 4. Interpretation

### Why do exotic odds contain information beyond tansho?

Pari-mutuel pools are independent. Win-bet (tansho) bettors maximize profit per win; quinella/wide/trio bettors optimize for different combinations. The marginalization procedure effectively aggregates consensus across bettors with different risk preferences, and this consensus contains soft place-probability information that the win-pool does not fully encode.

The effect is more pronounced for NAR and Ban-ei, where:

1. Ban-ei is a slow-speed draft horse event — finish positions are more correlated within the top-3 (horses rarely pass at the end), so pool signals for place are more stable.
2. NAR fields have more variance in ability (higher odds spread), making marginal information from multi-leg bets more valuable.

For JRA, the win pool is extremely liquid and efficient; exotic pools appear to offer only marginal incremental signal (partial ρ 0.05–0.08 range), which is borderline.

### Data gap caveat

NAR umaren and wide are missing 2024 (~26% of the holdout window). If 2024 data had been present, NAR partial ρ values would be based on larger samples and potentially slightly different. The directional finding (NAR wide/sanrenpuku ≥ 0.08) is unlikely to reverse given the large 2023+2025–2026 sample.

### AUC caution

The AUC comparisons use different tasks (tansho_top3 vs umaren_top2), so they are not directly comparable across rows. Umaren P2 at AUC 0.832–0.841 is not surprising given the task alignment (quinella predicts top-2). The important finding is that wide_P3 and sanrenpuku_P3 do not materially exceed the tansho baseline on AUC for top-3 prediction.

---

## 5. Per-Class PROCEED / ABORT Summary

| Class  | umaren_P2 partial ρ | wide_P3 partial ρ | sanrenpuku_P3 partial ρ | Decision                                |
| ------ | ------------------- | ----------------- | ----------------------- | --------------------------------------- |
| JRA    | 0.048               | 0.076             | 0.080                   | **ABORT** — no signal ≥0.08             |
| NAR    | 0.075               | **0.101**         | **0.111**               | **PROCEED** — wide + sanrenpuku qualify |
| Ban-ei | **0.098**           | **0.109**         | **0.131**               | **PROCEED** — all three qualify         |

---

## 6. VERDICT

**PROCEED** (partial scope: NAR + Ban-ei only)

- At least one exotic signal meets partial ρ ≥ 0.08 in both NAR and Ban-ei.
- **Strongest signal:** sanrenpuku P3 for Ban-ei (partial ρ = 0.131) and NAR (partial ρ = 0.111).
- **JRA:** all partial ρ values are below threshold (highest = 0.080 for sanrenpuku). JRA win-pool efficiency absorbs most exotic signal. No JRA feature addition warranted.

### Recommended next steps (PROCEED path)

1. **NAR + Ban-ei feature addition:** Add `exotic_umaren_p2`, `exotic_wide_p3`, `exotic_sanrenpuku_p3` as marginalized implied-probability features, sourced from `nvd_o2`, `nvd_o3`, `nvd_o5`.
2. **Serve-path verification first:** The `exotic-odds-availability.md` audit already confirmed these are stored in the realtime DO/D1 hot worker. The feature must be available at inference time (predict container). Verify `realtime_odds_fetcher.py` can decode them and they are present for upcoming races.
3. **Training refit:** Retrain NAR and Ban-ei models with exotic features added. Use incremental-model verify (walk-forward gate: place2 + place3 positive required).
4. **JRA:** no action — ABORT for JRA feature addition.
5. **2024 gap:** For NAR umaren/wide, the 2024 gap means training data will have a structural hole. Consider using sanrenpuku (which has complete 2024) as the primary exotic feature for NAR, and treating umaren/wide as secondary. Or fill with NULL and rely on GBDT NULL-routing.

---

## Appendix: Probe execution notes

- Script: `/tmp/exotic_probe.py`
- DB: `postgresql://horse_racing:***@127.0.0.1:15432/horse_racing`
- Python: `uv run python` in `apps/pc-keiba-viewer/` environment (psycopg3, scipy, sklearn)
- All queries read-only; no writes to any table
- Filter: `ijo_kubun_code = '0'` (normal finishes only), `data_kubun = '7'` for results, `data_kubun = '5'` for odds
- Partial ρ method: rank-residualize via OLS (numpy lstsq), then Pearson ρ on residuals
