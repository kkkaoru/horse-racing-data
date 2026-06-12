# JRA Fukusho Odds Partial ρ Probe

**Date:** 2026-06-13  
**Scope:** JRA 2023–2026, `jvd_o1.odds_fukusho` decoded per-horse, partial Spearman ρ vs place outcomes  
**Verdict:** **ABORT** — fukusho_implied_p3 partial ρ for is_top3/is_top2 is 0.02–0.03 (well below gate ≥0.08); finish_norm ρ passes numerically but is a full-ordinal artifact driven by tansho, not place-specific signal.

---

## 1. Untried Verification

### What was previously tested

- `unused-columns-census-jra.md` (2026-06-12): marked `jvd_o1` columns (`odds_tansho`, `odds_fukusho`, `odds_wakuren`) as **DO-NOT-RETEST (odds time-series)** — this blanket label was applied to ALL o1 columns.
- `exotic-odds-place-signal.md` (2026-06-12): probed **o2/o3/o5** (umaren/wide/sanrenpuku) for JRA → ABORT (highest partial ρ = 0.080 for sanrenpuku).
- Neither document ran the specific `fukusho_implied_p3` or `fukusho_tansho_ratio` derived features.

### What has NOT been tested until this probe

`fukusho_implied_p3` (overround-normalized inverse of fuku_mid per horse) and `fukusho_tansho_ratio` (place-to-win residual) were **untried** as incremental JRA features. This probe fills that gap.

---

## 2. Schema / Decode Findings

### jvd_o1 table layout (confirmed by cross-check against jvd_se)

| Field                          | Type         | Notes                                  |
| ------------------------------ | ------------ | -------------------------------------- |
| `odds_tansho`                  | VARCHAR(224) | Fixed 28-slot × 8-char packed string   |
| `odds_fukusho`                 | VARCHAR(336) | Fixed 28-slot × 12-char packed string  |
| `data_kubun`                   | VARCHAR(1)   | `'5'` = pre-race final snapshot        |
| `hatsubai_flag_tansho/fukusho` | VARCHAR(1)   | `'7'` = enabled (not `'1'` as assumed) |

### Stride decode (validated against jvd_se for 18-horse races)

**Tansho stride = 8 chars per horse:**

```
umaban(2) + odds(4) + ninkijun(2)
```

`tansho_odds = CAST(odds_field AS INTEGER) / 10.0` → matches `jvd_se.tansho_odds` exactly (all 18 horses verified).

**Fukusho stride = 12 chars per horse:**

```
umaban(2) + fuku_min(4) + fuku_max(4) + ninkijun(2)
```

`fuku_min = CAST(min_field AS INTEGER) / 10.0`, `fuku_mid = (fuku_min_raw + fuku_max_raw) / 20.0`

Verified: horse 12 (favourite, ninkijun=01) → fukusho min=1.4x, max=1.9x ✓; horse 13 (long shot) → min=87.2x, max=137.0x ✓.

**IMPORTANT NOTE on `data_kubun`:** The `data_kubun='5'` in `jvd_o1` corresponds to the pre-race CONFIRMED odds snapshot. The `hatsubai_flag` in this dataset is universally `'7'` (not `'1'`); the earlier 0-row extraction was caused by filtering `hatsubai_flag = '1'`.

### Serve parity

The hot worker (`apps/sync-realtime-data/src/jra.ts`) has `parseFukushoOdds()` and scrapes JRA fukusho as min/max odds from the `単勝・複勝` page (same URL as tansho). Fukusho is fully stored in `odds_snapshots` with `odds_type = 'fukusho'`. **Serve parity exists** — if the feature were useful, it could be served at inference time without infrastructure changes.

---

## 3. Probe Methodology

- **Data:** `jvd_o1` (JRA 2023–2026, `data_kubun='5'`) joined to `jvd_se` (results, `data_kubun='7'`, `ijo_kubun_code='0'`) via race + umaban.
- **N:** 162,239 horse-race rows across 11,871 races.
- **Features:**
  - `fukusho_implied_p3` = `(1/fuku_mid) / Σ(1/fuku_mid_i)` (overround-normalized per race)
  - `fukusho_tansho_ratio` = `fukusho_implied_p3 / tansho_implied_win`
- **Outcomes:** `is_top3` (binary top-3 finish), `is_top2` (binary top-2 finish), `finish_norm` (1 − rank-norm, full ordinal)
- **Controls:** `tansho_implied_win` + `ninkijun` (ranked residuals via OLS, partial Spearman ρ method)
- **Gate:** ρ ≥ 0.08 AND within-race variation present

---

## 4. Per-Class Partial Spearman ρ Results

### fukusho_implied_p3 (partial ρ, controlling for tansho + ninkijun)

| Class             |       N | ρ / is_top3 | ρ / is_top2 | ρ / finish_norm |
| ----------------- | ------: | ----------: | ----------: | --------------: |
| G1/Special (005)  |  42,189 |     +0.0225 |     −0.0104 |         +0.0961 |
| Open/Listed (010) |  21,029 |     +0.0183 |     −0.0122 |         +0.0948 |
| 3yr-Special (016) |  10,496 |     +0.0047 |     −0.0124 |         +0.0881 |
| 2yr (703)         |  61,173 |     +0.0289 |     −0.0015 |     **+0.1350** |
| Novice (701)      |  12,525 |     +0.0224 |     +0.0056 |         +0.0257 |
| OVERALL           | 162,239 |     +0.0237 |     −0.0056 |         +0.1017 |

**p-values (OVERALL):** all significant (p < 0.001) due to large N, but effect sizes for top3/top2 are tiny.

### fukusho_tansho_ratio (partial ρ, controlling for tansho + ninkijun)

| Class             |       N | ρ / is_top3 | ρ / is_top2 | ρ / finish_norm |
| ----------------- | ------: | ----------: | ----------: | --------------: |
| G1/Special (005)  |  42,189 |     −0.0391 |     −0.0434 |         +0.0972 |
| Open/Listed (010) |  21,029 |     −0.0380 |     −0.0469 |         +0.0842 |
| 3yr-Special (016) |  10,496 |     −0.0317 |     −0.0297 |         +0.0861 |
| 2yr (703)         |  61,173 |     −0.0957 |     −0.0849 |         +0.0983 |
| Novice (701)      |  12,525 |     −0.0962 |     −0.0849 |         +0.0187 |
| OVERALL           | 162,239 |     −0.0548 |     −0.0521 |         +0.0882 |

---

## 5. Interpretation

### Why finish_norm ρ ≥ 0.08 but is_top3/is_top2 ρ << 0.08?

`finish_norm` is a full-ordinal outcome (position 1–18 normalized to [0,1]). The ρ ≥ 0.08 for finish_norm reflects that **fukusho odds capture general horse quality** — they rank horses similarly to tansho odds across the full spectrum. This is not the same as saying fukusho contains place-specific signal.

Decile analysis confirms this: the top-3 hit rate pattern for `fuku_p3` is nearly **identical** to `tansho_implied_win`:

|      Decile | fuku_p3 top3-rate | tan_win top3-rate |
| ----------: | ----------------: | ----------------: |
|  0 (lowest) |             0.94% |             1.02% |
|           4 |             13.1% |             12.8% |
| 9 (highest) |            65.98% |            65.73% |

The two signals are co-linear. After controlling for tansho, fukusho adds < 2.5pp of partial ρ on top3, which is below the gate.

### Why is fukusho_tansho_ratio NEGATIVELY correlated with top3/top2?

This is an artifact of how the ratio is defined:

- Long-shot horses have tiny `tansho_implied_win` (denominator)
- Their `fukusho_implied_p3 / tiny_tansho_win` inflates the ratio
- Long shots rarely hit top3 → **high ratio = low top3 hit rate**

Decile 0 (lowest ratio, i.e. favourites): 61.2% top3 rate. Decile 9 (highest ratio, i.e. long shots): 5.6% top3 rate. The negative partial ρ for top3/top2 is not exploitable signal — it is the same popularity signal already captured by `ninkijun` and `tansho_implied_win`.

The finish_norm ρ for ratio (+0.09) is again a full-ordinal artifact.

### Within-race variation: present

- `fukusho_implied_p3` within-race std: mean = 0.0701, min = 0.026 → variation present across all races.
- `fukusho_tansho_ratio` within-race std: mean = 0.581, min = 0.099 → variation present.

Within-race variation is NOT the binding constraint here — the problem is collinearity with tansho.

---

## 6. Serve Parity Note

Fukusho odds ARE scraped and stored in the realtime pipeline. The `apps/sync-realtime-data/src/jra.ts` `parseFukushoOdds()` function extracts min/max odds from the `単勝・複勝` HTML table. `odds_snapshots` stores these with `odds_type = 'fukusho'`. Zero infrastructure work would be needed if the feature were adopted.

However, since the feature is ABORT, serve parity is moot.

---

## 7. VERDICT: ABORT

**Deciding ρ:**

- `fukusho_implied_p3` / `is_top3`: ρ = +0.024 (gate: ≥0.08) — **FAIL**
- `fukusho_implied_p3` / `is_top2`: ρ = −0.006 — **FAIL**
- `fukusho_tansho_ratio` / `is_top3`: ρ = −0.055 (wrong direction, artifact) — **FAIL**
- Only `finish_norm` partial ρ passes the gate numerically, but it is a full-ordinal artifact: the decile analysis confirms fukusho and tansho produce nearly identical top-3 hit rate curves; the partial ρ on finish_norm is driven by general market quality, not place-specific signal.

**Root cause:** JRA tansho and fukusho pools are highly co-determined. The pari-mutuel mechanism means that win-pool market efficiency (already captured via `tansho_implied_win` + `ninkijun_se`) subsumes the place pool. This is the same conclusion as `exotic-odds-place-signal.md`: JRA's market efficiency absorbs exotic signals.

**No JRA feature addition from fukusho odds warranted.** This probe exhausts the `jvd_o1` signal space for JRA.

---

## Appendix: Execution Notes

- DB: `postgresql://horse_racing:***@127.0.0.1:15432/horse_racing` (local PostgreSQL)
- DuckDB: `SET memory_limit='6GB'; SET threads=4;`
- Extraction: DuckDB postgres scanner, read-only
- Script: `/tmp/fukusho_probe2.py` + `/tmp/verify_decode2.py`
- Partial ρ method: rank-residualize via OLS (numpy lstsq), then Spearman ρ on residuals
- Filter: `ijo_kubun_code='0'` (normal finishes), `data_kubun='7'` for results, `data_kubun='5'` for odds
- All 162,239 horse-race rows used; no train/holdout split (this is a signal gate probe, not a model gate)
