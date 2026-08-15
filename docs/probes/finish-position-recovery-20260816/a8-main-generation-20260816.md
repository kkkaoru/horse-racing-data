# A8 main generation — isolated command

Race: `2026/08/16/A8/04` (Prix Jacques le Marois). Post 22:50 JST.
Window: **21:00–22:00 JST**. No PostgreSQL / Neon / R2 / cache writes.

## One command

Dry-run (uses the 07:12 official-card JSON, no network):

```sh
uv run --project apps/pc-keiba-viewer python \
  docs/probes/finish-position-recovery-20260816/generate_a8_main.py --dry-run
```

21:00 execute (one polite official-card GET, isolated tmp artifacts only):

```sh
uv run --project apps/pc-keiba-viewer python \
  docs/probes/finish-position-recovery-20260816/generate_a8_main.py --execute --fetch
```

Optional: reuse a saved HTML instead of fetching.

```sh
uv run --project apps/pc-keiba-viewer python \
  docs/probes/finish-position-recovery-20260816/generate_a8_main.py \
  --execute --html tmp/jacques-le-marois-a8-market-20260816/jra-jacques-le-marois-20260816-0712.utf8.html
```

## What the command does

1. Load a 10-runner board (saved JSON, saved HTML, or one official GET).
2. Judge the revised valid-full gate:
   - 10/10 positive finite odds
   - competition ranking (ties allowed; next rank skips)
   - odds nondecreasing with rank, and same rank ⇒ same odds
3. If the board fails, write `a8-board-invalid.json` and exit 2. No scores.
4. If it passes, inject 13 production board-derived market features and leave
   the 3 similar-race market features NULL (`sim_odds_*`, `sim_fav_win_rate`).
   Deauville is not in the JRA similar-race pool.
5. Rescore the existing market-null 250-vectors with
   `jra-cb-v9-sim-2013-CLEAN`. Audit NULLs become `0.0` only inside CatBoost.
6. Write isolated JSON under `tmp/jacques-le-marois-a8-dry-run-20260816/`
   (dry-run) or `tmp/jacques-le-marois-a8-main-20260816/` (execute).
7. Always publish coverage and quality: `nonnullLabel` (`47-47/250` on the
   07:12 dry-run) and whether softmax is nearly uniform. A printed ranking is
   not the same as a meaningful prediction.

## Coverage and quality (07:12 dry-run)

- Non-NULL features: **47/250** for every runner (34 market-null + 13 board).
- 203/250 remain NULL. Identity-dependent JRA person/pedigree/similar-race
  populations stay missing because A8 horses have no usable JV horse IDs.
- Softmax is **not uniform** (min 1.59%, max 22.61%). The board separates the
  field; most of that separation comes from the 13 market features.

## Production formulas used for the 13 board features

| Feature                               | Formula                               |
| ------------------------------------- | ------------------------------------- |
| `tansho_odds_raw`                     | official tansho odds                  |
| `tansho_ninkijun_raw`                 | official ninki (competition rank)     |
| `popularity_score`                    | `(ninki - 1) / (N - 1)`               |
| `odds_score`                          | `clamp01(ln(max(odds, 1)) / ln(300))` |
| `inverse_odds_implied_prob`           | `1 / odds`                            |
| `inverse_odds_market_share`           | implied / race sum(implied)           |
| `inverse_odds_rank_in_race`           | SQL `rank()` on implied desc          |
| `popularity_rank_in_race`             | SQL `rank()` on ninki asc             |
| `odds_score_diff_from_race_avg`       | odds_score − race mean                |
| `popularity_score_diff_from_race_avg` | popularity_score − race mean          |
| `popularity_odds_disagreement`        | abs(popularity_score − odds_score)    |
| `field_dominant_favorite_indicator`   | 1人気 odds / 2人気 odds               |
| `horse_popularity_vs_field`           | ninki / N                             |

No median fallback: a valid full board already has odds and ninki for every runner.

## Inputs that must exist

- `tmp/jacques-le-marois-a8-experimental-20260815/a8-market-null-prediction-full.json`
- `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/artifacts/jra-cb-v9-sim-2013-CLEAN/{model.json,metadata.json}`
- For dry-run: `tmp/jacques-le-marois-a8-market-20260816/hot-board-check-0712.json`

## Tests

```sh
uv run --project apps/pc-keiba-viewer pytest -q \
  docs/probes/finish-position-recovery-20260816/test_generate_a8_main.py
```
