# A8 early market board check — 2026-08-16 07:12 JST

## Neon FP (same moment)

- Overall: **80 races / 940 rows**, latest `07:07:10` JST
- Baseline 04:43: 18/199 → full-day completion while host local generation ran
- JRA 36/490 (all `jra-cb-stage1-marketfree235-2013`)
- NAR 32/333 (all marketfree stage1)
- Ban-ei 12/117
- Interpretation: local DuckDB 1.5.5 write burst confirmed; not container-only drip

## A8 official card fetch

- One polite GET of `https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73`
- Saved under `tmp/jacques-le-marois-a8-market-20260816/`
- Structured check: `tmp/jacques-le-marois-a8-market-20260816/hot-board-check-0712.json`

## Valid-full gate (old rules — strict 1..N permutation)

| Check                        | Result                                    |
| ---------------------------- | ----------------------------------------- |
| 10/10 positive finite odds   | **PASS**                                  |
| Rank permutation 1..10       | **FAIL** — ranks `[7,8,1,4,4,10,9,6,3,2]` |
| Odds nondecreasing with rank | **FAIL** (blocked by rank fail)           |
| **validFullBoard**           | **NO**                                    |

Failure mode under old rules: umaban 4 シュトラウス and umaban 5 モアサンダー both show `6.9` / `4番人気`; rank 5 is missing.

## Re-judge 07:14 — competition ranking (advisor revision)

No re-fetch. Reused `hot-board-check-0712.json`.
Artifact: `tmp/jacques-le-marois-a8-market-20260816/hot-board-rejudge-competition-0714.json`

| Check                                               | Result                                         |
| --------------------------------------------------- | ---------------------------------------------- |
| 10/10 positive finite odds                          | **PASS**                                       |
| Competition ranking (ties allowed; next rank skips) | **PASS** — sorted ranks `1,2,3,4,4,6,7,8,9,10` |
| Odds nondecreasing with rank                        | **PASS**                                       |
| Same rank ⇒ same odds                               | **PASS** (both 4人気 = 6.9)                    |
| **revisedValidFullBoard**                           | **YES**                                        |

Conclusion: the 07:12 `NO` was caused by an overly strict permutation rule, not a broken board. The board is usable under competition ranking.

## Decision

- 07:12 early overlay was correctly skipped under the then-agreed rules.
- Under revised rules the board is usable; no automatic early overlay unless advisor requests it.
- Main A8 prediction window remains **21:00–22:00 JST**.
- No DB / R2 / cache writes.
