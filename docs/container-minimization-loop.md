# Container runtime minimization — loop tracker (from 2026-09-23)

Goal: run JRA / NAR / Ban-ei running-style and finish-position predictions on
Cloudflare Workflows + Workers and keep production Container runtime minimal,
with production verification for every change. Parity rules from
`container-native-parity-review.md` still apply: a native path may publish only
after full published-value parity against the Python reference on frozen
inputs, with fail-open to Python.

## Baseline (predict Container `a0348266`, standard-4 / 12 GiB)

| JST day            | Type           | predict GiB-h | race-chain GiB-h | CPU util |
| ------------------ | -------------- | ------------- | ---------------- | -------- |
| 9/16, 9/17         | NAR-only       | 28–36         | 9–11             | ~6%      |
| 9/18–9/22          | JRA/NAR/Ban-ei | 220–562       | 5–108            | ~6%      |
| 9/23 (after 06:42) | NAR-only       | ~2–4 per hour | 0 after 06h      | ~3%      |

Hourly profile on 9/20: ~2 predict instances alive 09–24h at ~6% CPU, i.e.
the cost was held-idle memory, not compute. The 9/22–9/23 fixes (fenced
day-base rebuilds, Workflow-committed day-base) cut the daytime rate by ~85%.

## Where Container time goes now (9/23)

1. Day-base DuckDB build: once per category/day via `RaceDayWorkflow`
   (`sync-day-base`), Container not detached.
2. Initial per-race predictions: race-chain Container, ~05–06h JST.
3. Post-weight rescores: NAR and Ban-ei always use the predict Container;
   JRA rescores also reached the Container on 9/20 (no `Rescore Worker` log in
   7 days) despite `JRA_WORKER_RESCORE_ENABLED=1`.
4. Night 00–04h JST: ~10 GiB-h/h predict with ~2% CPU on a NAR-only night.

Running-style inference is already Worker-native (LightGBM trees in
`sync-realtime-data`); its only Container dependency is the day-base parquet.

## Milestones (cheapest and safest first)

| #   | Item                                                        | Status                |
| --- | ----------------------------------------------------------- | --------------------- |
| M1  | Day-base rebuild loop on partial RS (night/day idle holder) | deployed 9/23, verify |
| M2  | JRA rescore reaches Container despite Worker flag — fix     | open                  |
| M3  | Ban-ei rescore Worker-native (shadow parity → enable)       | open                  |
| M4  | NAR rescore Worker-native (scorer + parity harness)         | open                  |
| M5  | Initial per-race predictions Worker-native                  | open                  |
| M6  | Running-style independent of Container day-base             | open                  |
| M7  | Day-base DuckDB layers ported to R2 SQL / Workers           | open                  |

JRA and Ban-ei changes need verification on their race days (next JRA:
9/26–27). A NAR-only day does not verify JRA or Ban-ei.

## Log

- 2026-09-23 16:30 JST — M1 root cause: `running-style-race-count-66-of-71`
  kept NAR day-base "stale" on 9/22 and rebuilt the 12 GiB day-base every few
  minutes until midnight; each rebuild reproduced the same bytes. Readiness now
  waives the race-count check only when the artifact holds exactly the current
  RS rows (content hash + row count). Commit 011f9f1f, Worker-only version
  `b424bae0` (Container images untouched). Verify: predict GiB-h tonight and on
  9/26–27 JRA days versus the baseline above.
