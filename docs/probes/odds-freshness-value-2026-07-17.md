# Odds freshness value — morning vs near-post, for the coordinator re-enable decision (2026-07-17)

- **Date**: 2026-07-17
- **Trigger**: `COORDINATOR_ENABLED=0` since 2026-07-11 means near-post rescore
  is off — JRA predictions are generated once by the ~09:30 JST cron and
  scored against odds as they stood at that time, never refreshed against
  odds at/near actual post. This probe quantifies how much that staleness
  plausibly costs, to attach a number to campaign-summary §f item ⑦
  (coordinator re-enable decision).
- **MLflow**: run `9feb00f14f974f7aab77d228c4d0a17b`, experiment
  `jra-odds-freshness-value-2026-07-17`.
- **Scope note up front**: the originally-specified design (2-arm champion
  replay on the 264-race summer-3-venue window, odds features swapped
  morning-vs-near-post, paired cell×rank1-5 comparison) **could not be
  executed** — see §1 for why — and this doc instead reports a **direct,
  real-data input-drift measurement** as a bounding proxy. This is a partial
  answer, explicitly scoped as such per the task's own fallback instruction.

## 1. Data availability check (why the full 2-arm replay wasn't possible)

Checked every plausible source of a genuine intraday odds _time series_ for
JRA races:

1. **`odds_snapshots` (D1, `apps/sync-realtime-data`)** — this table does
   carry multi-snapshot history (confirmed schema:
   `(race_key, odds_type, combination, fetched_at, odds, rank, ...)`,
   indexed for time-series reads). But querying it for the actual target
   window found **zero rows for any `jra:%` race key in June or July 2026 at
   all** (checked 2026-06-13, the 264-race replay's start date, and broadly).
   The only window with real JRA tansho-odds time-series data anywhere in
   this table is **2026-05-23/05-24** (72 races, 2.77M rows) — seven weeks
   before the current date, five weeks before the 264-race replay's own
   window starts, and at venue `04` (Niigata), not any of the summer-3
   venues (02/03/10). JRA odds snapshot capture for this table appears to
   have run only briefly in late May and not since — worth flagging as its
   own finding (§4).
2. **`REALTIME_HOT`** — checked `apps/sync-realtime-data/src/types.ts:207`:
   this is a Cloudflare **service binding** (`{ fetch: typeof fetch }`), i.e.
   a live Worker-to-Worker RPC call, not a data store. It has no historical
   retention by construction — it always returns whatever the upstream
   source says _right now_, so it cannot answer "what did the odds look like
   this morning" for a past race at all.
3. **`jvd_o1` (local PG mirror of JV-Data's odds table, the "b) jvd 確定オッズ"
   fallback the task brief anticipated)** — checked directly: every single
   row in this table (114,334 total, all of 2026 JRA/NAR) has
   `data_kubun='5'` (settled/final) and `happyo_tsukihi_jifun` (announcement
   timestamp) is **always the null-placeholder `'00000000'`**, and there is
   **exactly one row per race** (verified: `count(*) = count(distinct
race)` for every 2026 summer-venue race checked). JRA-VAN's own wire
   format does support multiple intraday odds announcements, but whatever
   this repository's ingestion pipeline does with `jvd_o1` collapses to a
   single final snapshot — there is no morning-time value recoverable from
   this table at all, for any date.

**Conclusion**: no source has genuine intraday odds history for the summer-3-
venue window this campaign is otherwise using. Building a _fresh_ 16-layer
feature harvest (the 264-race replay's own machinery) for the one window
that _does_ have real snapshot data (72 late-May races, all-JRA-venue, not
summer-3) was not achievable inside the remaining time budget once data
discovery had run its course. Given the task's own fallback instruction for
exactly this situation, the rest of this doc reports a lighter-weight,
still-real, still-quantitative analysis instead: measuring how much the
market's own signal (tansho odds / popularity rank) actually moves between
morning and near-post, directly from the one dataset that has real snapshots.

## 2. Method

For the 71 JRA races in the 2026-05-23/24 `odds_snapshots` sample with
`odds_type='tansho'` data (1 race excluded — no rank=1 row at one of the two
anchor times):

- **"Morning" anchor**: the first available snapshot at or after 09:30 JST
  on race day — chosen to match the actual production cron's generation
  time (`apps/finish-position-cron/wrangler.jsonc`, JST 09:30 feature-build
  cron), not the dataset's earliest available point (~01:11 JST, well before
  any real cron would run — using that instead would only _overstate_ the
  staleness gap, so 09:30-anchoring is the more defensible choice; both are
  reported since they agree closely, see §3).
- **"Near-post" anchor**: the last available snapshot for that race
  (`MAX(fetched_at)`), a reasonable proxy for "closest data actually
  available before the race," though not guaranteed to be the literal final
  tick before the gate opens.
- For each race, took the horse ranked `rank=1` (tansho favorite) at each
  anchor and checked whether it's the same horse, plus the relative change
  in that favorite's own odds value between anchors.
- Pure SQL aggregation directly against the real D1 table (see §5 for the
  query) — no synthetic data, no interpolation, no model scoring involved in
  this part.

## 3. Result

| Metric                                                                    | Value                          |
| ------------------------------------------------------------------------- | ------------------------------ |
| Races with valid morning+near-post tansho rank-1 pair                     | 71                             |
| Median morning→near-post span                                             | 661.7 min (~11h)               |
| Minimum span                                                              | 402.9 min (~6.7h)              |
| **Favorite (rank-1) changed, 09:30-anchored → near-post**                 | **20/71 = 28.2%**              |
| Favorite changed, earliest-available (~01:11 JST) → near-post             | 21/71 = 29.6% (agrees closely) |
| Favorite's own odds, median absolute relative change (when unchanged too) | 19.2%                          |
| Favorite's own odds, p75 absolute relative change                         | 32.7%                          |
| Favorite's own odds, max absolute relative change                         | 46.9%                          |

**Roughly 3 in 10 JRA races see their market favorite change entirely**
between a 09:30-ish snapshot and the last odds reading before post, and even
when the favorite horse stays the same, its odds typically move by ~19%
(median) to ~33% (p75). This is not a small-print effect — it's a
first-order amount of drift in the single input family this campaign's own
evidence repeatedly identifies as the most powerful predictor available
(market top1 rate ≈ 33.7% vs the champion model's own 20-24% top1 in the
264-race replay, `jra-summer3-local-replay-2026-07-17.md` §2.1/§8.4).

## 4. What this does and doesn't establish

**Does establish**: the _raw input_ the coordinator's near-post rescore
would refresh is genuinely, substantially stale under the current
09:30-cron-only regime — this is a real, measured fact, not a guess. A
model whose predicted_score depends materially on odds-derived features
(as this repo's own feature-importance work has repeatedly found for the
champion) is necessarily scoring a meaningfully different favorite/rank
structure than what the market actually settles on, for close to 3 in 10
races.

**Does not establish**: a precise top1/place2/place3 accuracy delta (pp) for
turning the coordinator back on. That would require the full paired
champion re-score design the task specified — same race, same everything
except the odds-derived features (`tansho_odds`, `tansho_ninkijun`,
`popularity_score`, `odds_score` — the exact race-fresh-in-principle column
set identified in today's separate day-base-split investigation,
`docs/probes/serving-latency-architecture-2026-07-17.md` §1a), scored twice
and compared. The model does **not** simply mirror the market (it disagrees
with the market's own top1 pick on the majority of races by design — that's
the entire reason a model exists rather than just serving `tansho_ninkijun`
directly), so a 28% input-favorite-churn rate is an **upper bound on the
opportunity**, not a direct estimate of the achievable accuracy gain — the
true number is smaller than 28pp of top1, likely by a large factor, and
could not be responsibly quantified further without the full re-score.

## 5. Recommendation for §f ⑦ and for next steps

1. **For the coordinator decision itself**: this is genuine, if partial,
   evidence _in favor_ of re-enabling near-post rescore being worth
   pursuing — the underlying input the whole mechanism exists to refresh is
   confirmed to drift substantially and often (not a rare edge case), on the
   one real dataset available to check. It should be weighed alongside the
   `wrangler.jsonc` comment's own stated reason `COORDINATOR_ENABLED` was
   turned back off on 2026-07-11 same-day: the R2 feature-cache the rescore
   path depends on is never populated by the focused-full path, so every
   rescore attempt today would `CacheMissError` and silently fall back to a
   full ~15-27 min rebuild completing after post — i.e. **the mechanism is
   currently unsafe to re-enable regardless of this doc's finding**, until
   that caching defect is fixed. This doc quantifies the upside; it does not
   change the precondition.
2. **Snapshot retention, as the task's own fallback anticipated**: JRA
   odds-snapshot capture into `odds_snapshots` ran for only ~2 days in late
   May 2026 and has not run since (verified empirically, §1). If this
   question is worth revisiting with a real paired model re-score (the
   original design), that requires either (a) resuming continuous JRA odds
   capture into `odds_snapshots` so a _future_ window has genuine intraday
   history to replay against, or (b) accepting the 05-23/24 window as the
   basis and investing the time to build a fresh feature harvest for those
   72 races specifically. Neither was in scope to execute in this session's
   remaining budget.
3. **Next cycle, if pursued**: run the originally-specified 2-arm champion
   replay against whichever window ends up with real snapshot data, feeding
   morning-anchored vs near-post-anchored `tansho_odds`/`tansho_ninkijun`/
   `popularity_score`/`odds_score` through the existing high-fidelity 264-race
   replay machinery (`apps/pc-keiba-viewer/tmp/candidate-jra-summer3-local-replay-2026-07-17/`,
   ρ=0.93 validated parity vs genuine production) — everything else about
   that pipeline is already reusable as-is.

## 6. Artifacts

- Query + result: ad hoc D1 SQL against `sync-realtime-data`'s `odds_snapshots`
  (read-only `SELECT`, no writes) — see §2 for the exact anchor definitions;
  raw JSON results retained in this session's scratchpad, not committed
  (per repo convention, `tmp/`/scratchpad artifacts are not tracked).
- MLflow run `9feb00f14f974f7aab77d228c4d0a17b`,
  experiment `jra-odds-freshness-value-2026-07-17` — full metric table logged
  via `mlflow_tracking log-eval --eval-regime oos`.
- Reused (read-only, unmodified): `apps/sync-realtime-data/migrations/0001_init.sql`,
  `0008_jra_race_keys.sql` (schema/history confirmation), local PG `jvd_o1`
  (schema + coverage confirmation), `apps/finish-position-cron/wrangler.jsonc`
  (`COORDINATOR_ENABLED` precondition context), the 264-race replay doc and
  its `docs/probes/jra-summer3-local-replay-2026-07-17.md` (market/model top1
  reference numbers).
