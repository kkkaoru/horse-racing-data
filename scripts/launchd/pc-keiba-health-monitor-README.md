# `pc-keiba-health-monitor` — pipeline daily health checks

A Mac `LaunchAgent` that runs every 15 min during the JST 09:00-22:59
racing-day window and pages on patterns of failure across the racing-data
pipeline. Created in response to the **2026-06-28 fetch-results outage**:
the queue stopped processing at JST 15:42 and we discovered it ~7 h later
visually in the viewer. No alert, no log threshold, no automation noticed.
This monitor is the post-mortem control.

## What each check guards against

Every check returns 0 (healthy) or 1 (failing). A check needs to fail **3
consecutive ticks** (~45 min) before a notification fires, so a single
flap during a brief Cloudflare hiccup never pages.

| Check                    | What it guards                                                                                                                | Threshold rationale                                                                                                                                                                                       |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fetch_results_recent`   | D1 `fetch_logs` has at least one `job_type='fetch-results' status='ok'` row in the last 30 min during JST 13:00-21:30.        | Race results land every 1-2 min during racing hours. 30 min of silence = the queue or worker is dead, exactly the 2026-06-28 symptom.                                                                     |
| `fetch_weights_recent`   | Same as above for `fetch-weights` (race window JST 11:00-21:00).                                                              | Bataiju (馬体重) lands T-60..40 min before post. 30 min of silence during racing = scrape stalled.                                                                                                        |
| `queue_backlog`          | Cloudflare API `queues` backlog for `sync-realtime-data-jobs`.                                                                | > 200 messages **for two consecutive ticks** (~30 min). Single-tick spikes (e.g. weekend morning when 30 races enqueue in 2 min) are ignored — we wait for the backlog to actually persist before paging. |
| `today_results_coverage` | D1 `realtime_race_sources` — races finished > 30 min ago with NO `last_result_fetch_at` AND NO `result_complete_at`.          | This is the 2026-06-28 canonical signal. Threshold > 5 races = at least five finished races were silently skipped. Anything ≤ 5 could be normal (cancelled/postponed races, ban-ei single-row gaps).      |
| `today_trends_render`    | Viewer `/api/races/.../trends` returns a JSON body containing `finishPosition` keys somewhere in the tree.                    | The trends section is the canary the operator looks at to spot stale data. If the response shape collapses (e.g. SSR broke, KV cache poisoned), this catches it.                                          |
| `mojibake_in_responses`  | Viewer `/api/races/.../sections/training` — three sample races, fail if **any** body contains `U+FFFD` (replacement char).    | When upstream JRA/NAR HTML scrapes hit a charset edge-case, kanji turn into `���`. A regression that re-encodes bodies wrong would slip past tests but show up immediately here.                          |
| `weight_freshness`       | D1 `realtime_race_sources` joined to `horse_weight_snapshots` — races started > 60 min ago today with no associated snapshot. | Threshold > 5 races = the weights pipeline silently stalled for at least five finished races. Mirrors `today_results_coverage` but for the weights pipeline.                                              |

### Alert pattern

- **1st failure**: log only (label `FAIL (1) — first failure, log only`).
- **2nd failure**: log + AMBER label (`FAIL (2) — AMBER (notify at 3)`).
- **3rd consecutive failure**: log + RED label + macOS notification via `osascript -e 'display notification'`.
- **Recovery**: the next OK reading resets the counter to 0 and logs `RECOVERED (was at N consecutive failures) — counter reset`. The next failure starts over at 1.

State lives in `/Users/kkk4oru/Library/Logs/pc-keiba-health/state.json` and
is a simple `{ "<check>_consecutive_failures": N, ... }` map.

## Schedule

`StartCalendarInterval` fires at minute 0, 15, 30, 45 of every JST hour
between 09 and 22 inclusive (= 56 fires/day). Outside that window the
racing pipeline is naturally quiet (pre-09:00 the daily JRA mirror has not
landed yet; post-23:00 the last Ban-ei card is long over) and we don't
want to alert on expected silence.

The plist assumes the Mac is configured to **Asia/Tokyo (JST)** — see
the timezone caveat at the bottom.

## Files

- `pc-keiba-health-monitor.sh` — wrapper that does all the work.
- `com.kkk4oru.pc-keiba-health-monitor.plist` — LaunchAgent definition.
- `health-monitor-fixtures/*.json` — dry-run mocks for every external call
  (the `fail__*.json` variants drive deliberate-failure paths in tests).

## Install

```sh
launchctl bootstrap gui/$(id -u) \
  /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/scripts/launchd/com.kkk4oru.pc-keiba-health-monitor.plist
```

If you edit the plist later you must `bootout` first and `bootstrap`
again — launchd caches the loaded plist.

## Uninstall

```sh
launchctl bootout gui/$(id -u)/com.kkk4oru.pc-keiba-health-monitor
```

## Status check

```sh
launchctl print gui/$(id -u)/com.kkk4oru.pc-keiba-health-monitor \
  | grep -E 'state|last exit code|next firing'
```

Healthy output: `state = waiting`, `last exit code = 0`, and the next JST
quarter-hour timestamp.

## Manual fire

```sh
launchctl kickstart -k gui/$(id -u)/com.kkk4oru.pc-keiba-health-monitor

# Or invoke the script directly:
bash scripts/launchd/pc-keiba-health-monitor.sh
```

## Logs

- `~/Library/Logs/pc-keiba-health/stdout.log` / `stderr.log` — raw streams
  (launchd-managed).
- `~/Library/Logs/pc-keiba-health/YYYYMMDD.log` — per-JST-day dated log.
- `~/Library/Logs/pc-keiba-health/state.json` — consecutive-failures map.
- `~/Library/Logs/pc-keiba-health/lock-skips.log` — one line per tick
  where a previous run was still in progress.

Tail today's:

```sh
tail -f ~/Library/Logs/pc-keiba-health/$(date -u -v+9H +%Y%m%d).log
```

## Silencing notifications

When you're on vacation or knowingly working through a known outage:

```sh
touch /tmp/pc-keiba-health-silenced
```

Logs still run; only the macOS notification is suppressed. Re-arm with:

```sh
rm /tmp/pc-keiba-health-silenced
```

## Dry-run mode (for hacking on the script)

Every external call is replaced by a fixture file. State writes go to a
per-run tmpdir so they cannot clobber the live counter file.
Notifications are NEVER sent in dry-run; the planned body is printed.

```sh
# All checks pass:
HEALTH_DRY_RUN=1 bash scripts/launchd/pc-keiba-health-monitor.sh

# Drive deliberate failures (loads health-monitor-fixtures/fail__<name>.json
# when present, falls back to the ok fixture otherwise):
HEALTH_DRY_RUN=1 HEALTH_DRY_RUN_FIXTURE_PREFIX=fail__ \
  bash scripts/launchd/pc-keiba-health-monitor.sh

# Three failure ticks to confirm the 3rd one notifies — pin a temp state dir
# so the counter persists across runs:
TEST_DIR=/tmp/health-flow-$$ && mkdir -p "$TEST_DIR"
for i in 1 2 3; do
  HEALTH_TMP_ROOT="$TEST_DIR" HEALTH_DRY_RUN=1 \
    HEALTH_DRY_RUN_FIXTURE_PREFIX=fail__ \
    bash scripts/launchd/pc-keiba-health-monitor.sh
done
```

## Secrets handling

- `CLOUDFLARE_DEBUG_TOKEN` is read from the repo-root `.env`.
- `CLOUDFLARE_ACCOUNT_ID` is read from the same file (queue-backlog check
  is skipped with a WARN log if either is missing).
- `PC_KEIBA_ACCESS_CLIENT_ID` / `_SECRET` are read from
  `apps/pc-keiba-viewer/.env.local` (used for the viewer endpoint checks
  behind Cloudflare Access).
- `NEON_DATABASE_URL` is read from `apps/local-postgresql/.env.replica`
  (currently only loaded for future Neon-side checks; not used by the
  current 7 checks but loaded eagerly so the failure mode if it's missing
  is detected at install time, not at first incident).

All tokens are masked with `<first4>...<last4>` in logs.

## Timezone caveat

`StartCalendarInterval` uses the system local timezone (no `TZ` field
exists for launchd plists). This plist assumes the Mac is configured to
**Asia/Tokyo (JST)**. If you change the system timezone, edit each `Hour`
integer in the plist accordingly. The wrapper computes its window
boundaries from UTC+9 directly, so the **check logic** is robust to TZ
drift; only the **firing times** depend on the system TZ.

## Why this lives next to the existing launchd agents

The existing `com.kkk4oru.race-prediction-guard` LaunchAgent is a
_completeness_ guard: it re-kicks prediction work when D1/Neon counts
disagree. This new agent is a _failure-detection_ monitor: it watches
the things the guard cannot self-heal (queue backlogs, viewer surface
correctness, fetch-log silence) and pages the operator. The two agents
do not coordinate state — they look at the system from different angles.
