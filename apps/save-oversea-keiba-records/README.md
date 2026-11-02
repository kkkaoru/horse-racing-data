# save-oversea-keiba-records

Manual macOS Bun CLI for saving one overseas race and its runners to local PostgreSQL as one `jvd_ra` row and multiple `jvd_se` rows. Each run takes two dynamic identifiers: a JRA racecard identifier and a secondary-source race identifier.

## Usage

From the repository root:

```sh
bun run --filter save-oversea-keiba-records save -- \
  '<jra-racecard-id>' '<secondary-race-id>' \
  --venue-code '<jv-venue-code>' \
  --race-number '<jv-race-number>'
```

The default is a dry run: the command connects to local PostgreSQL, resolves master data, prints the diff, and writes nothing.

Options:

- `--apply`: write only after the diff gate reports `safe`.
- `--dry-run`: explicitly select the default no-write mode.
- `--jra-file <path>`: read the JRA or JRA-VAN World card from a local file and skip its HTTP request.
- `--jra-url <url>`: explicitly fetch a `https://world.jra-van.jp/race/<race>/<year>/racecard/` card while the official JRA CNAME is unavailable. Without this option, the existing official JRA URL path remains the default.
- `--secondary-file <path>`: read the secondary card from a local file and skip its HTTP request.
- `--venue-code <code>`: required JV venue code used in the storage key.
- `--race-number <number>`: required JV race number used in the storage key.

Value-taking options also accept `--option=value` form. Local-file overrides are loaded in preference to HTTP, so a run makes only the requests needed for sources without overrides.

## Environment

The CLI reads values from the operator's environment; it does not load an environment file itself.

| Variable                                | Requirement                                                                                                                                                                                                              |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `POSTGRES_HOST`                         | Optional; defaults to `127.0.0.1`.                                                                                                                                                                                       |
| `POSTGRES_PORT`                         | Optional; defaults to `15432`.                                                                                                                                                                                           |
| `POSTGRES_DB`                           | Required.                                                                                                                                                                                                                |
| `POSTGRES_USER`                         | Required.                                                                                                                                                                                                                |
| `POSTGRES_PASSWORD`                     | Required.                                                                                                                                                                                                                |
| `OVERSEA_SECONDARY_CARD_URL_TEMPLATE`   | Required when the secondary source is fetched over HTTP. It must be a full card URL template containing the literal `{RACE_ID}` placeholder; the CLI substitutes the secondary race identifier for every occurrence.     |
| `OVERSEA_SECONDARY_MARKUP_PROFILE_PATH` | Required to parse the secondary card. Absolute path to a local JSON markup profile that describes how to locate horse-number cells, gate cells, entity identity links, and affiliation labels in the secondary document. |

Environment values, credentials, fetched documents, and source-specific markup must never be printed or committed.

### Secondary markup profile (operator-supplied)

The secondary source restricts automated access. This repository therefore does **not** publish that site's class tokens, identity-route prefixes, or other markup structure.

Operators keep a private JSON profile outside version control (for example `secondary-markup-profile.json` in this app directory, which is gitignored) and point `OVERSEA_SECONDARY_MARKUP_PROFILE_PATH` at it. The committed parser is selector-agnostic: it receives the profile at call time and never hardcodes live selectors.

Profile fields:

| Field                   | Meaning                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| `horseNumberClassToken` | Class-token prefix of the horse-number cell                            |
| `gateClassToken`        | Class-token prefix of the gate cell (token + digit in the live markup) |
| `horsePathSegment`      | Path fragment that precedes the horse identity in href values          |
| `jockeyPathPrefix`      | Path prefix that precedes the jockey identity in href values           |
| `trainerPathPrefix`     | Path prefix that precedes the trainer identity in href values          |
| `affiliationLabels`     | Non-empty list of affiliation label strings recognized in each row     |

Do not commit a real profile. Do not paste live selectors into tests, docs, or chat logs that will be archived in the public tree.

### Source result profiles

`src/sources/secondary-result-parser.ts` also parses cached horse, jockey, trainer, and owner result tables. Its table marker, identity-route prefixes, URL template, and cell indexes are supplied by an operator-owned ignored profile. The parser does not contain live source markup.

`src/sources/secondary-pedigree-parser.ts` parses cached pedigree AJAX JSON only; it does not perform network requests. The operator-owned ignored profile supplies the pedigree table marker, horse identity route prefix, and source URL template. The stored output keeps the source-native IDs and spellings for sire, sire-sire, dam, and dam-sire so later JV name matching remains auditable.

Horse rows map to `oversea_horse_race_history` with explicit source provenance. Person rows map to `oversea_person_race_history`; a missing source horse link remains null and the published horse name is retained. Empty source fields are never replaced with synthetic JV identifiers.

## Resumable history collection

For many entities (an overseas meeting's runners, past editions and connections), queue them with
`scripts/collect-queue.sh BASE_DIR QUEUE_FILE [PAGE_BUDGET]`. Each queue line is `kind id`, with the plan at
`BASE_DIR/plans/<kind>-<id>.json`. It collects serially, records `done`/`blocked` in `BASE_DIR/state.txt`,
logs to `BASE_DIR/collect.log`, and skips finished entities, so rerun the same command after a reboot.
Keep `BASE_DIR` outside temporary directories.

From this package directory:

```sh
bun run history collect /private/history-plan.json /private/history-snapshot 50
bun run history status /private/history-snapshot
bun run history audit /private/history-snapshot
```

The acquisition and audit implementation is in this package (`history-cli.ts`,
`history-proxy.ts`, and `history-audit.ts`), not temporary scripts. Paths are CLI
arguments; no user home, temporary directory, session ID, or source identity is
hardcoded. Keep private plans and evidence in a persistent directory outside
version control, for example `$HOME/.local/share/horse-racing-data/history/`.

Netkeiba history requests always use the existing production proxy's
`/playwright` HTTP endpoint. This endpoint owns its cache and browser fallback;
the CLI itself does not launch a browser or connect directly to netkeiba. Other
sources retain their explicit original encoding and transport. Proxy bytes are
UTF-8 even when the source plan specifies EUC-JP. Redirects, non-200 responses,
non-HTML responses and invalid UTF-8 stop collection without direct fallback.
The exact response, SHA-256, target/transport URLs, HTTP status and metadata are
preserved before validation. Exclusive `.raw.pending.json` reservations prevent
concurrent requests and blind replay after an unknown outcome. Existing raw
files or metadata are never overwritten. Inspect and retain failed/in-flight
evidence before an explicitly authorized new attempt; do not clear it to force
an automatic retry.

`audit` is offline: it checks raw-byte SHA/provenance, reparses every committed
page, checks the published total and pagination chain, and compares all rows
(including duplicate occurrences and order) with the checkpoint. It supports
both genuine archived proxy POST envelopes and proxy GET responses. It refuses
missing or unsupported provenance, changed originals, and an archive/plan that
changes during the audit. Run it after collection settles. Exit `2` means the
verified snapshot is still partial, not that the remaining pages were acquired.
It never fetches, updates the checkpoint, promotes another snapshot, or publishes
to databases. Preserve its JSON output as a separate immutable audit receipt.

Run only one collector per snapshot directory. Re-run the same command to resume: completed pages and archived responses are reused rather than cold-refetched. A completed snapshot is immutable evidence, not a claim that the source has no newer results; refreshing a changing source requires a separately reviewed snapshot/baseline workflow.

The private plan requires `kind` (`horse`, `owner`, `jockey`, or `trainer`), the observed `sourceId` and HTTPS `initialUrl`, explicit `encoding`, `initialHtmlPath` (null or a normal-browser HTML export), and `profile`. The profile contains `markup` (the result parser profile described above), `populationPattern` (a regular expression capturing the source-declared total), `nextLabels`, and `emptyMarker`, plus optional `uncountedFinishTexts`: listed finish texts that the source itself leaves out of its published total, such as scratched entries. Those rows are still archived and published. Use only observed public navigation and actual markup. Do not infer totals from the number of fetched rows, guess endpoints, or bypass login/paywall restrictions.

The collector archives raw bytes, fetch metadata, decoded HTML, and an atomic parsed-row/checkpoint file under the private directory. HTTP redirects/access failures, parse failures, changing totals, pagination loops, or a terminal count mismatch do not count as success. An explicit source zero is distinct from missing data and from zero real-world starts. A configured browser export is preferred for its initial page; it does not authorize automated access-control bypasses.

Exit codes: `0` = archival completion, `2` = bounded chunk paused, `1` = blocked/failure. Reports deliberately say `databasePublished: false`. `status` describes checkpoint completion, not freshness or the last acquisition error; retain the collection report when diagnosing blocked pages.

### Incremental local mirroring while acquisition continues

```sh
# From the repository root; credentials are loaded privately, never printed.
bun --env-file=.env --env-file=apps/local-postgresql/.env run \
  apps/save-oversea-keiba-records/src/history-main.ts mirror-local \
  /private/history-snapshot /private/local-receipts/pass-1 --confirm-write
# After more pages have arrived, use a NEW receipt directory:
bun --env-file=.env --env-file=apps/local-postgresql/.env run \
  apps/save-oversea-keiba-records/src/history-main.ts mirror-local \
  /private/history-snapshot /private/local-receipts/pass-2 --confirm-write
```

Create the receipt parent directory first. `mirror-local` freezes the current
atomic archive and plan into an exclusive new directory without stopping or
modifying the collector. It accepts only a loopback PostgreSQL endpoint, using
`OVERSEA_HISTORY_LOCAL_DATABASE_URL` or the explicit `POSTGRES_PORT`,
`POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` values. It never connects
to production or writes to Catalog. Each bounded 250-row transaction reselects
only missing rows, inserts without updates, and checks every submitted column.
A separate post-commit readback precedes the final receipt. Natural-key index
predicates accelerate the checks without removing any null-safe column comparison.

Existing receipt directories cannot be replayed. On failure, retain the frozen
input and chunk journals and inspect the database before a new pass; a later
pass reselects actual missing rows rather than assuming failed work rolled back.
Conflicting existing records fail closed by default. After inspecting a conflict,
append `--preserve-existing-variants` to a new `mirror-local` pass to retain those
existing DB records and record the excluded source variants in chunk journals
while inserting other non-conflicting rows. `existingConflictingRows` reports
these exceptions, and canonical completeness stays false. This option never
updates existing records or treats excluded variants as inserted. Genuine
source-partial rows stay in the frozen archive. A successful local mirror is not full source completion:
check `sourceComplete` and `canonicalCoverageComplete`. Newly acquired rows are
picked up by a subsequent pass, with their own frozen input and receipts.

### Explicit production history mirroring

```sh
# Load OVERSEA_HISTORY_PRODUCTION_DATABASE_URL privately, with explicit TLS.
bun run history mirror-production /private/history-snapshot \
  /private/production-receipts/new-pass --confirm-production
```

Production uses the same 250-row insert-only transactions and independent
post-commit all-column readback as local mirroring, but requires its own command,
confirmation, explicit database URL and target-bound receipts. It never falls
back to local credentials. After inspecting existing conflicting records,
`--preserve-existing-variants` retains those records and journals the source
variants without falsely reporting canonical completeness. This command publishes
PostgreSQL only; Catalog and raw R2 preservation are separate verified stages.

### Past overseas editions from cached result pages

```sh
OVERSEA_RACE_RESULT_PROFILE_PATH=/private/race-result-profile.json \
  bun run past-edition /private/past-results/<page>.html \
  --source-race-id '<secondary-race-id>' --race-name '<JV race title>' \
  --venue-code A4 --race-number 09 [--confirmed-field-size N] [--apply]
```

JV often carries only the Japanese runners of an overseas race that JRA did not
sell. This command reads a result page already fetched through the proxy and
parses it with a private profile. The profile holds the table marker, route
prefixes, date and course patterns, trainer badges and cell indexes, and must
not be committed. The command uses local PostgreSQL only and runs as a dry run
unless `--apply` is given. It inserts `jvd_ra` only when the race key is absent.
It inserts `jvd_se` only for horse numbers that have no row, so official JV rows
are never rewritten. Listed non-starters (scratched or excluded) are not inserted, so runner rows match the field size, as the race request requires. It stores finish position, JV abnormality code and winner
time. It also adds `oversea_runner_source_id` rows (`ON CONFLICT DO NOTHING`)
for runners whose source page exists. The race key uses the published local race
date, as JV does. Some older source pages list only part of the field. The report
prints `parsedRunners`, `starters` and `sourcePartial`. Overseas sources are normally partial, so a page with horse-number gaps is still stored with the runners it publishes and reported as `sourcePartial: true`; history archives likewise keep fewer listed rows than the published total as a source gap. No runner or row is ever invented, and more counted rows than published still fails closed.

### Event-wide history scope (every overseas race)

Registering an overseas race always covers the race's past editions and every
participant's full pre-race record, not only the current runners. After the
current card and the past editions are stored locally (including their
`oversea_runner_source_id` rows):

```sh
# Lists every horse, jockey, trainer and owner of the meeting's races and of every
# earlier JRA overseas race with the same title (full-width spaces and a grade suffix ignored).
bun run history event-entities 2026 1101 A4 /private/event-entities.txt
# Writes one private history plan per entity that has no plan yet (existing plans are kept).
OVERSEA_HISTORY_PLAN_TEMPLATES_PATH=/private/history-plan-templates.json \
  bun run history event-plans /private/event-entities.txt /private/plans
```

The template file is private like the markup profiles: a JSON object with
`horse`, `jockey`, `trainer` and `owner` entries, each `{ "urlTemplate": "…{ID}…",
"plan": { …the rest of a history plan… } }`. Collect each planned entity with
`history collect`, then `mirror-local`, `mirror-production`, `source-ids-production`
and the Catalog sync as described above. Owners of past runners become known from
the collected horse pages, so rerun `event-entities` / `event-plans` after a horse
pass to add them.

### Scratches (取消)

US post positions shift down when an entry scratches, so a page's 枠順 is the actual
stall and our umaban is the program number. Scratched entries of past editions stay in
jvd_se with `ijo_kubun_code` 1 and never receive a gate. On race day, rerun `save --apply`
with the refreshed JRA card: a horse now shown as 取消 has its stored unplaced row marked
`ijo_kubun_code` 1 and its gate cleared, and the later horses' shifted gates come from the
card. `source-ids-production` copies those gate changes (including the cleared gate) for a
meeting dated today or later (JST); past meetings stay fill-only. Send the changed jvd_se
rows with `production prepare/apply` for each race.

### Past post positions, sex/age and weights from ahonoora.com

netkeiba does not publish post positions for past overseas editions. The
ahonoora.com race-history pages (Shift_JIS) list 着順, 馬番 (newer pages only), 枠順,
性齢 and 斤量 per runner. Fetch is serial with a 2-second interval and keeps pages
already archived. Apply matches each runner by its unique finish (a numeric 馬番 on
the page must also agree; unplaced runners need it). It only fills empty values
(`gate_number` NULL; `seibetsu_code` / `barei` / `futan_juryo` blank or zero). A
differing stored value is kept and reported, and a page that cannot be read safely
(for example a repeated post position) is reported and skipped. Only local is written.

```sh
bun run history post-positions-fetch /private/ahonoora 1996 bcdirtmile bcsprint bcmile bcturf bcclassic
bun run history post-positions-plan /private/ahonoora A4 local
bun run history post-positions-apply /private/ahonoora A4 local --confirm-write
# Production: gates through source-ids-production (fill-only) per meeting, and changed
# jvd_se rows through `production prepare/apply` for each affected race.
```

### Meeting-specific finish predictions

Once the histories are stored locally, one command trains a pooled ranker for every
race of an overseas meeting from the past editions of its titles. It runs a year-by-year
walk-forward over 12 XGBoost rankers and 4 plain pre-race orderings, picks the one with the
best unordered top-1..5 overlap, and writes `evaluation.json` and a `kv-bulk.json` of
`pred:fp:v1:{date}:{venue}:{race}` bodies that the generic finish-prediction section reads.
No odds, popularity or gate features are used. The pick is retrospective on few races.

```sh
# From apps/pc-keiba-viewer with the local PostgreSQL settings (POSTGRES_HOST included).
PYTHONPATH=src/scripts uv run python src/scripts/overseas_event_model.py 20261101 A4 /private/model-out
# Publish the reviewed bodies (72 h TTL) to the viewer's prediction KV namespace.
bunx wrangler kv bulk put /private/model-out/kv-bulk.json --namespace-id <DETAIL_SECTION_CACHE_KV id> --remote
```

### Overseas pedigrees from archived horse pages

The horse pages archived by `history collect` (fetched only through the production
proxy) embed the same two-generation pedigree table as the pedigree response, so
pedigrees are stored without any extra request:

```sh
OVERSEA_PEDIGREE_MARKUP_PROFILE_PATH=/private/pedigree-profile.json \
  bun run history pedigree-plan /private/history local
bun run history pedigree-apply /private/history local --confirm-write
bun run history pedigree-apply /private/history production --confirm-write
```

It inserts only horses missing from `oversea_horse_pedigree`, in one locked
transaction with a readback. A horse whose page has no pedigree table is skipped
(a source gap, never filled). A stored row that differs is kept as-is and only
reported in `conflicts`. Sync `oversea_horse_pedigree` to the Catalog afterwards.

### Race times and closing 3F

```sh
# netkeiba (archived horse pages; measured values only). Private cell indexes:
OVERSEA_RACE_TIME_PROFILE_PATH=/private/race-time.json \
  bun run history race-times-apply /private/history local --confirm-write
# Overseas gaps from the secondary results-by-date pages (private profile; serial
# proxy requests every 6 s, cached days are never refetched; a challenge page stops the run).
# Stored history races of one meeting's runners that still have no time (local, read-only):
bun run history missing-race-times 2026 1103 B6 /private/missing-times.json
OVERSEA_RACING_POST_PROFILE_PATH=/private/rp.json \
  bun run history racing-post-times-fetch /private/missing-times.json /private/racingpost
# Full result pages for runners the day pages do not list (same course and distance only):
bun run history racing-post-races-fetch /private/missing-times.json /private/racingpost /private/history
bun run history racing-post-times-apply /private/missing-times.json /private/racingpost /private/history local --confirm-write
# Official JV runner rows of JRA-sold overseas races (read from local PostgreSQL, measured):
bun run history jv-race-times-apply production --confirm-write
```

The official JRA overseas card (fetched for `save`) lists each current runner's last four
races with that horse's time. Read it first, so JRA is the primary source of the current runners'
times; the card markup lives in a private profile (`runnerStart`, `pastRunStart`,
`horseNumberPattern`, `datePattern`, `finishPattern`, `timePattern`, `lastThreeFurlongsPattern`):

```sh
OVERSEA_JRA_CARD_RACE_TIME_PROFILE_PATH=/private/jra-card-race-time.json \
  bun run history jra-card-race-times-apply /private/cards/jra-07.utf8.html 2026 1103 B6 07 local --confirm-write
```

Runners map to netkeiba horses through the race's local `oversea_runner_source_id` rows, and a
past run fills a stored history row only when exactly one row of that horse has the same date.
The card prints a beaten runner's time as the winner's time plus the margin, so only a winner's
time is `measured`; the others are `estimated` (`race_time_source` = `jra`). The card leaves
closing 3F of overseas races empty, so none is invented.

Both fill only NULL columns and report differing stored values. `race_time_source` and
`race_time_kind` record the origin: a winner's published time is `measured`; a placed
runner's time is `estimated` as winner time + cumulative margin × 0.2 s per length. The day
pages list only the first three finishers; `racing-post-races-fetch` adds the full result pages
of the same course and distance (netkeiba venue labels are mapped to courses in the code), which
list every finisher. A result page whose analysis states the winner's own closing 3f sectional ("Closing 3f sectional: … winner 37.00") gives that winner's closing 3F; nothing else is used, so every stored closing 3F is a published (measured) value and none is estimated. The chart site with sectionals sits behind a
bot challenge and is not used. Sync `oversea_horse_race_history` to the Catalog afterwards.

### Explicit production source-id mirroring

```sh
# Same private environment as mirror-production plus the local POSTGRES_* values.
bun run history source-ids-plan 2026 1101 A4
bun run history source-ids-production 2026 1101 A4 /private/source-id-receipts/new-pass --confirm-production
```

The production Viewer joins overseas runners to their netkeiba histories through
`oversea_runner_source_id`, which the `production` race request does not publish.
These commands copy one meeting's local `source=netkeiba` rows to production.
They insert missing rows and fill only production NULL columns. Any differing
non-null value fails closed and nothing is written. One transaction holds a
table lock, reads the rows back and checks them before commit. `source-ids-plan`
is read-only. The receipt directory must be new.

**Database publication is a separate stage.** `prepareHistoryArchive` validates source scope, preserves genuine missing runner/finish records as `sourcePartialRows`, rejects other invalid canonical rows and conflicting duplicates, and reports archival coverage. `buildMissingHistoryStatements` selects only candidates without an exact all-column database match. `createHistoryDatabase` inserts that delta inside a transaction, verifies both the delta and full canonical input before commit, and reports submitted/inserted/verified counts. Existing conflicting records cause rollback, not overwrite. These APIs do not write JV rows, models, forecasts, or runner mappings. The CLI connects these stages with explicit target selection:

```sh
bun run history prepare /private/history-snapshot local
bun run history apply /private/history-snapshot local --confirm-write
```

For production, use `production` instead of `local`. Set the corresponding private environment variable `OVERSEA_HISTORY_LOCAL_DATABASE_URL` or `OVERSEA_HISTORY_PRODUCTION_DATABASE_URL` outside shell history/logs. Production requires an explicit TLS mode (`require`, `verify-ca`, or `verify-full`). Connections have bounded connection/statement timeouts and are closed after each operation.

Preparation is read-only against the database; it writes a private `database-prepared.json` binding the exact plan/archive bytes and target configuration. Review its counts before apply. Apply rejects changed artifacts/targets, reselects missing rows within the transaction, and writes `database-receipt.json` only after commit and full readback. A durable receipt prevents accidental apply replay. If commit succeeded but receipt persistence failed, retry safely reselects missing rows instead of blindly resubmitting the old batch. Keep separate publication directories for separate database targets; reuse the immutable plan/archive files rather than refetching source pages.

An operation's `status: complete` means that operation finished, not full source availability. Check `databasePublished`, `sourceComplete`, `sourcePartialRows`, and `canonicalCoverageComplete` together. Records with genuine missing runner/finish evidence remain in the archive and are not invented to satisfy canonical validation. Publication commands do not refresh source snapshots, synthesize statistics, or activate forecasts.

Keep plans, fetched markup, credentials, source-partial evidence and database receipts outside version control. Never treat archived counts, queue acceptance, a successful HTTP response, or partial canonical publication as complete coverage.

## Data flow

1. Load both documents concurrently, preferring the supplied local files and otherwise making one HTTP request per source.
2. Parse the official JRA card or the explicitly selected JRA-VAN World card and the secondary source.
3. Reconcile runners by horse number, never by row order. If a preliminary secondary card has not published horse numbers, use only a unique exact horse-name match after NFKC normalization and whitespace removal. Duplicate or unmatched names remain unresolved. JRA/JRA-VAN data is authoritative for descriptive fields; the secondary source contributes only horse, jockey, and trainer entity identifiers. The dry-run report prints every accepted horse-name-to-secondary-ID mapping for operator review.
4. Verify entity identifiers against the local JV horse, jockey, trainer, and owner masters.
5. **Numeric-only master backfill (option 1):** when a secondary id already has a valid JV primary-key shape (pure ASCII digits, exact width) and that code is absent from the local master, plan an insert of a minimal overseas-visitor master row (`jvd_um` / `jvd_ks` / `jvd_ch`). Never mint synthetic or alphanumeric keys. Never UPDATE or DELETE existing masters. Owner master (`jvd_bn`) is **not** inserted (secondary identity has no reliable 6-digit owner code; name-only resolution only). Placeholders (`0000000000` / `00000`) are never inserted. Alphanumeric secondary ids stay unresolved and race rows keep zero placeholders as before.
6. Map the reconciled race to complete `jvd_ra` and `jvd_se` rows (entity resolution treats planned master inserts as present so race rows use the real codes after apply).
7. Compare the proposed runner rows with the current database state. Dry-run also prints `=== Master backfill (numeric-only) ===`.
8. Write only when `--apply` was supplied and the safety gate is `safe`. Masters are inserted first inside the same transaction (`INSERT … ON CONFLICT DO NOTHING`), then `jvd_ra` / `jvd_se`, then `oversea_runner_source_id` rows for secondary-source IDs (`source=netkeiba`). `oversea_runner_identity` is not overwritten (that table remains the jra-van display-name row). Dry-run prints planned source-ID upserts (`umaban` + `source_horse_id` present/absent) without writing.

## Published fields stored in JV columns

The mapper persists every published field that has a real JV home and a verified encoding:

| Published field       | JV column                   | Encoding / notes                                                                                                                           |
| --------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Runner count          | `jvd_ra.shusso_tosu`        | Two-digit zero-padded field size. `toroku_tosu` stays `00` to match real overseas JV rows.                                                 |
| Start time (JST)      | `jvd_ra.hasso_jikoku`       | Published JST `HH:MM` encoded as four zero-padded digits (`23:35` → `2335`). Missing/unparseable → `0000`. Local start time is not stored. |
| Coat colour           | `jvd_se.moshoku_code`       | Standard JV two-digit coat codes (e.g. 鹿 → `03`).                                                                                         |
| Gate / stall number   | `jvd_se.wakuban`            | Published gate (JRA 「ゲート」) for gates 1–9. Column is `varchar(1)`, so gate ≥ 10 falls back to `0` (no silent truncation).              |
| Win odds              | `jvd_se.tansho_odds`        | Odds × 10 in four zero-padded digits (1.6 → `0016`). Null/overflow → `0000`.                                                               |
| Popularity            | `jvd_se.tansho_ninkijun`    | Two-digit rank. Null/overflow → `00`.                                                                                                      |
| East/west affiliation | `jvd_se.tozai_shozoku_code` | From master resolution (`4` = overseas), not from free-text trainer country.                                                               |

### Known column limitations

| Limitation   | Detail                                                                                                                                                           |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Gate ceiling | `jvd_se.wakuban` is `varchar(1)`. Overseas fields with 10+ runners cannot store gate numbers above 9 here without a schema change; those gates fall back to `0`. |

### Published but not storable in `jvd_ra` / `jvd_se`

| Published field                       | Why it is not stored                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Local start time                      | Only the published JST start time is written to `hasso_jikoku`.                                                                                                                                                                                                                                                                      |
| Form record (e.g. `10.5.1.1`)         | No form-string column on `jvd_ra` / `jvd_se`.                                                                                                                                                                                                                                                                                        |
| Sire / dam / damsire                  | Pedigree lives on master tables (`jvd_um`). When the secondary horse id is already JV-shaped (10 digits) and missing locally, option-1 numeric-only backfill inserts a minimal `jvd_um` row with sire/dam/damsire names from the JRA card. Alphanumeric secondary ids stay on `ketto_toroku_bango='0000000000'` (no synthetic keys). |
| Trainer country (FR / IRE / GB / JPN) | Not a separate JV code on `jvd_se`; only east/west affiliation is stored.                                                                                                                                                                                                                                                            |

## Safety gate

Before any write, the dry-run diff classifies each compared column as `unchanged`, `enriched`, `changed`, or `REGRESSION`; new and database-only runners are reported separately. A `REGRESSION` means a real stored value would be replaced by a blank or all-zero placeholder. That is treated as data destruction, so the command aborts without writing even when `--apply` was supplied.

There is no bypass flag.

## Idempotency and non-destructive updates

For `jvd_se`, an incoming real `ketto_toroku_bango` first promotes an existing all-zero placeholder-key row for the same race and horse number with a parameterized `UPDATE`. The runner is then upserted on the complete primary key. If an incoming key is still the placeholder while a real-key row already exists, the placeholder upsert is skipped so a second runner row is not created.

If a stored real `ketto_toroku_bango` differs from the incoming real key for the same race and horse number, or a placeholder row and a real row both already exist for that horse number, that runner is **not written**. The conflict is logged in English (race key, umaban, stored key, incoming key), and the process exits non-zero. Other non-conflicting runners in the same batch may still be written. There is no automatic merge or delete; a genuine identity conflict is a human ops decision.

Both `jvd_ra` and `jvd_se` upserts preserve stored data. For every non-key column, an incoming value that is blank after trimming ASCII and ideographic spaces, or consists only of zeros, keeps the existing table value; only a substantive incoming value replaces it. Re-running the same command therefore does not add duplicate race or runner rows and does not clobber enriched values with placeholders.

## Testing

```sh
bun run --filter save-oversea-keiba-records test:coverage
```

The Vitest configuration requires at least 95% statements, branches, functions, and lines coverage.

## Targeted production requests (existing Worker authority)

`production` prepares and submits one overseas race through the existing
`daily-keiba-sync` staging → Catalog → Neon pipeline. It does not deploy Workers,
copy the local database, bypass the legacy-replica guard, advance the daily cursor,
or activate prediction models.

Prerequisites: Bun, authenticated Wrangler, and an authenticated `cf` CLI (D1 queries
and the Catalog queue push go through `cf`; R2 staging through Wrangler). The CLI never
logs in or approves anything. Supply these non-secret resource settings
through the environment (and the local PostgreSQL settings above):

| Variable                         | Purpose                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------- |
| `CLOUDFLARE_ACCOUNT_ID`          | Explicit account; pinned on every `cf` call so no other account receives writes |
| `PC_KEIBA_SYNC_DATABASE_ID`      | Current daily-sync D1 database ID                                               |
| `PC_KEIBA_CATALOG_QUEUE_ID`      | Current Catalog queue ID, not its name                                          |
| `PC_KEIBA_SOURCE_STAGING_BUCKET` | Current daily-sync source-staging bucket name                                   |

From this app directory, with the environment already loaded:

```sh
bun run production prepare /private/operator/race-request 2026 0919 A4 05
# Review production-input.json and production-plan.json before proceeding.
bun run production apply /private/operator/race-request --confirm-production
bun run production status /private/operator/race-request
```

- `prepare` only reads the selected local race and writes owner-only local artifacts.
  It refuses to overwrite a durable request. Complete source column layouts, one
  overseas race, distinct runners and the declared field size must agree.
- `apply` requires explicit confirmation and an unchanged plan. It checks the
  managed account, uploads both immutable stages, downloads and verifies their
  SHA-256 digests, registers a manual run with `advance_cursor=0`, then submits
  exactly two Catalog jobs. Existing Worker leases/indexes and Neon mirroring
  remain authoritative. No other date or table is requested.
- Queue acceptance is **not publication**. Use `status` and require both tables
  to report `catalog_status=succeeded` and `neon_status=succeeded`, with the run
  itself succeeded. Verify actual production rows and the authenticated UI.
- A local acceptance receipt prevents duplicate submissions. If an operation
  fails between registration, queue submission and receipt persistence, inspect
  `status` and the existing run before any recovery. Do not delete the receipt,
  regenerate a run ID, or blindly retry `apply`.
- These requests publish only `jvd_ra` and `jvd_se`. Local source-ID and master
  backfills are not silently replicated to production. Missing genuine JV horse
  numbers remain unknown; never invent a numeric identifier to satisfy a UI filter.

### Prediction publication contract

`src/prediction-publication.ts` exports `buildPredictionStatements` and
`publishPredictions` for an approved database transaction adapter. Supply the
validated race request, the complete prediction JSON array, and one generation
timestamp. It checks all runner identities, unique ranks, finite scores and
probability ranges before opening the transaction. All statements are scoped to
`overseas-lgbm-fp-v3`, source `overseas`, and the selected race. The module performs
no DDL, deletes, master writes, or active-model changes. `UMABAN_XX` is permitted
only as a prediction-row identity for a runner whose real JV number is unknown;
it is never written to JV source/master tables.

The adapter must verify the registered production field, execute the complete
generation atomically, and read back all runners before committing. The current
Viewer reads these predictions from Neon; Catalog success alone is insufficient.
A Viewer that filters out zero JV horse numbers can still hide legitimately
registered overseas runners. Treat that as a Viewer compatibility blocker, not
as permission to fabricate identifiers or deploy unrelated working-tree changes.
