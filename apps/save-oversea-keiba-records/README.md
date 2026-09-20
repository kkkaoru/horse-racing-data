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

From this package directory:

```sh
bun run history collect /private/history-plan.json /private/history-snapshot 50
bun run history status /private/history-snapshot
```

Run only one collector per snapshot directory. Re-run the same command to resume: completed pages and archived responses are reused rather than cold-refetched. A completed snapshot is immutable evidence, not a claim that the source has no newer results; refreshing a changing source requires a separately reviewed snapshot/baseline workflow.

The private plan requires `kind` (`horse`, `owner`, `jockey`, or `trainer`), the observed `sourceId` and HTTPS `initialUrl`, explicit `encoding`, `initialHtmlPath` (null or a normal-browser HTML export), and `profile`. The profile contains `markup` (the result parser profile described above), `populationPattern` (a regular expression capturing the source-declared total), `nextLabels`, and `emptyMarker`. Use only observed public navigation and actual markup. Do not infer totals from the number of fetched rows, guess endpoints, or bypass login/paywall restrictions.

The collector archives raw bytes, fetch metadata, decoded HTML, and an atomic parsed-row/checkpoint file under the private directory. HTTP redirects/access failures, parse failures, changing totals, pagination loops, or a terminal count mismatch do not count as success. An explicit source zero is distinct from missing data and from zero real-world starts. A configured browser export is preferred for its initial page; it does not authorize automated access-control bypasses.

Exit codes: `0` = archival completion, `2` = bounded chunk paused, `1` = blocked/failure. Reports deliberately say `databasePublished: false`. `status` describes checkpoint completion, not freshness or the last acquisition error; retain the collection report when diagnosing blocked pages.

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

Prerequisites: Bun, authenticated Wrangler, and an already-authorized Executor
`cloudflare-api.user.<profile>` integration. Authorization prompts require operator
approval; the CLI never approves them. Supply these non-secret resource settings
through the environment (and the local PostgreSQL settings above):

| Variable                         | Purpose                                                                               |
| -------------------------------- | ------------------------------------------------------------------------------------- |
| `CLOUDFLARE_ACCOUNT_ID`          | Explicit account for Wrangler; checked against the managed API account before staging |
| `PC_KEIBA_CLOUDFLARE_PROFILE`    | Existing Executor Cloudflare user-profile name                                        |
| `PC_KEIBA_SYNC_DATABASE_ID`      | Current daily-sync D1 database ID                                                     |
| `PC_KEIBA_CATALOG_QUEUE_ID`      | Current Catalog queue ID, not its name                                                |
| `PC_KEIBA_SOURCE_STAGING_BUCKET` | Current daily-sync source-staging bucket name                                         |

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
