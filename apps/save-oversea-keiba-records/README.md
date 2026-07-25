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
- `--jra-file <path>`: read the JRA card from a local file and skip its HTTP request.
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

## Data flow

1. Load both documents concurrently, preferring the supplied local files and otherwise making one HTTP request per source.
2. Parse the JRA card and the secondary source.
3. Reconcile runners by horse number, never by row order. JRA data is authoritative for descriptive fields; the secondary source contributes only horse, jockey, and trainer entity identifiers.
4. Verify entity identifiers against the local JV horse, jockey, trainer, and owner masters.
5. **Numeric-only master backfill (option 1):** when a secondary id already has a valid JV primary-key shape (pure ASCII digits, exact width) and that code is absent from the local master, plan an insert of a minimal overseas-visitor master row (`jvd_um` / `jvd_ks` / `jvd_ch`). Never mint synthetic or alphanumeric keys. Never UPDATE or DELETE existing masters. Owner master (`jvd_bn`) is **not** inserted (secondary identity has no reliable 6-digit owner code; name-only resolution only). Placeholders (`0000000000` / `00000`) are never inserted. Alphanumeric secondary ids stay unresolved and race rows keep zero placeholders as before.
6. Map the reconciled race to complete `jvd_ra` and `jvd_se` rows (entity resolution treats planned master inserts as present so race rows use the real codes after apply).
7. Compare the proposed runner rows with the current database state. Dry-run also prints `=== Master backfill (numeric-only) ===`.
8. Write only when `--apply` was supplied and the safety gate is `safe`. Masters are inserted first inside the same transaction (`INSERT … ON CONFLICT DO NOTHING`), then `jvd_ra` / `jvd_se`.

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
