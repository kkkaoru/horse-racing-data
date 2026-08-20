# Neon primary read-only is `source=session` (2026-08-16)

Recorded 2026-08-16 03:18 JST by `oversea-horse-race` from advisor's
direct `pg_settings` / `pg_db_role_setting` read. No writes were made
during that inspection. Do not treat this note as a deploy or schema
change.

## Finding

Neon `NEON_PRIMARY_URL` was **not a replica** and **not a database/role
default**. The read-only flags were inherited from a reused pooler
session.

| Check                            | Result                                                    | Meaning                                                 |
| -------------------------------- | --------------------------------------------------------- | ------------------------------------------------------- |
| `pg_is_in_recovery()`            | `false`                                                   | This endpoint is primary, not a replica                 |
| `transaction_read_only`          | `on`, `source=session`                                    | Current txn is read-only because a session SET remained |
| `default_transaction_read_only`  | `on`, `source=session`                                    | Same session contamination, not a persisted server GUC  |
| `pg_db_role_setting` on `neondb` | empty                                                     | No `ALTER DATABASE` / `ALTER ROLE` default exists here  |
| `pg_db_role_setting` on `mlflow` | `(None, 'mlflow', ['default_transaction_read_only=off'])` | Only the MLflow DB has an explicit off default          |
| Connected role / DB              | `neondb_owner` / `neondb`                                 | The production write path's database                    |

`source=session` means someone issued `SET` on that backend session and
the txn-mode pooler later reused the same backend. A new client
connection can still inherit `default_transaction_read_only=on`.

This is the same class of failure recorded for 2026-08-10:

> Txn-mode pooler can inherit `default_transaction_read_only=on` on the
> primary.

## Why this matches tonight

- Running-style Neon writes (`sync-realtime-data`) already use
  `withWritableClient`: `BEGIN` → `SET TRANSACTION READ WRITE` →
  `SHOW transaction_read_only` must be `off` → DML → `COMMIT`.
  2026-08-16 RS reached 80/80 / 940 rows.
- Finish-position Neon writes are owned by the predict-container Python
  upsert path (`predict_upcoming.py` `execute()` / `upsert_sql.py`).
  Production image at 03:14 JST did **not** yet have the equivalent
  belt.
- Advisor reproduced `SET TRANSACTION READ WRITE` flipping
  `transaction_read_only` from `on` to `off` on the current primary.
  Connection-string `options=-c default_transaction_read_only=off` was
  not validated (IPv6 path failed from the advisor host).
- Canary `2026/08/16/04/01` returned HTTP 200 `accepted` at 02:57 JST
  but Neon stayed at **8 races / 87 rows**, latest
  `2026-08-15T17:35:51Z`, with `04/01 = 0` through at least 03:15 JST.
- A later debug POST for `04/02` returned `status=busy`,
  `racesPredicted=0` in 0.3s. Accepted enqueue is not proof of a
  writable upsert.

## Correct fix

Per-transaction overwrite is the correct fix. Do **not** RESET a
session GUC on a txn-mode pooler.

```text
BEGIN
SET TRANSACTION READ WRITE
SHOW transaction_read_only   -- must be off, else refuse DML
INSERT / UPSERT
COMMIT
```

`apps/sync-realtime-data/src/neon-writable-client.ts` is the reference.
The same sequence belongs in the container upsert path, including the
reconnect retry. Session-level `SET` + `RESET` is unsafe with Neon
transaction pooling.

## What not to do tonight

These remain forbidden without a later explicit user approval:

- `secret put` / endpoint swap
- `ALTER ROLE`
- `ALTER DATABASE neondb SET default_transaction_read_only = off`

The last item is a plausible durable follow-up because `mlflow` already
has that default. It is **not** tonight's recovery action. The session
contamination is intermittent; yesterday's 68-race write succeeding
does not prove the server default is healthy.

## Operational stop conditions

Stop and investigate instead of enqueueing more races when any of these
hold:

- writable probe fails
- `SHOW transaction_read_only` stays `on` after `SET TRANSACTION READ WRITE`
- SQLSTATE `25006` / `ReadOnlySqlTransaction` persists
- admin path returns `accepted` or `busy` while Neon row counts do not
  move

Replaying more focused races does not repair a read-only session.
