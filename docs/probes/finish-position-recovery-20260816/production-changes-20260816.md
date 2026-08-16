# Production changes on 2026-08-16

Do not say “本番操作ゼロ”. One write happened.

## What changed

**R2 `pc-keiba-features-archive` feat-cache PUT, ~07:50 JST.**
Record: `feat-cache-healthy-seed-0816.md`. Script:
`put-healthy-feat-cache-0816.sh`. **80 / 80** keys:

`feat-cache/catalog-v1/{jra|nar|ban-ei}/20260816/{keibajo}/{bango}/features.parquet`

Source: host full-day parquet (`feat-jra-layer-16`,
`feat-nar-v7-final`, `feat-ban-ei-v7-final`), split per race.

This is a **production object overwrite**. `mode=rescore` reads
these keys. It is not a no-op.

Two keys already existed (dead-pedigree JRA HITs). Those were
replaced. Copies kept in-repo (do not delete):

- `r2-hit-before-overwrite-0816/jra-04-12.parquet`
- `r2-hit-before-overwrite-0816/jra-07-01.parquet`

The other 78 were new objects (MISS → object). Still a production
write.

pi-fix-developer did **not** run the PUT. This session’s writes
are docs + read-only probes (D1 SELECT, Neon SELECT, R2 HEAD,
`containers list`).

## What did not change

| surface                          | 08-16                                                                                                     |
| -------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `finish-position-cron` deploy    | no. Live `0c76062e`                                                                                       |
| viewer deploy                    | no. Live `06fd3c24`                                                                                       |
| secret put / `DAY_BASE_SPLIT`    | no                                                                                                        |
| queue pause / unpause / settings | no                                                                                                        |
| D1 migration apply               | no. 0006 still unapplied                                                                                  |
| Neon DML from operators          | no. 80/940 is the overnight host one-shot plus later **pipeline** UPSERTs (`01/02` 10:54), not a hand SQL |
| `replica:push` / catalog PUT     | not in this stall track                                                                                   |
| container start / stop / destroy | no (`list` / `instances` only)                                                                            |

## How to say it

Wrong: 本番操作ゼロ.
Right: **コードと設定は触っていない。feat-cache 80 件は PUT した。**
