# NAR / Ban-ei vs JRA on the weight-rescore path (pre-12:04)

Read-only. No deploy. Written **before** NAR weights so either
outcome has a list, not a story invented after the fact.

Weight trigger for all three is the same function:
`triggerRescoreAfterWeights` → POST `/api/internal/rescore-race` →
`sendRescoreRaceMessage` → `PREDICT_QUEUE` →
`processContainerPerRaceRescore` (held `/predict` `mode=rescore`).

`RESCORE_ENABLED` is `"1"`. That path **does not read**
`RESCORE_CATEGORIES`. `COORDINATOR_ENABLED` is `"0"`, so the T-25
tick is off for everyone. Yesterday D1 already had nar ok=20 and
ban-ei ok=12. Category is not a trigger allowlist.

## Same for JRA / NAR / Ban-ei

- Race-key parse only maps `jra:*` → jra, `nar:*:83|65` → ban-ei,
  other `nar:*` → nar. Then the same POST body shape.
- No `deliveryTrackingId`.
- `CONTAINER_PER_RACE_CATEGORIES` is `{jra, nar, ban-ei}`.
- Same held `stub.fetch` + `parseNdjsonStream`. No app abort.
- Same Neon table and the same undeployed writable-txn gap.
- Same live DuckDB 1.5.3 in the image.

If NAR also posts with an unchanged `prediction_generated_at`, the
stall is **not** JRA-only code. It is this shared consume / held
fetch / write belt.

## Different (use only if NAR moves and JRA did not)

| difference                    | what it is                                                                               | can it explain a Neon write?                                                                              |
| ----------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Clock                         | JRA trigger 09:10, NAR ~12:04, Ban-ei ~13:54                                             | Maybe load / queue depth. Not a branch.                                                                   |
| DO name                       | `predict-{category}[-shard]`                                                             | **Yes, isolation.** A wedged `predict-jra` need not block `predict-nar`.                                  |
| Feat-cache inventory at 08:25 | NAR+Ban-ei **44/44 HIT** 07:48–07:50; JRA sparse                                         | **Only after consume starts.** HIT is seconds; MISS is `LAYER_CHAIN`. Last-Modified still 07:49 ≠ unread. |
| `LAYER_CHAIN` length          | JRA **17** / NAR **10** / Ban-ei **7**                                                   | Only on MISS. Shorter, not zero.                                                                          |
| Pedigree SQL                  | JRA `--target-race` + `INNER JOIN jra_um` + `MIN_RACES=5`. NAR `LEFT JOIN nar_um/nar_nu` | **Quality of a written row**, not whether a row is written.                                               |
| Coordinator allowlist         | live `"nar,ban-ei"`                                                                      | Unused while coordinator is off. Weight path ignores it.                                                  |
| Source tables                 | `jvd_se` vs `nvd_se`                                                                     | Past the trigger once D1 `fetch-weights` is ok.                                                           |
| Model family                  | market-free JRA vs NAR/Ban-ei champions                                                  | UPSERT target is the same table.                                                                          |

Do **not** treat a NAR success as “pedigree JOIN was the stall”.
That JOIN does not sit on enqueue or on the Worker wait.

Best NAR-success explanations that stay on this list: **different
DO**, **later clock / emptier queue**, or **HIT vs MISS after
consume actually started**. None of those are proven for JRA
today.

Best NAR-failure explanation: the shared belt.

Observe only the three questions on
`nar-banei-1204-observe-checklist-20260816.md`. Then pick a row
from this page. Do not add a tenth hypothesis.

No production change.
