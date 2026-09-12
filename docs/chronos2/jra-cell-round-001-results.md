# JRA cell round 001 — rejected after fixed later-year evaluation

- 6 count-selected cells × 16 actual head/LoRA training trials = **96 completed trials**.
- Training labels: 2020–2022; development: 2023. Independent cell-local Rustuna studies.
- Primary metric: exact finishing-position matches at ranks 1–5, not winner-in-TopK recall.
- Three development selections were frozen before the new later-year readout. No post-readout retuning.
- 2024–2026 were already observed in earlier experiments; this is not an untouched holdout.

| Cell                        | Development delta vs market | 2024–2026 aggregate delta               | Decision     |
| --------------------------- | --------------------------- | --------------------------------------- | ------------ |
| `jra-cell-e87801b09f41533a` | +1, +2, +2, 0, +4           | −4, −2, +2, +2, +1                      | Reject       |
| `jra-cell-a44a08d57b5d7bdb` | +1, +2, 0, +3, 0            | −4, −2, −2, −1, −1                      | Reject       |
| `jra-cell-ce91aa24c61fb77f` | +2, +1, +1, 0, 0            | 0, −3, −3, −2, −1                       | Reject       |
| `jra-cell-1fe5c25bbbff7342` | No selection                | Market retained                         | No candidate |
| `jra-cell-c6cb49456ebfe04e` | No selection                | Market retained                         | No candidate |
| `jra-cell-6fb3d077b4600c0d` | No selection                | Market retained; zero 2026 target races | No candidate |

The fixed readout covered **358 races**, retaining sparse-history market fallback and explicit per-rank supports. No candidate satisfied the annual non-regression conditions. The exact current production incumbent was unavailable; market comparison must not be described as incumbent comparison.

## Data audit and reboot recovery

Persisted selected model hashes were verified after the Mac reboot. The development freeze SHA-256 is `dbbd95bcdc7ed0ac0b1a35b3b4fbc8f8a16dbab63c09b3abddfba9c5fa1301a9`.

Later-year context contains 16,346 rows, including 390 strictly prior NAR observations. One partial overseas JVD race (`jra:2024:1101:A4:09`) incorrectly used two recorded runners as its denominator. Official RA metadata reports 10 starters. Only `field_size` and `performance_rating` were repaired using that count; the original file and source metadata were retained. No domestic target race was removed, and other partial-race features are not certified or consumed by this scalar experiment.

Combined history SHA-256: `a9ad5dc2758b478a5871821cc644ec876eb538eefb68a7f48da8c13235ad4bbe`. Duplicate horse/dates and invalid ratings: zero.

Artifacts: `.cache/chronos2/cell-improvement/real-round-001/`, including the development freeze, source snapshot, trial records/weights, validation input provenance, CPU point arrays and annual reports. `fixed-validation-summary.json` contains all six results.

Verification: **304 tests passed**, total coverage **96.78%**, validation module **94%**, history repair **100%**; Ruff, basedpyright and scoped Chronos ty passed. Unrelated full-package ty diagnostics remain outside this claim.

Nothing was published, deployed or used to overwrite production predictions. Independent NAR-flat and Ban-ei experiments remain separate work, not evidence of a JRA improvement.
