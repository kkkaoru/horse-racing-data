# Overseas race history in domestic finish-position features

Decision date: 2026-08-15

## Decision

Do not add overseas `data_kubun = 'B'` races to the domestic finish-position feature pipeline.

The overseas records remain available for display and source-linked history. They must not be added only at serving time, because doing so would create train/serve skew. Adding them to both training and serving is technically possible, but the measured benefit does not justify retraining, parity validation, cell-level evaluation, and production risk.

## Current behavior

`apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` deliberately limits the JRA category to domestic venue codes `01` through `10`:

- `category_source_filter` near line 391 applies the domestic venue predicate.
- The upcoming/history record selection near line 802 applies the same predicate.

Consequently, an overseas JRA reference race such as the 2026 Prix Jacques le Marois (`keibajo_code = 'A8'`, `data_kubun = 'B'`) is neither a domestic prediction target nor a domestic history record.

This also means that overseas starts by horses with real JV registration numbers, such as Sixpence and Strauss, do not contribute to their later domestic history features.

## Measured impact

A local PostgreSQL measurement on 2026-08-15 found:

| Population                                                           |  Horses |
| -------------------------------------------------------------------- | ------: |
| Horses with a real `ketto_toroku_bango` in `data_kubun = 'B'`        |     713 |
| Horses with a real registration number and domestic JRA history      | 176,404 |
| Domestic-history horses that also have an overseas reference history |     708 |
| Affected share                                                       | 0.4014% |
| Overseas starts belonging to the affected horses                     |   4,401 |

Even under an optimistic assumption that the affected horses improve by 5 percentage points, the pooled upper-bound effect is approximately `0.4014% × 5pp = 0.02pp`. This is far below the established top-1 retraining noise floor of approximately ±0.4pp.

## Rejected implementation

The technically safest candidate was to keep domestic target selection unchanged and union real-registration-number overseas rows into history construction only, behind a default-off flag. It would still require all of the following before production:

1. Rebuild training data with the identical history rule.
2. Retrain the affected models.
3. Validate value-level train/serve parity against a faithful serving store.
4. Evaluate by category/condition cell and across top-1, place-2, and place-3 metrics.
5. Preserve an instant rollback flag.

Because the expected effect is below the measurable noise floor, this work is not approved.

## Reconsideration conditions

Re-evaluate this decision only if at least one condition changes materially:

- the share of domestic horses with overseas histories rises substantially;
- a separately measurable overseas-history cohort or model is introduced;
- a high-value individual-horse workflow explicitly requires the overseas start; or
- an evaluation design can measure an effect below the current retraining noise floor without introducing train/serve skew.

The overseas source mappings and display histories should remain intact regardless of this prediction decision.
