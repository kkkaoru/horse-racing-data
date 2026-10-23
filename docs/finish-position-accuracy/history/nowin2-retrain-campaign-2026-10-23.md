# -nowin2 retrain campaign (2026-10-23)

Released in 04f4505c / 2b580f07 / bd18151a; artifacts uploaded to R2 and
finish-position-cron deployed on 2026-10-23.

## Retrain tooling fixes

The retrain scripts run from the git-ignored `.cache/tansho-ban-retrain/`
campaign folder. The fixed `retrain.py`, `rs_retrain.py` and
`train_transformer.py` are tracked in
[`nowin2-retrain-campaign-2026-10-23-scripts/`](nowin2-retrain-campaign-2026-10-23-scripts/);
copy them into the campaign folder to start a new campaign. The fixes:

- `retrain.py` loaded feature-store years only through `train_end - 1`, so
  models whose recorded window reaches into 2026 (ban-ei, some NAR cells)
  were trained on data 5-8 months stale. It now loads through
  `max(train_end - 1, end year of every job's training_window)`.
- `retrain.py` looked up a NAR cell's routing scope by the exact
  `model_version`; after the `-nowin1` rename no variant matched and cells
  trained on the whole NAR store (2.7M rows). The lookup now matches
  `variant.model_version.rsplit("-nowin", 1)[0] == old_version`.
- The NAR store drops keibajo 81-84 (Ban-ei raced there in 2005-06), matching
  the builder fix in c1419826.
- `train_transformer.py` filters the same venues; `rs_retrain.py` relabels
  running style from the first non-null corner (bd7040a6), so <=1200m races
  have labels.
- `retrain.py --suffix=-nowin2` must use the `=` form (argparse reads
  `-nowin2` as a flag otherwise).

## Holdout results (2026 races after each training window)

| model                                       | before        | after                   |
| ------------------------------------------- | ------------- | ----------------------- |
| ban-ei cb-v8 / cb-v9 top-1                  | 31.73 / 31.05 | 32.08 / 31.22           |
| NAR 50-sprint-summer cell top-1 (198 races) | 47.98         | 48.48                   |
| NAR iter12 top-1                            | 38.25         | 38.14 (CI -0.46..+0.27) |
| running style JRA calibrated acc (sprint)   | 45.93         | 50.02                   |
| running style NAR calibrated acc (sprint)   | 47.03         | 49.00                   |
