# Arc-only production prediction

Dedicated XGBoost inference for the Prix de l’Arc de Triomphe. This Worker does not replace the general finish-position model or write predictions to PostgreSQL. It scores a frozen, provenance-bound 2026 field on every GET, behind the existing Cloudflare Access hostname. No direct source-site traffic or external inference dependency.

Production verified2026-10-03: Worker version `811ac902-66ac-4b2a-a5ad-8a520fd4d3e5`; all16ranks and both native model scores matched production readback (maximum error5e-16). Invalid Access credentials returned401. Three observed Worker invocations all returned200/ok. Evidence: `.cache/arc-model-20261003/production-verification.json` and `production-observability-verification.json`.

## Routes

```sh
# Run from repository root; uses the existing Access service credentials.
bun run prod get /api/arc-prediction/health
bun run prod get /api/arc-prediction/2026-10-04
bun run prod get /api/arc-prediction/evaluation
```

Only the bundled race date is supported; unsupported dates return404. Inputs are frozen (`liveFeatures:false`), not live odds or refreshed entries. Changes to declarations/history require rebuilding the feature card and deploying a verified model/card pair. Predictions use `predictedRank`; `baseScore` and `winScore` are separate model margins, **not calibrated probabilities**.

## Viewer integration

The normal detail page `/races/2026/10/04/A8/05` uses this API through the Viewer's
`ArcPredictionTable`, not the generic domestic scoring/ensemble path. The Viewer
checks all 16 current horse numbers/names, known registration IDs, unique source
IDs and ranks, and the frozen-card provenance before displaying the supplied rank.
Changed declarations or an unavailable API fail closed; margins are never shown
as probabilities. The snapshot and retrospective evaluation limitations remain visible.

The generic finish-prediction section's `modelPredictionFeatures` can therefore
remain empty for this card: the dedicated API supplies its predictions. Domestic
prediction-readiness totals do not include this standalone Arc model. Other dates
or overseas races are not silently routed to the frozen 2026 Arc card.

## Training and selection

- Both components train on all verified complete Arc fields available before their cutoff: **2004 and2010–2025**,17races/284runners for production. Earlier incomplete/winner-only years remain excluded, never fabricated.2014 became usable after checking existing person-derived horse histories.
- Source snapshot: `.cache/arc-model-20261003/source-v2.json`, SHA256 `597b85f9161c5a3e79b2013a692f638355245e9cec285b34a1e605865d44dbe9`. Derived snapshot adds87unambiguous events to3774primary events without altering originals. Its manifest retains one differing distance variant; the primary horse record remains authoritative.
- Every feature uses history strictly before the race day. Historical folds train on prior races only:14/15/16training races for2023/2024/2025.
  -73bounded candidates: pairwise ranker, NDCG ranker and win-indicator regressor, with varied depth/regularization/tree counts. No single candidate improved Top1 while preserving every year's previous Top2–5 precision.
- Base candidate14 preserves the previous **precision counts** for each year's Top2–5; this does not mean every previously correctly selected horse is identical. The stronger identity-preservation search found no eligible candidate.
- Win head candidate58 was selected using2011–2022development scores only. It reranks **only the base model's first two horses**. Thus its additional processing cannot change any unordered Top2–5 set.
- The base was selected using2023–2025non-regression comparisons. **The reported scores are retrospectively tuned, not an untouched test or an estimate of future accuracy.** There is no global optimality guarantee beyond the tested candidates/policy.2024/2025winners remain missed.

Precision means `|predicted TopK ∩ actual TopK| / K`, ignoring order.

| Year | Top1 | Top2 | Top3 | Top4 | Top5 |
| ---- | ---: | ---: | ---: | ---: | ---: |
| 2025 |  0/1 |  1/2 |  2/3 |  2/4 |  2/5 |
| 2024 |  0/1 |  0/2 |  1/3 |  2/4 |  3/5 |
| 2023 |  1/1 |  1/2 |  1/3 |  2/4 |  3/5 |

Compared with the original model: Top1 increases from0/3to1/3; no year's Top2–5precision falls.2025Top3 andTop4also improve. See `src/artifacts/evaluation.json` for configurations, predictions, actual results, cutoffs, hashes and limitations.

## Reproduction and verification

Python implementation and tests live in `../pc-keiba-viewer/src/scripts/{arc_finish_model,train_arc_finish_model,arc_model_search}.py` and adjacent package `tests/`. `train_arc_finish_model.py` is the baseline, hash-pinned dataset/training CLI; its generated `features.json` feeds `arc_model_search.search`. The search retains every candidate and freezes development selection separately from evaluation-year comparisons.

The deployed two-stage model is reproduced from that feature table using:

```python
base = train_before(data, cutoff=cutoff, config=TrainingConfig(
    n_estimators=240, max_depth=2, min_child_weight=5, reg_lambda=1,
))
head = train_before(data, cutoff=cutoff, config=TrainingConfig(
    objective="reg:squarederror", n_estimators=240,
    max_depth=2, min_child_weight=1, reg_lambda=1,
))
result = rerank_top_two(predict_card(base, card), predict_card(head, card))
```

Historical fold models/predictions and native production predictions are retained in `.cache/arc-model-20261003/top1-preserved-v1/`; full search evidence is in `top1-search-v1/`. Neither original snapshots nor baseline evaluation are overwritten. Composite model identity is SHA256 of `baseModelSha256 + ":" + winModelSha256 + ":top2-win-rerank"`.

```sh
bun run --cwd apps/arc-prediction check
# Python full gates, from apps/pc-keiba-viewer:
bun run python:check
```

Tests independently compare both Workers model scores against native XGBoost (absolute error<1e-7), assert final16-runner ordering, temporal/schema/field validation, tied scores, route behavior and disclosure of retrospective selection. The shared XGBoost scorer uses float32 inputs to match native split comparisons.

Deploy through `wrangler.jsonc` only after all gates and native parity pass. Keep `TMPDIR`, `XDG_CONFIG_HOME`, `WRANGLER_LOG_PATH` and build outputs inside this repository. `workers_dev` and preview URLs are disabled. Never use Playwright or interactive Access login for production checks.
