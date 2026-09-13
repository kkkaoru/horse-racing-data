# Native rescore parity review — 2026-09-13

## Confirmed JRA blocker

The successful live small-Container canary published 16 rows using
`jra-joken-703-dirt-intermediate-qsm-gated-v1`. Read-only Neon verification
matched the generation independently reported by readiness.

`apps/finish-position-cron/src/scoring/jra-shadow-scorer.ts` currently selects
`jockey_pedigree_703` for class `703` with fresh odds. Its comment claiming to
mirror current Python rules is not sufficient evidence of current serving
parity: the live Python-selected model above is different.

`src/scoring/rescore-consumer.ts::scoreAndWrite` builds entries, selects and
loads this native model, scores it, optionally rescoring with Stage 1 on the
standard-deviation guard, checks the deadline, and upserts predictions.
That orchestration does not invoke Python's routed Prophet adjustment.
`predict_lib/prophet_adjustment.py` resolves policy using category, cell,
branch and served signature before applying its coverage/score/spread guards.
Matching one tree evaluator is therefore insufficient to replace this path.

Missing native R2 artifacts currently cause authoritative Python fallback.
Do not upload those artifacts merely to remove the observed 14.843-second
native fallback delay: availability could turn a routing mismatch into a
published prediction change. The delay is pre-Container work, so eliminating
it alone is not a measured reduction in Container memory-time.

## Evidence required for a safe replacement

- Freeze the exact feature order and NaN positions, artifact manifest and
  policy versions, active/excluded entry set, weight generation, market and
  weather inputs, day-card context, and routing inputs at scoring time.
- Run the complete Python reference and candidate native pipeline without
  network or publication side effects on those same inputs. Compare final
  model identity, serving signature, every published value, ranks and audit
  fields—not merely raw model scores or a final model ID.
- Cover fresh and missing odds, confidence and weather guards, applicable
  ensembles/specialized variants, Prophet enabled/disabled/insufficient data,
  exclusions, ties and nonfinite-value handling. Evaluate Ban-ei separately;
  a JRA result does not establish Ban-ei parity.
- Verify existing cache/entry/weight attestations, deadlines, ownership,
  reconnect/cleanup and unchanged retry budgets through the publication
  boundary before permitting any native write.
- Keep unsupported contracts on the authoritative Python path. Treat an
  explicit contract guard as a correctness boundary, not a claim of cost
  savings; preserve fail-open-to-Python behavior and fail-closed publication
  validation.

## Capture limitation and next representative comparison

The production prediction table stores model version, generation and some
odds/weight audit columns but no complete served signature. Later mutable
snapshots cannot reconstruct a proven identical live input set. The completed
canary verifies natural fresh-weight publication and cleanup, while its two
CPU-profile comparisons used frozen pre-weight inputs and a different final
model. These are distinct pieces of evidence, not an exact live replay.

Before expanding the small role, prepare offline immutable captures covering
additional JRA routes and maximum fields, then separately NAR and Ban-ei.
Include complete-image startup/resource limits and failure/cleanup paths.
Do not replay historical production requests, submit duplicates, reset
attempts, upload native artifacts, or infer day-base sizing from this work.
No runtime change or additional production rollout is made by this review.
