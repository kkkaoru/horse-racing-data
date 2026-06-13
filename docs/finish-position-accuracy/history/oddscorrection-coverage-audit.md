# oddsCorrectionEnabled Coverage Audit

**Date:** 2026-06-13  
**Scope:** `apps/pc-keiba-viewer` — client-side finish-order correction paths  
**Question:** Does toggling "オッズで予想を補正" cleanly enable/disable ALL odds/popularity influence?

---

## 1. How `oddsCorrectionEnabled` flows in `finish-position-prediction.ts`

### Entry point

`buildFinishPredictionRowsFromResults` (line 574) receives two relevant params:

- `oddsCorrectionEnabled?: boolean` — the toggle value
- `marketOverrides?: ReadonlyMap<string, FinishPredictionMarketOverride>` — realtime tansho odds/rank map

### Gate logic (lines 643–649)

```ts
const isOddsCorrectionApplied = oddsCorrectionEnabled ?? !isNewHorseMaiden;
const finalRunnerConfig = {
  ...runnerConfig,
  oddsWeight: isOddsCorrectionApplied ? runnerConfig.oddsWeight : 0,
  popularityWeight: isOddsCorrectionApplied ? runnerConfig.popularityWeight : 0,
};
```

When `oddsCorrectionEnabled = false`:

- `isOddsCorrectionApplied = false`
- `oddsWeight = 0`, `popularityWeight = 0`

**Both weights are zeroed.** Downstream `calculateScore` skips candidates with `weight <= 0`, so neither the "単勝" nor "人気" candidates contribute to the score.

### Stored vs realtime odds (lines 663–666)

```ts
const marketOverride = marketOverrides?.get(horseNumber);
const storedPopularity =
  marketOverride?.popularity ?? parseStoredNumber(runner.tanshoNinkijun, "00");
const storedOdds = marketOverride?.odds ?? parseOdds(runner.tanshoOdds);
```

`storedOdds`/`storedPopularity` are **always computed** regardless of the flag — they fall back to `runner.tanshoOdds`/`runner.tanshoNinkijun` (stored morning odds from PG) when `marketOverrides` is absent.

With `oddsCorrectionEnabled = false`:

- `marketOverrides` is `undefined` (gated in the component — see §2)
- `storedOdds`/`storedPopularity` are set to the stored morning values
- BUT their corresponding weights are 0 — so they enter the `candidates` array at weight 0 and are skipped by `calculateScore`
- They are **still written to the returned row** (`row.storedOdds`, `row.storedPopularity`) for display purposes only

**Conclusion for §1:** When OFF, `oddsWeight = 0` and `popularityWeight = 0` — stored odds/popularity do NOT influence the score. The values appear in the row struct for column display, not scoring.

---

## 2. `getHorseHistoryAdjustedConfig` — debut horse path

`getHorseHistoryAdjustedConfig` (lines 351–381) modifies weights **before** the gate:

```ts
if (horseResultsCount <= 1) {
  oddsWeight: baseConfig.oddsWeight * oddsRestoreMultiplier + 0.015,  // e.g. 2× + 0.015
  popularityWeight: baseConfig.popularityWeight + DEBUT_POPULARITY_WEIGHT_BUMP,
}
```

This produces a `runnerConfig` with **elevated** oddsWeight/popularityWeight for debut horses.

The gate then zeros them:

```ts
const finalRunnerConfig = {
  oddsWeight: isOddsCorrectionApplied ? runnerConfig.oddsWeight : 0,
  popularityWeight: isOddsCorrectionApplied ? runnerConfig.popularityWeight : 0,
};
```

The elevation is computed before zeroing, but since the gate sets to 0, the elevated value is **discarded**. No leak.

---

## 3. `getConditionAdjustedConfig` — JRA/NAR multipliers

`getConditionAdjustedConfig` (lines 290–349) scales `oddsWeight` and `popularityWeight` on `baseConfig` before `getHorseHistoryAdjustedConfig` is called. Same conclusion: the gate is applied last on `runnerConfig`, so these multipliers only matter when the gate is ON. No leak when OFF.

---

## 4. `marketOverrides` construction in the component

In `finish-position-prediction-table.tsx` (lines 523–533):

```ts
const marketOverrides =
  oddsCorrectionEnabled && tanshoRows.length > 0
    ? buildFinishPredictionMarketOverrides(tanshoRows)
    : undefined;
setDisplayRows(
  buildFinishPredictionRowsFromInputs({ ...inputs, oddsCorrectionEnabled }, marketOverrides),
);
```

When OFF: `marketOverrides = undefined`. The lib function therefore uses stored `tanshoOdds`/`tanshoNinkijun`, but with zero weight. Correct.

---

## 5. Score tiebreaker

Sort at line 746:

```ts
provisionalRows.toSorted(
  (left, right) => left.score - right.score || Number(left.horseNumber) - Number(right.horseNumber),
);
```

Tiebreaker is horse number only — **no odds/popularity tiebreaker**. Not a leak.

---

## 6. Other client-side paths that call `buildFinishPredictionRows*`

### Path A — `horse-race-results-table.tsx` → `RACE_FINISH_PREDICTION_RESULTS_EVENT`

File: `src/app/races/detail/horse-race-results-table.tsx` lines 742–766

```ts
window.dispatchEvent(
  new CustomEvent(RACE_FINISH_PREDICTION_RESULTS_EVENT, {
    detail: {
      rows: buildFinishPredictionRowsFromResults({
        currentDistance,
        currentKeibajoCode,
        currentRaceDate,
        currentSource: source,
        currentTrackCode,
        results: visibleResults,
        runners,
        // NOTE: oddsCorrectionEnabled is NOT passed
        // NOTE: marketOverrides is NOT passed
        // NOTE: modelPredictionFeatures, similarityFeatures, sameDayVenueJockeyWins NOT passed
      }),
    },
  }),
);
```

This dispatches an event that `finish-position-prediction-table.tsx` listens to (lines 570–584) and **replaces `displayRows`** unconditionally with the event payload.

**Gap A (CRITICAL): `oddsCorrectionEnabled` is NOT forwarded.**

Result: `oddsCorrectionEnabled` defaults to `undefined` → `isOddsCorrectionApplied = !isNewHorseMaiden` (true for non-maiden races, false for maiden races). Regardless of the user's toggle, whenever the user interacts with the race results filter table (changing visible results), the event fires and **overwrites the prediction table with rows computed at `oddsCorrectionEnabled = undefined`**, effectively always applying odds correction for non-maiden races.

Additionally: `marketOverrides` is not passed, so it uses stored morning odds — not realtime odds — even when the main path was using realtime.

This event path also drops: `modelPredictionFeatures`, `similarityFeatures`, `sameDayVenueJockeyWins`, `currentGradeCode`, `currentKyosoJokenCode`, `currentKyosoJokenMeisho` — producing a degraded prediction row.

### Path B — `race-ai-data.ts` (AI assistant export)

Lines 527–531:

```ts
const tanshoRows = realtime?.odds?.latest.tansho ?? [];
const finishRows = buildFinishPredictionRowsFromInputs(
  finishPayload.inputs,
  tanshoRows.length > 0 ? buildFinishPredictionMarketOverrides(tanshoRows) : undefined,
);
```

`oddsCorrectionEnabled` is **not passed** → defaults to `undefined` → always applies odds correction when realtime odds are available, regardless of user preference. This is the AI export path — not the visible prediction table — so it does not affect the displayed order, but it means AI responses always include odds-corrected predictions regardless of the toggle.

### Path C — `ai-json-export-section.tsx`

Line 311–314 (same pattern as Path B):

```ts
const finishRows = buildFinishPredictionRowsFromInputs(
  finishPayload.inputs,
  tanshoRows.length > 0 ? buildFinishPredictionMarketOverrides(tanshoRows) : undefined,
);
```

Same issue: `oddsCorrectionEnabled` absent, always applies odds if realtime available. This affects the JSON export, not the visible table.

### Path D — `overall-score-table.tsx`

This table displays its own `OverallScoreRow[]` pre-computed server-side. It does NOT call `buildFinishPredictionRows*`. It fetches realtime odds for display only (column values), not for re-ranking. No gap.

### Path E — `realtime-race-section.tsx`

No calls to `buildFinishPredictionRows*`. No gap.

---

## 7. Summary table

| Influence path                                                          | What it is                                      | `oddsCorrectionEnabled` gates it?                                                            |
| ----------------------------------------------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `oddsWeight` in `finalRunnerConfig`                                     | Score weight for stored/realtime odds           | **YES** — zeroed when OFF                                                                    |
| `popularityWeight` in `finalRunnerConfig`                               | Score weight for stored/realtime popularity     | **YES** — zeroed when OFF                                                                    |
| `getHorseHistoryAdjustedConfig` debut elevation of oddsWeight           | Pre-gate multiplication for debut horses        | **YES** — gate applied after, result is 0                                                    |
| `getConditionAdjustedConfig` JRA/NAR multipliers on oddsWeight          | Pre-gate scaling                                | **YES** — gate applied after, result is 0                                                    |
| `marketOverrides` construction                                          | Realtime tansho map                             | **YES** — not built when OFF                                                                 |
| Stored `tanshoOdds`/`tanshoNinkijun` value in returned row              | Display-only fields                             | N/A — weight is 0, no score impact                                                           |
| `horse-race-results-table.tsx` → `RACE_FINISH_PREDICTION_RESULTS_EVENT` | Rewrites displayRows when result filter changes | **NO** — `oddsCorrectionEnabled` omitted, defaults to `undefined` → always ON for non-maiden |
| `race-ai-data.ts` `buildFinishPredictionRowsFromInputs`                 | AI assistant prediction                         | **NO** — always applies realtime odds if available                                           |
| `ai-json-export-section.tsx` `buildFinishPredictionRowsFromInputs`      | JSON export                                     | **NO** — always applies realtime odds if available                                           |
| Score sort tiebreaker                                                   | Horse number                                    | N/A — no odds in tiebreaker                                                                  |

---

## 8. OFF semantics today

When `oddsCorrectionEnabled = false` on the **main path** (direct toggle):

- `oddsWeight = popularityWeight = 0` — odds/popularity **do not affect the score**
- Other signals (horse history, recent form, jockey, trainer, similarity, model) still blend with their normal weights
- **OFF does NOT mean "pure model"** — it means "model + history + jockey + trainer + similarity, but no odds/popularity"
- The UI hint says "モデル予想をそのまま表示" — this is **technically misleading**: OFF still blends non-odds client-side signals around the backend model score

However, when the user changes the result filter (e.g., filters to a specific track surface), `horse-race-results-table.tsx` dispatches the event which **overwrites displayRows with odds correction implicitly ON**. The toggle's visual state does not change, so the user sees the checkbox as OFF but the displayed order reflects ON semantics.

---

## 9. Recommended fixes

### Fix 1 (CRITICAL — visible prediction table desync): `horse-race-results-table.tsx`

The `RACE_FINISH_PREDICTION_RESULTS_EVENT` dispatch at lines 742–766 must pass the current `oddsCorrectionEnabled` value and the full `inputs` from the prediction section. The cleanest approach:

**Option A:** Remove this event mechanism entirely. The results filter already causes a re-render of its own table; the prediction table should not be driven by filter changes in a different table. The prediction section fetches its own `inputs` from the server payload and manages its own state.

**Option B:** Pass `oddsCorrectionEnabled` via the event and read it in `horse-race-results-table`. This requires `horse-race-results-table` to subscribe to the same `useSyncExternalStore` localStorage store. It also still omits `modelPredictionFeatures`/`similarityFeatures`/`sameDayVenueJockeyWins`, so the emitted rows will always be degraded vs. the primary calculation.

Option A is strongly preferred — the event-driven cross-component override produces a second, inferior prediction in place of the authoritative one.

### Fix 2 (MINOR — AI export paths): `race-ai-data.ts` and `ai-json-export-section.tsx`

These are export/AI-assistant paths and don't affect the visible prediction table. However, for consistency with user intent, they should either:

- Always pass `oddsCorrectionEnabled: true` explicitly (AI uses best-available signal — reasonable default), **or**
- Accept and forward the current `oddsCorrectionEnabled` state if the component has access to it

Since these paths are invoked in response to user-initiated export actions, hardcoding `oddsCorrectionEnabled: true` (or simply always building the `marketOverrides` map as they currently do) is acceptable and intentional — the AI/export always shows the odds-informed view.

### Fix 3 (UX label mismatch)

The hint "オフ: モデル予想をそのまま表示" is inaccurate — OFF still blends jockey/trainer/history/similarity signals. The label should say something like "オフ: オッズ補正なし（成績・騎手・モデルの複合予測）" to be accurate. This is a cosmetic fix, not a functional bug.

---

## 10. Verdict

The flag **does NOT fully gate all odds correction** for the visible prediction table. The primary toggle path (main `useEffect` in `FinishPositionPredictionTable`) correctly zeroes both `oddsWeight` and `popularityWeight`. But the secondary event path from `horse-race-results-table.tsx` unconditionally overwrites `displayRows` with a call that omits `oddsCorrectionEnabled` and `marketOverrides`, silently reverting the toggle to the default-ON behavior (for non-maiden races) whenever the user interacts with the result filter table. Fix 1 (Option A) is the recommended resolution.
