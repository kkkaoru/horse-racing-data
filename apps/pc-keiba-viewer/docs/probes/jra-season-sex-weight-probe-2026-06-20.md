# JRA Season × Sex × Weight Interaction Probe (2026-06-20)

## Summary

Investigation into a potential **3-way interaction** between race month (season),
horse sex (`seibetsu_code`), and body weight on JRA finish-position prediction
accuracy.

**Decision: PROCEED** with a 3-feature GBDT addition (raw `seibetsu_code`,
`zogen_sa`, `kaisai_month`) plus a multi-layer calibration pipeline.

**Key finding:** The 3-way cross-feature is **NOT** needed. There is no genuine
3-way structure — weight is a clean global main effect. The only real exploitable
signal is a **2-way sex × season** miscalibration (summer mare under-pick), which
a GBDT can learn non-linearly from the raw month + sex + weight columns.

---

## 1. 3-Way Interaction (month × sex × weight): NO signal beyond 2-way

- **Weight is a clean global monotone main effect.** Precision rises smoothly with
  body weight: light 37.9% → heavy 43.8%.
- The **same weight gradient holds within every (season, sex) cell** — there is no
  cell where the weight effect reverses, flattens, or sharpens in a way that would
  justify a true 3-way term.
- Large per-cell spreads (e.g. summer geldings showing a 14.9pp swing) are **n < 500
  noise**, not structure. They do not survive once cell sizes are accounted for.

**Conclusion:** No genuine 3-way interaction. Weight contributes as a main effect
and should be added as a raw feature, not crossed.

---

## 2. Real Signal: 2-way sex × season (summer mare under-pick)

This is the one genuine, exploitable miscalibration uncovered by the probe.

- **Mares genuinely run better in summer.** Empirical win-rate rises from 5.5% →
  7.2% in summer months.
- **The model fails to credit them.** Despite the higher realized win-rate, the
  model does not lift summer mares.
- **The model over-backs colts in summer instead:**
  - Colt August **overpick: +0.70pp**
  - Mare August **underpick: −0.79pp**
- **Direction matters:** this is the **OPPOSITE** of the common "spring estrus" lore.
  The effect is a _summer_ sex-imbalance, not a spring one.
- **Geldings are uniformly under-picked** (−0.4 to −1.3pp) across the calendar, with
  **no seasonal shape** — i.e. geldings need a flat correction, not a seasonal one.

**Conclusion:** A real 2-way sex × season miscalibration exists and is correctable.

---

## 3. Mare spring estrus: NOT supported by data

- There is **no spring-specific mare effect** in the data.
- The genuine mare effect is **summer**, not spring.
- The popular spring-estrus narrative should **not** be encoded as a feature or prior;
  it is contradicted by the empirical win-rates.

---

## 4. zogen_sa × sex × month: nothing exploitable

- Average official weight change (`zogen_sa`) is **flat at ~4.4–5.6kg** across all
  (sex, month) cells.
- There is **no mare-specific seasonal weight pattern** worth turning into a feature.
- `zogen_sa` is still worth adding as a raw main-effect feature (weight-change is
  generically informative), but **not** as a sex × season interaction.

---

## 5. Venue 3-way (Tokyo / Hanshin / Hakodate)

- The worst-calibrated cells are **mares at Tokyo / Hanshin in spring-to-early-summer**.
- These are simply the **same sex × season miscalibration re-localized** to specific
  venues — not a distinct venue-driven interaction.
- There is **no distinct venue × sex × weight interaction** to exploit independently.

**Conclusion:** Venue does not add a new axis here; the signal collapses back to
sex × season.

---

## 6. Features Added (committed 28bfed7)

Three raw columns added so the GBDT can learn the interactions non-linearly:

| Feature         | Type       | Description                                      |
| --------------- | ---------- | ------------------------------------------------ |
| `seibetsu_code` | int        | 1 = 牡 (colt), 2 = 牝 (mare), 3 = セン (gelding) |
| `zogen_sa`      | signed int | Official body-weight change (kg)                 |
| `kaisai_month`  | int (1–12) | Raw race month                                   |

These are **raw** features. No cross / interaction term is precomputed — the model
derives interactions from the raw triplet.

---

## 7. 5-Method Evaluation Summary

| Method                       | Assessment                                                                | GO / NO-GO                 |
| ---------------------------- | ------------------------------------------------------------------------- | -------------------------- |
| **ML** (GBDT features)       | Add raw features, let GBDT learn interactions                             | **GO** — primary path      |
| **RL** (contextual bandit)   | MLX RL ABORT (37% < 49%); lookup-table bandit possible but cells too thin | **WEAK-GO** — low priority |
| **Statistics** (calibration) | Isotonic regression / empirical Bayes per (sex, season) cell              | **GO** — Layer 2           |
| **Mathematics** (ANOVA)      | Formal 3-way interaction significance test                                | **GO** — gate-0 validation |
| **Programmatic correction**  | Post-scoring lookup table for miscalibrated cells                         | **GO** — Layer 3           |

---

## 8. Implementation Plan: 4-Layer Pipeline

1. **Layer 1 — ML (GBDT):** Train with the new raw features (`seibetsu_code`,
   `zogen_sa`, `kaisai_month`) so the model learns the 3-way relationship
   non-linearly from raw month + sex + weight.
2. **Layer 2 — Statistical coefficients:** Compute cell-level calibration residuals
   (per (sex, season) cell) via isotonic regression / empirical Bayes.
3. **Layer 3 — Programmatic correction:** Apply a summer-mare boost lookup table to
   correct the residual miscalibration that the GBDT does not fully absorb.
4. **Layer 4 — Ensemble:** Weighted combination of the above, **place-preserving**
   (do not disturb exact 2着/3着 ordering when applying corrections).

---

## 9. Decision

**PROCEED** with the 3-feature GBDT addition (already committed at `28bfed7`) plus
the multi-layer calibration pipeline above.

The 3-way cross-feature is **NOT needed**: the GBDT learns the interactions from the
raw month + sex + weight inputs. The only genuine exploitable signal is the **2-way
summer mare under-pick** miscalibration, which is best addressed by the Layer 2/3
calibration stages on top of the raw-feature GBDT.
