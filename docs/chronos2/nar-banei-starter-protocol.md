# NAR / Ban-ei starter protocol — before training outcomes

The two domains remain separate. The initial metadata selections are research pools using grade/condition proxy labels, **not detailed B4/C3-class routing**. Ban-ei `joken-000` pools have unspecified conditions and are only distinguished by season and the other recorded dimensions.

## Complete starter accounting

Use official RA `shusso_tosu`, checked against complete SE races **before** cohort filtering. The [JRA-VAN TARGET abnormality code table](https://targetfaq.jra-van.jp/faq/detail?id=705&site=SVKNEGBV) identifies:

- 1: scratch; 2: starting exclusion; 3: race exclusion — not starters.
- 4: did not finish; 5: disqualified — retain as actual starters.
- 0, 6 and 7: normal, remounted and demoted — retain.

The first audit excluded only 1/2 and therefore failed. Excluding 1/2/3 makes all official starter counts agree: **736,520 source starters**. There are no duplicate source keys or unknown starter registrations. Confirmed cancelled training races (`data_kubun=9`, zero starters) are logged separately; no 2023 evaluation race was removed.

Missing classified finishes stay undefined (`performance_rating=NULL`), never fabricated last-place values. The source contains 43 unexplained missing finishes in **2018–2019**, before the training/evaluation labels; retain their rows but omit these undefined values from numeric historical contexts. Missing normal classified results from 2020 onward fail the exporter.

Training windows continue to require a finite supervised target. **Inference windows must not require a realized target value**, so DNF/disqualified starters with enough earlier history receive forecasts, rather than outcome-dependent market fallback. Only genuinely sparse earlier history invokes fallback. Exact-rank supports distinguish absent official ranks from successful predictions.

Finite decimal odds **1.0 are valid**; zero/missing prices remain invalid. This input-domain correction is made before any NAR/Ban-ei Chronos training outcome inspection. It does not retune JRA candidates.

## Limitation of prior JRA evidence

The rejected JRA round used the previously frozen classified-finisher history source, not independently attested complete starter rosters. Its rejection remains unchanged; do not upgrade that source-cohort check into a full-starter/PIT approval. The new starter-aware NAR/Ban-ei data protocol must be reported separately.
