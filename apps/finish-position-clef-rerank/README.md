# finish-position-clef-rerank

Race-day rerank of the served finish-position prediction with Cloudflare-hosted
[clef-flash](https://huggingface.co/Cloudflare/clef-flash) (`@cf/cloudflare/clef-flash`).

## Flow

1. A training host (this Mac) builds one bundle per race and uploads it to
   `DETAIL_SECTION_CACHE_KV` as `clefrr:v1:bundle:{YYYYMMDD}:{venue}:{race}`. The day's
   list goes in `clefrr:v1:index:{YYYYMMDD}`. A bundle holds:
   - the fusion weights;
   - a point-in-time feature-ranker score for each runner, covering heatmap-equivalent
     partnerships (horse, horse×jockey, jockey×venue, jockey×trainer×venue, owner×venue,
     jockey×trainer×owner, gate) and pedigree combinations (sire×damsire, sire line×damsire,
     sire×dam's damsire, each also by surface/distance band, venue and going);
   - the anonymized clef-flash System One request, with no names, dates, odds or popularity.
2. The Worker reads the served prediction (`pred:fp:v1:...`) and injects its ranks into the
   request. It then calls clef-flash and fuses the within-race z-scores:
   `base·z(served rank) + feature·z(feature score) + Σ weight·z(clef answer)`. The clef answers are
   P(win) and P(second) from choice questions, used as log-probabilities, and P(top3) and P(top5)
   from yes/no questions, used as logits.
   The result is stored in `clefrr:v1:result:...`. A cached result is reused until the
   served prediction is regenerated, for example by the post-weight rescore.
3. A cron trigger warms every indexed race every 10 minutes from 08:00 to 17:50 JST. The
   viewer reads `GET /api/clef-rerank/{YYYYMMDD}/{venue}/{race}` and hides the panel when
   the Worker answers 404 or fails.

`POST /api/clef-rerank/probe` forwards one System One request to the hosted model. Use it
for local-vs-hosted parity checks. Like every route here, it sits behind Cloudflare Access.

## Commands

```sh
bun run --filter finish-position-clef-rerank check
cd apps/finish-position-clef-rerank && bunx wrangler deploy
bunx wrangler kv bulk put <bulk.json> --namespace-id a013fdf6e7224c3ab0c227be3c899073 --remote
```

## Training (`training/`)

The scripts in this folder ran on the training Mac for the 2026-10-18 race day. They expect:

- the clef-flash weights in `~/models/clef-flash` (`hf download Cloudflare/clef-flash`);
- a working directory at `~/models/clef-work`.

Use `uv` with `training/pyproject.toml` (torch on MPS, transformers, lightgbm, duckdb).

| Step | Script                                | What it does                                                                                                                                                                                                                             |
| ---- | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `extract.py`, `merge.py`, `merge2.py` | Copy JV tables from local PostgreSQL. Fill the days after the last local load (10/05 onward) from Neon, with retries for cold starts. Patch JRA 10/17 results from the realtime D1 snapshots. Store 10/17 predictions in `keiba.duckdb`. |
| 2    | `features.py`, `form2.py`, `hist.py`  | Point-in-time features (prior days only), covering the heatmap partnerships, pedigree combinations, form and class change, and the last five runs.                                                                                       |
| 3    | `train_feat.py`, `train_feat2.py`     | LightGBM lambdarank feature rankers trained on 2013–2025.                                                                                                                                                                                |
| 4    | `run_clef.py` / `run_hosted*.py`      | clef-flash answers from local inference on MPS, or from Workers AI through the probe route.                                                                                                                                              |
| 5    | `eval_*.py`, `fit_v1.py`, `stack.py`  | Walk-forward evaluation on the blind 2026 nowin1 scores, and conditional-logit weight fitting.                                                                                                                                           |
| 6    | `shuka*.py`                           | 秋華賞 model: a strongly regularized linear specialist trained on 3yo-filly graded turf races.                                                                                                                                           |
| 7    | `make_bundles.py`                     | Build and upload the race-day KV bundles.                                                                                                                                                                                                |

Results recorded on 2026-10-18 (JRA, served nowin1 rank = base):

- **2,610 races in 2026:**
  - Base + both feature rankers raised top-1 win from 26.6% to 27.3%, top pick in the top 3 from 58.2% to 58.9%, and top-3 overlap from 49.9% to 50.5%.
  - NAR did not improve, so no NAR bundles are published.
- **clef-flash on its own** reaches 24% top-1 when given the ranks (v1), and about chance level from raw runs (v2) or evidence questions (v4). Pairwise reranking of the top 5 (v3) lowered accuracy.
- **Fitted clef weight:** P(top3) gets about 0.2 relative to the base rank. On 777 races this gave a small held-out gain, which is why r1 uses `clefTop3 = 0.2`.
- **10/17 JRA (36 races):** top-1 win went from 11.1% (served) to 19.4% (r1).
- **Local vs hosted parity:** clef-flash answers differ by a mean of 0.04 and a maximum of 0.30. The top-1 pick agrees in 10 of 12 races.

### v2 (2026-10-18 05:40): Plackett-Luce fusion aimed at box5 / box3 / exacta

The request now asks four things: the winner (choice), the runner-up (choice), and for each runner
whether it finishes in the top three and in the top five (yes/no). Weights come from a top-5
Plackett-Luce fit on all 2,610 JRA races of 2026, with answers from hosted clef-flash: base 0.388,
feature 0.514, and clef winner −0.065, second 0.018, top3 −0.032, top5 0.086.

Expanding-window CV (test months March–October, 2,052 JRA races):

| Configuration                       | box5  | box3  | exacta |
| ----------------------------------- | ----- | ----- | ------ |
| Served base                         | 3.90% | 8.28% | 6.82%  |
| PL base + feature + clef (deployed) | 4.19% | 8.33% | 6.82%  |
| Base + feature, no clef             | 4.19% | 8.38% | 6.77%  |

The held-out top-5 PL NLL improves by only 0.001 per race when clef is added, so clef's incremental
information is close to zero and the gain comes from the features.

Other attempts, none of which beat the served base on these metrics:

- NAR (3,000 races; base sum 39.00 vs 37.5–38.6 for every fusion);
- ban-ei (1,279 races);
- the hierarchical P5/P3/P1/P2 composition;
- giving clef the jockey, trainer, sire and damsire names, which made it worse.

### v3 (2026-10-18 06:45): apply the rerank only to served base models it improves

`training/eval_per_model.py jra` splits the out-of-month CV (2,052 races) by the served base model. A model is
approved only with at least 30 CV races, box5 / box3 / exacta each no worse than the served order, and a positive
sum. Bundles carry the approved list as `baseModels`; the Worker returns 404 without calling clef-flash when the
served model is not on it, so the viewer keeps the served order.

| Served base model (approved)                   | CV races | Δbox5 | Δbox3 | Δexacta |
| ---------------------------------------------- | -------- | ----- | ----- | ------- |
| jra-joken-703-pooled-yetirank-v2               | 177      | +0.56 | 0.00  | +1.13   |
| jra-joken-005-turf-mile-yeti-gated-v1          | 73       | 0.00  | +1.37 | +1.37   |
| jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1 | 62       | +3.23 | 0.00  | 0.00    |
| jra-joken-703-turf-long-spring-qsm-gated-v1    | 47       | 0.00  | 0.00  | +2.13   |
| jra-joken-703-other-extended-qsm-gated-v1      | 43       | +2.33 | 0.00  | 0.00    |
| jra-joken-703-querysoftmax-maxrange-v1         | 35       | +2.86 | 0.00  | 0.00    |
| jra-joken-005-dirt-1700-summer-qsm-gated-v1    | 35       | 0.00  | +2.86 | 0.00    |
| jra-joken-703-turf-1400-qsm-gated-v1           | 31       | 0.00  | +3.23 | 0.00    |

Not approved: jra-cb-v9-sim-2013-clean (508 races, box3 −0.20) and every other model. The 秋華賞 specialist was
also dropped: on 132 cohort races it gains box3 (5.30→6.82) but loses exacta (7.58→6.82). No single model's gain
is significant (bootstrap P(no gain) ≥ 0.12), and selecting on months ≤6 then testing months 7–9 kept box5 (+1.4
pt) but not box3 / exacta, so treat the approved list as provisional. On 2026-10-18 this covers 8 races: Tokyo
1/2/7R, Niigata 1/3/6R, Kyoto 1/3R.
