# Journal of Equine Science — literature collection

Curated extraction of papers from the **Journal of Equine Science** (J-STAGE, journal
code `jes`, Online ISSN 1347-7501) that are **useful for racehorse finishing-position
(着順) prediction** — the modelling task of this repository.

Source listing: <https://www.jstage.jst.go.jp/browse/jes/37/0/_contents/-char/en>
(processed **newest volume/issue first**).

## Layout

| Path                       | Purpose                                                                                                                                 |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `SUMMARY.md`               | Single aggregate markdown of **all relevant** papers (the running digest).                                                              |
| `papers/<docid>-<slug>.md` | One markdown per relevant paper, with structured extraction from the PDF.                                                               |
| `processing-log.md`        | Every article examined and its keep/skip decision. This is the resumable state of the collection loop — newest-first cursor lives here. |

Helper scripts that drive collection live in `scripts/journals/`.

## Relevance criteria

A paper is **kept** only if it offers knowledge, features, or signals that could plausibly
improve a model predicting a horse's race finishing position. Concretely, any of:

- Determinants of racing performance — physiology, biomechanics, cardiovascular/respiratory
  fitness, muscle fibre type, VO₂max, lactate/blood markers, stride dynamics.
- Performance-degrading conditions — orthopaedic injury, lameness, EIPH, airway disease,
  illness — i.e. anything that affects race outcome or scratch/withdrawal risk.
- Training load, conditioning, fatigue, recovery and their performance effects.
- Genetics/genomics of athletic ability, speed/stamina (e.g. _MSTN_), heritability of racing
  ability; pedigree/breeding effects on performance.
- Effects on performance/time of: carried weight (斤量/futan), age, sex/maturity, track
  surface/going, distance, environment, jockey.
- Statistical / machine-learning modelling of equine racing performance or related outcomes.
- Fitness monitoring (heart rate, recovery) predictive of performance.

**Excluded**: pure reproduction, conservation genetics of non-racing breeds, lab assay/method
papers (unless the assay measures a performance biomarker), dental age estimation, parasitology/
microbiology unrelated to performance, general husbandry/welfare unrelated to performance.

Borderline papers are kept with an explicit `relevance` note explaining the tangential link.

## Methodology

1. Enumerate volumes newest-first from the J-STAGE volume list.
2. For each issue, list articles (title, authors, type, pages, release date, PDF URL).
3. Title + abstract screen against the criteria above; record the decision in `processing-log.md`.
4. For kept papers: download the PDF, read it, and write a structured markdown extraction in
   `papers/`, then update `SUMMARY.md`.

This collection is built incrementally by an autonomous loop; `processing-log.md` records how
far back the cursor has reached.

## Status — COMPLETE

The **entire J-STAGE archive of the Journal of Equine Science (Vols 5–37, 1994–2026)** has been
screened. Volumes 1–4 do not exist on J-STAGE (the `jes` archive begins at Vol 5, 1994).

- **~490 articles examined**, **119 kept** as relevant to finishing-position prediction.
- Each kept paper has a detailed extraction in `papers/<docid>-*.md`: verbatim abstract, full
  methods, all quantitative results reproduced as tables, discussion, limitations, and concrete
  feature-engineering notes mapped to JRA/NAR/JBBA data fields.
- Per-paper files total ~16,700 lines (median ~136 lines each).
- Feature families covered: **A** injury/soundness/scratch-risk, **B** respiratory/airway,
  **C** exercise-physiology/fitness markers, **D** genetics/pedigree, **E** environment/heat,
  **F** conformation/body-size/gait, **G** statistical modelling / age & environment effects.

## Tooling

`scripts/journals/jstage.py` (+ `test_jstage.py`, 26 tests / 99% coverage) is a stdlib-only
J-STAGE helper: `parse_contents()` / `fetch_contents(vol, issue)` to list a volume's articles,
and `download_pdf()` for polite PDF retrieval with retry. CLI: `contents --vol N [--issue M]`
and `download --pdf-url URL --dest PATH`. Cached PDFs live in `scripts/journals/cache/`
(git-ignored / not tracked). PDF→text used `pypdf`; a few old scanned volumes (Vol 6) required
OCR (tesseract).
