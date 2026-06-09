# Journal of Equine Science — literature collection

Curated extraction of papers from the **Journal of Equine Science** (J-STAGE, journal
code `jes`, Online ISSN 1347-7501) that are **useful for racehorse finishing-position
(着順) prediction** — the modelling task of this repository.

Source listing: <https://www.jstage.jst.go.jp/browse/jes/37/0/_contents/-char/en>
(processed **newest volume/issue first**).

> **Note — `docs/journals/` now hosts TWO collections:**
>
> - **(a) Journal of Equine Science** (`jes`, J-STAGE) — the curated, per-paper, relevance-screened
>   collection that the rest of this file documents.
> - **(b) 馬の科学 (Uma no Kagaku)** — a companion **raw** full-issue corpus under
>   [`horse-sciences/`](./horse-sciences/README.md), a JRA 競走馬総合研究所 internal bulletin
>   captured in full but **not yet relevance-screened**. See
>   [Companion collection — 馬の科学](#companion-collection--馬の科学-jra-競走馬総合研究所) below.

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

The companion 馬の科学 corpus is built by a separate helper, `scripts/journals/reflow_es.py`
(see the companion section below); the two scripts share only the git-ignored
`scripts/journals/cache/` PDF cache.

## Companion collection — 馬の科学 (JRA 競走馬総合研究所)

A second, separate collection lives under [`horse-sciences/`](./horse-sciences/README.md). It
is a Japanese-language, JRA-internal bulletin captured as a **raw full-issue corpus** — the
counterpart to, but deliberately distinct from, the curated `jes` collection above.

- **誌名 / source**: 馬の科学 (_Uma no Kagaku_), published by **JRA 競走馬総合研究所**
  (Equine Research Institute, Japan Racing Association). Source page:
  <https://company.jra.jp/equinst/publications/es.html>
- **Coverage**: **Vol.50 (2013) – Vol.56 No.4 (2019-12)**. The journal **休刊** (ceased) at
  Vol.56 No.4. JRA hosts PDFs only for Vol.50–56; older volumes are not online.
- **Raw, not curated** — KEY DIFFERENCE from the `jes` collection: every issue is captured
  **in full** (要約なし / not summarized), and the corpus is **not yet relevance-screened**
  into kept/skip per-paper rows. The `jes` collection is curated to 119 kept papers; this one
  is a complete-text corpus awaiting screening.
- **Layout**: [`horse-sciences/README.md`](./horse-sciences/README.md) is the full 28-issue
  catalog (発行日 + PDF URLs). Each issue is one file
  `horse-sciences/volNN-noM-YYYY-MM.md` with a hand-cleaned `## 目次` plus the full PDF body
  text — **28 issue files** in all.
- **Vol.50 caveat**: for Vol.50 (No.1–4) JRA hosts only the cover / 目次 scan (1–2 pages);
  full-text PDFs begin at **Vol.51**. Documented per-file.
- **vol55-no2 added**: Vol.55 No.2 was the one previously missing issue and has been added
  (full text).
- **Relevance to finishing-position prediction**: the same feature families as the `jes`
  collection — exercise physiology / fitness (心拍・乳酸・VO₂), 整形外科 / 腱・骨折 injury &
  soundness, 呼吸器, 遺伝 / 系統 genetics & pedigree, 馬体 / 装蹄 / 歩様 conformation / gait,
  暑熱 / 環境, plus JRA-specific surveillance — but as **raw text, awaiting screening**.
- **Tooling**: `scripts/journals/reflow_es.py` (+ `test_reflow_es.py`, 25 tests) is a
  deterministic, content-preserving body reflow (rejoins wrapped TOC / leader lines, strips
  PDF control-char garbage) guarded by a char-multiset invariant. Extraction used **PyMuPDF**
  (resolves the `90pv-RKSJ-H` CID fonts) with a **tesseract** OCR fallback. Cached PDFs live in
  `scripts/journals/cache/` (git-ignored).
