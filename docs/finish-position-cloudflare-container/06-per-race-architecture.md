# 06. Per-Race 予測アーキテクチャ feasibility（レースごと分解）

> 調査日: 2026-06-19 / read-only 調査 + 設計のみ（コード変更・重い pipeline 実行なし）
> 前提ドキュメント: `01-current-mechanism.md` 〜 `05-pilot-results.md`
> 結論先出し: **per-race を「Cloudflare Container の実行単位（chunk unit）」にするのは非推奨**。
> 理由は本文 §0 / §6 で詳述。USER の狙い（CF 仕様適合 × Mac 非依存 × OOM 回避）は、
> **per-race を chunk 単位にする**のではなく、**各 feature layer の内部を per-target-race に scope する
> 既存の最適化（doc 05 で h2h に既に適用済み）を全 layer に展開**することで達成するのが正解。

---

## 0. エグゼクティブサマリ（最重要）

USER 指示は「Cloudflare Containers の仕様（per-request・15 分枠・メモリ枠）に合わせて、**レースごと**に予測する」。
コードを精読した結果、この狙いを満たす方法は 2 つに分かれ、**どちらを採るかで結論が真逆になる**。

| 解釈                                                                     | 何をするか                                                                                                        | 評価                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. per-race を「container の実行単位」にする**                         | Cron → Queue に 1 レース 1 message を流し、container が 1 レースだけ feature build + score + UPSERT               | **非推奨**。支配コスト（21 年 history 全 scan）は **per-category 固定**で、レース数に比例しない。1 レースごとに container を起こすと **その 21y scan を N(=48) 回払う** → Neon compute が **数十倍**に膨れ、`e84b078` の Neon コスト削減を真っ向から regress する。wall-clock も悪化。doc 01 §3 / doc 04 §3a が既に「当日出走馬にスコープしても 21y scan は減らない」と adjudication 済み。 |
| **B. layer 内部を per-target-race に scope する（intra-layer scoping）** | container の実行単位は **per-category のまま**。各 layer が target parquet の race key 集合に history join を絞る | **推奨**。OOM の真因（h2h の `O(全期間²)` pairwise）を消す。doc 05 Phase 4 で h2h に既に適用され OOM 解消・全 NAR 60 races を 788s で完走。残りの global 系 layer（near-miss / baba-pedigree / banei-futan-class / lineage）にも同型の filter を足せば、メモリ余裕が増し JRA leg の安全マージンが伸びる。                                                                                   |

**core 事実（コードで確認）**: 着順予測の支配コストは「**当日レース数**」ではなく「**21 年分の per-horse history を Postgres から scan する量**」。
`finish_position_features_duckdb.py` の base build は target window の race だけでなく、
`history_start = today − 21y` から `to_date` までの **全 race / 全馬** を `rec` / `se` テーブルに staging し
（L421-422, L629-630: `kaisai_nen between history_year and to_year` で date BETWEEN scan）、
そのあと history feature を `h.race_date < t.race_date` join で付与する。
**history side は target 馬で絞られていない**（全 population scan）。
したがって target を 1 レースに削っても scan 量はほぼ不変 → **per-race chunking は cost を下げない**。

→ **推奨は B（layer 内部の per-race scoping を全 layer に展開）＋ 実行単位は per-category（doc 03/04 のまま）**。
A（per-race を container chunk 単位にする）は Neon コスト・複雑性の両面で不採用。

---

## 1. 現行のビルド単位と global 依存（layer 分類）

### 1.1 ビルド単位

現行 `predict_upcoming.py` → `pipeline_runner.build_pipeline` の実行単位は **per-category**。

1. `build_base_argv`（`pipeline_args.py:221`）が `finish_position_features_duckdb.py` を
   `--category {jra|nar|ban-ei} --target-date YYYYMMDD --days-ahead N` で 1 回起動。
   → base parquet（その window の全レース、UPCOMING 含む）を出力。
2. `layer_chain_for(category)`（`pipeline_args.py:102`）の 14（JRA）/ 8（NAR）/ 5（Ban-ei）layer を
   順次 subprocess 起動。各 layer は前段 parquet を入力に、Postgres history を追加 scan して列を append。
3. `build_upcoming_feature_rows` が final parquet を `groupby(race_id)` して
   `race_id -> entries` の dict を返す（`pipeline_runner.py:236`）。score は Python 側で per-race ループ。

**race 列挙ロジック**: 「upcoming = `finish_position` NULL」は SQL レベルで明示はされておらず、
base build が target window の全レースを出し、UPCOMING 行は history feature が NULL/0 のまま survive する
（`pipeline_runner.py` docstring）。score 時点では race_id ごとに分かれている。
**1 レース単位に絞る arg は base build にも layer にも存在しない**（後述 §2.3）。

### 1.2 layer 分類表（race-scopable / global）

凡例:

- **入力**: parquet-only（PG 不要） / PG-history（`--pg-url` + `--from-date 20100101` で 21y scan）
- **集計スコープ**: own-history（その馬自身の過去走のみ） / within-race（同一レースの 8-18 頭内の相対 rank/zscore） / **GLOBAL**（他レースの馬・血統 cohort 等、dataset 横断）
- **race-scopable?**: YES（計算形が 1 レース＋各馬 history で閉じる） / **NEEDS-REFACTOR**（global cohort を含むが target-race scope filter で bound 可能） / NO（真に dataset 全体が必要）

| #   | layer                       | 入力                                      | 集計スコープ                                    | race-scopable?               | 根拠（file:line）                                                                                                                    |
| --- | --------------------------- | ----------------------------------------- | ----------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | add-race-internal           | parquet-only                              | within-race                                     | **YES**                      | `partition by ... race_bango`（race 内 rank のみ。PG 不要）                                                                          |
| 2   | add-market-signal           | PG-history(20100101)                      | within-race                                     | **YES**                      | odds を date range で pull するが feature 計算は `over (partition by ... race_bango)` の within-race normalize                       |
| 3   | add-sectional-and-weight    | PG-history                                | own-history（直近5走）                          | **YES**                      | `row_number() over (partition by ... ketto_toroku_bango order by h.race_date desc)`, `h.race_date < t.race_date`                     |
| 4   | add-futan-juryo             | PG-history                                | own-history + within-race                       | **YES**                      | `horse_window partition by source, ketto_toroku_bango` + race 内 futan rank                                                          |
| 5   | add-workout                 | PG-history(90日lookback)                  | own-history                                     | **YES**                      | `partition by ... ketto_toroku_bango`, `w.workout_dt < rk.race_dt`                                                                   |
| 6   | add-near-miss               | PG-history                                | own + within-race + **sire/damsire cohort**     | **NEEDS-REFACTOR**           | horse/jockey 累積=own、`race_favorite_dominance`=within-race、だが `partition by sire_id, kyori` は同 sire を持つ**全馬**を集計      |
| 7   | add-grade-race-lineage      | PG-history + lineage-races/\*.json        | **trial-race set（cross-race）** + own lookback | **NEEDS-REFACTOR**           | `race_serves_as_trial` を race_meta × trial_defs の CROSS JOIN で識別 → 他レースが「trial か」を全体で判定                           |
| 8   | **add-head-to-head**        | PG-history                                | **co-starter pairwise**                         | **YES（実装が global→OOM）** | §1.3 で詳述。pair は同一 target race 内のみだが `pair_history` を全期間 materialize（~150M 行）                                      |
| 9   | add-baba-pedigree-affinity  | PG-history                                | own baba + **sire/damsire cohort**              | **NEEDS-REFACTOR**           | `sire_baba_career partition by sire_id, baba_cond` / `damsire_baba_career` が pedigree population 全体                               |
| 10  | add-trainer-stable-affinity | PG-history                                | own-history（調教師別）                         | **YES**                      | `partition by source, chokyoshi_code, grade_code` / `... target_race_id`（調教師の全履歴は要るが他レースの「出走馬」は不要）         |
| 11  | add-pacestyle               | parquet + rs_preds(R2/PG, same-race join) | within-race                                     | **YES**                      | `left join rs_preds on rs.race_id = ...`、window/group-by なし                                                                       |
| 12  | add-course-numerical        | parquet + baked lookup parquet            | static lookup                                   | **YES**                      | `read_parquet(lookup)` join on `keibajo_code, kyori, track_code`（PG なし）                                                          |
| 13  | add-relationship-r1         | PG-history                                | own-history + within-race                       | **YES**                      | `race_window partition by ... race_bango` + `row_number() over (partition by ... ketto_toroku_bango)`, `rh.race_date < bi.race_date` |
| 14  | add_kohan3f_going (JRA)     | PG-history(`--history-from-year`)         | own-history（馬場別5走avg）                     | **YES**                      | `partition by ketto_toroku_bango ... order by hist_race_date desc`, `hist_race_date < b.race_date`                                   |
| 15  | add_exotic_odds (NAR)       | PG(`nvd_o2/o3/o5` date range)             | within-race                                     | **YES**                      | odds 文字列を `partition by ... race_bango` で race 内 decode/normalize                                                              |
| 16  | add-banei-futan-class       | PG-history                                | own + within-race + **sire/damsire cohort**     | **NEEDS-REFACTOR**           | `horse_futan_career`=own、`sire_futan_career partition by sire_id, futan_class`=cohort                                               |
| 17  | add-banei-grade-career      | PG-history                                | own-history（grade別）+ within-race             | **YES**                      | `horse_grade_career partition by ... grade_letter` + race field avg                                                                  |

### 1.3 head-to-head の OOM 真因（per-race 議論の核心）

h2h は **pairwise だが計算形は race-scopable**（pair は常に同一 target race の co-starter のみ。
1 レース ~18 頭 → 18×17/2 ≈ 153 pair）。`current_pairs` は `current_field` の self-join に
`cf2.ketto_toroku_bango > cf1.ketto_toroku_bango` を課しレース内に閉じる
（`add-head-to-head-features.py:169-183`）。

OOM の原因は per-race 集計ではなく、`stage_pair_history`（L84-114）が
**race 絞り込みの前に全期間 pairwise テーブルを materialize** する点:

```sql
-- 全 race_history の自己 JOIN を「target 絞り込み無しで」先に作る
create or replace temp table pair_history as
select h1.source, h1.race_date, h1.ketto_toroku_bango as horse_a,
       h2.ketto_toroku_bango as horse_b, h1.finish_position - h2.finish_position ...
from race_history h1 inner join race_history h2 on (同一レース) and h2.ketto > h1.ketto
-- docstring: 「JRA 全期間で約 150M 行想定」(L88)
```

`stage_target_races`（L117）の filter は `current_field` / `current_pairs` だけを縮める。
`pair_history` 自体は無条件に全期間を走るため、ここで 7.4GiB を使い切る（doc 05 Phase 3 で OOM 実測）。

**doc 05 Phase 4 の修正＝まさに「intra-layer per-target-race scoping」**:
`target_races` を `current_field` に inner join して `O(全期間²) → O(当日レース²)` に縮小。
これで NAR 60 races が OOM なく 788s で完走（memory_limit 6GB / 4 threads / disk spill 併用）。
→ **per-race の発想は「container chunk 単位」ではなく「layer 内部 scope」として既に部分採用され、効いている**。

> 残課題（doc 05 で未対応）: `pair_history` の materialize 自体が依然全期間。
> 当該 race の `ketto_toroku_bango` 集合で `race_history` を先に絞ってから self-join すれば、
> pair_history も bound できる（§4.2 のチェックリスト参照）。今は `current_pair_aggregates` の
> left join 段だけが絞られている。

### 1.4 global 依存の所在（まとめ）

真に「他レースの馬・血統 cohort」を要するのは **5 layer のうちの cohort 部分のみ**:

- **sire/damsire pedigree cohort**: near-miss(#6) / baba-pedigree(#9) / banei-futan-class(#16)
  → 「同じ sire/damsire を持つ全馬」の累積。target race の 18 頭の sire/damsire 集合に history を
  絞れば bound 可能（per-race-cohort 化）。
- **trial-race set**: lineage(#7) → 「どのレースが trial か」を race_meta 全体で識別。
  target race の grade に紐づく trial 定義だけ評価すれば bound 可能。

それ以外（own-history 系・within-race 系）は **1 レース＋各馬 history で完全に閉じる**。

---

## 2. per-race スコープ化の可否と工数

### 2.1 計算可否

| 観点                                                                | 可否                                                                                                                                                                                                                                                                             |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 レース（~8-18 頭）＋各馬の過去走履歴のみで全 layer を計算できるか | **可能**。own-history 系・within-race 系はそのまま閉じる。global cohort 系（5 layer）も「target race の sire/damsire/trial 集合」に history を絞れば近似でなく**厳密に**計算できる（cohort の定義自体が「同 sire の全馬」なので、target に登場する sire だけ集めれば値は同一）。 |
| 真に global で per-race 不能な layer                                | **無し**。within-race 相対も同一レース内で閉じる。dataset 全体の percentile を取る feature は確認されなかった。                                                                                                                                                                  |
| `predict_upcoming.py` の race 列挙を 1 race に絞れるか              | base build / layer に race filter arg が無いため**現状コード経路では不可**（§2.3）。                                                                                                                                                                                             |

### 2.2 ただし「計算できる」≠「per-race 実行が得」

per-race 化が**計算上**可能でも、**支配コスト（21y per-horse history scan）は per-race にしても消えない**:

- own-history 系 layer（#3,4,5,10,13,14,17）は「その馬の 21 年分」を要する。1 レース 18 頭でも
  18 頭分の 21y history を Postgres から引く必要があり、これを 48 レース個別に実行すると
  history scan が 48 回分散発生（後述 §3.3 で Neon コスト評価）。
- base build（`finish_position_features_duckdb.py`）は history を target 馬で絞らず全 population を staging
  （§0 の根拠）。per-race にするには base build の history CTE を target 馬の semi-join に書き換える
  大改修が必要で、これは「精度を変えない」保証が要る（feature 値が history population に依存しないことの検証）。

### 2.3 race filter arg の現状（既存経路の有無）

| script                               | race filter arg                                                                                                                                                                                                                  | 確認                                                  |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `finish_position_features_duckdb.py` | **無し**。`--target-date` / `--days-ahead` / `--from-date` / `--to-date` のみ（L211-226）。`--keibajo-code` / `--race-bango` は `--realtime-odds` parquet の schema 説明（L259）に現れるだけで、target 絞り込み arg では**ない** | `argparse` 全 add_argument を grep 済                 |
| 全 layer script                      | **無し**。`--input-dir` / `--output-dir` / `--pg-url` / `--from-date`（+ layer 固有 `--config`/`--category`/`--course-lookup`）のみ                                                                                              | `pipeline_args.build_layer_argv` + 各 script argparse |
| `predict_upcoming.py` / serve        | `category` / `runDate` / `daysAhead` / `mode`（`serve.py:111` `parse_predict_params`）。**race 粒度のパラメータは無し**                                                                                                          | serve.py 精読済                                       |

→ **per-race を回すには「新 arg（`--keibajo-code` + `--race-bango`、または `--race-id`）」を base build と全 layer に追加し、
serve の `parse_predict_params` にも race パラメータを追加する**必要がある。工数は §4.2。

---

## 3. メモリ・時間・Neon 見積り（コード/SQL からの論理推定。実行はしていない）

### 3.1 メモリ

| シナリオ                                                                    | DuckDB peak メモリ（論理推定）                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 現行 per-category（doc 05 実測）                                            | h2h `pair_history` ~150M 行（JRA）/ ~18M pair（NAR）→ 修正前 7.4GiB で OOM。修正後 memory_limit 6GB + disk spill で完走                                                                                                                                           |
| per-race（B: layer 内部 scope を全 layer 展開、実行は per-category のまま） | h2h は target_races=全レースのまま history 全 scan だが、`pair_history` を target 馬で絞れば peak が大幅低下。near-miss/baba 等の cohort も target 集合に絞れば中間テーブル縮小 → **6GB limit に余裕**で収まる見込み                                              |
| per-race（A: 1 レースを container 実行単位に）                              | 1 レース 18 頭の pairwise は ~153 pair で trivial。**しかし base build の history staging（全 population scan の中間テーブル）は per-race でも発生**するため、メモリ削減効果は h2h 集計部に限定。base build を target 馬 semi-join に改修して初めて 18 頭分に縮む |

要点: **OOM したのは h2h の集計中間テーブル**であり、**base build の history scan は元々 spill で吸収できていた**。
A（container per-race）はメモリには効くが、**それは base build 改修ありきで、Neon コストとのトレードオフが悪い**（§3.3）。

### 3.2 時間

doc 01 §3 / doc 05 実測:

- per-category NAR: base build ~35s + layer chain ~120-180s + score/UPSERT ~40-90s ≈ **210s**。
- doc 05 NAR 60 races: 修正後 **788s（~13.1 min）**（disk spill 併用で I/O 増、15 分枠ギリギリ）。
- **wall-clock は当日レース数にほぼ非依存**（doc 01 §7「per-race time is O(1), total is O(layer_chain_cost)」）。

per-race（A: container 1 レース単位）の wall-clock:

- 1 レース feature build = base build の **history scan が支配**（target 馬 semi-join 改修なしなら ~30-180s が
  レース数分繰り返される）。Queue `max_concurrency=3` 並列でも、48 レース ÷ 3 × (build+score) ≫ per-category 並列。
- → **per-race は wall-clock も悪化**（history scan を N 回払う）。改修して 18 頭 semi-join にしても
  per-race の固定 overhead（subprocess 起動 ×14 layer ×48 races、PG ATTACH ×多数）が無視できない。

### 3.3 Neon 負荷（fan-out vs monolithic）— `e84b078` regression 評価

| 方式                                                        | Neon scan 量（論理）                                                                                                                                                                                      | `e84b078`（Neon コスト削減）regression?                                                                                                                                                                                                         |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 現行 monolithic（per-category、history 全 population 1 回） | 21y × 全馬 × 14 layer ぶんの read を **カテゴリあたり 1 セット**（同 PG ATTACH 内で各 layer が再 scan するが「当日 1 回」）                                                                               | 基準（doc 04 §3 で「21y scan は不可避だが pre-wake で吸収」と受容済）                                                                                                                                                                           |
| **per-race fan-out（A）**                                   | 各レースの container が **21y history を独立に scan**。base build を target-馬 semi-join に改修しても、48 レース × 各馬 history を 48 回別々の Neon round-trip で引く。PG ATTACH/接続も 48×（layer 数）回 | **重大 regression**。Neon compute time が **概ね レース数倍** に増える。idle wake も増え `e84b078` が削った idle コストを再発生。**doc 04 §3a の「当日出走馬に絞っても scan 量は変わらない」結論と整合し、per-race fan-out は Neon コスト最悪** |
| per-category + layer 内部 scope（B）                        | monolithic と同じく **カテゴリ 1 セット**。layer 内部の scope は DuckDB 側の中間テーブル縮小であり Neon read 量は不変（むしろ pair materialize 縮小で僅かに減る可能性）                                   | **regression 無し**。Neon read は現行と同等                                                                                                                                                                                                     |

**結論（Neon 観点）**: per-race fan-out（A）は **Neon コストを数十倍にし、`e84b078` を真っ向から打ち消す**。
monolithic / B は Neon read を増やさない。→ **Neon コストの観点でも per-race chunk 単位は不採用**。

---

## 4. CF 適合アーキ案

### 4.1 推奨（B）: 実行単位 per-category のまま、layer 内部を全 layer per-target-race scope

doc 03/04 で確定済みの `Cron → Queue → Container（held /predict）→ Neon UPSERT` を維持し、
**feature pipeline 側を 2 点強化**するだけ。container chunk 単位は変えない。

```
Cron(03:00 NAR/Ban-ei, 09:30 JRA)
  → Queue finish-position-predict-jobs（per-category message、現行 doc 03 のまま）
    → Container DO（standard-4 / memory_limit 6GB / 4 threads / disk spill）
       held /predict?category=...&runDate=...  ← renewActivityTimeout で 15 分枠維持
         base build（per-category, target window）
         layer chain（各 layer が target_races の race-key 集合 / sire 集合に history join を絞る）★強化点
         score（Python per-race ループ）
         Neon UPSERT（idempotent）
```

★強化点:

1. **h2h `pair_history` を target 馬で先絞り**（doc 05 で `current_pair_aggregates` は絞ったが
   `pair_history` materialize 自体はまだ全期間）。
2. **cohort 系 4 layer（near-miss / baba-pedigree / banei-futan-class / lineage）に同型 filter**:
   target parquet の sire/damsire/trial 集合を temp table 化し、cohort window の前段で history を絞る。

これで JRA leg（最重・E-top2 込み）のメモリ余裕が増え、788s の 15 分枠マージンを広げられる。

### 4.2 R2 feature-cache の per-race での効き方

現行 `feat-cache/{category}/{runDate}/features.parquet`（`serve.py:267`, doc 04 §3A）は
`mode=full`（21y scan して R2 へ書く）/ `mode=rescore`（R2 から読み odds だけ更新して再 score、Neon 0）の 2 モード。

- **per-category cache（現行のまま）が最適**: 1 ファイル read で全レース再 score でき、race 時間帯の
  `mode=rescore` cron が Neon を触らない。
- per-race cache（`feat-cache/{category}/{runDate}/{keibajo}/{race}.parquet`）は **R2 PUT/GET が
  レース数分（×48）に増える**だけで利点が無い（rescore はどのみち全レース再 score したい）。
  → **per-race cache は不採用**。cache 粒度も per-category 維持。

### 4.3 もし将来 A（per-race chunk）が本当に必要になったら

唯一の正当化シナリオ = 「JRA leg が 15 分枠を超える」場合の**水平分割**。その場合でも
**per-race ではなく per-venue（`PREDICT_KEIBAJO`、doc 03 §2 の in-reserve lever）が先**。
per-venue は ~10-20 units で history fan-out が venue 単位（per-race の 1/数十）に留まり、Neon regression が緩い。
per-race は最後の手段で、§4.4 の改修と Neon コスト増を覚悟する場合のみ。

### 4.4 実装工数（ファイル単位、A=per-race を仮に実装する場合）

| 対象                                                        | 変更内容                                                                                                                                                           | 工数感                                              |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `finish_position_features_duckdb.py`                        | `--keibajo-code` + `--race-bango`（or `--race-id`）arg 追加。base build の rec/se staging と全 history CTE を target 馬 semi-join に書き換え（精度不変の検証必須） | **大**（2418 行、history CTE 多数。精度検証が重い） |
| 全 layer 17 本                                              | 同 race filter arg を受け、history を target race-key / 馬集合に絞る                                                                                               | **大**（17 ファイル × test 同時更新、coverage 95%） |
| `pipeline_args.py` / `pipeline_runner.py`                   | argv builder に race arg を伝播                                                                                                                                    | 中                                                  |
| `serve.py parse_predict_params` + handler                   | `keibajo` / `raceBango` パラメータ追加・validation・test                                                                                                           | 中（test_serve 更新）                               |
| Worker（dispatch.ts / container-class.ts / Queue producer） | Cron が race key を列挙し per-race message を enqueue。`max_instances` / `max_concurrency` 引き上げ                                                                | 中                                                  |
| **Neon コスト**（運用）                                     | fan-out で compute 数十倍。pre-wake / budget gate の再設計                                                                                                         | **要 USER 承認**（`e84b078` regression）            |

→ A は **総じて「大」工数 × Neon regression** で、得るもの（メモリ）は B で達成済み。**非推奨**。

### 4.5 実装チェックリスト（推奨 B）

- [ ] `add-head-to-head-features.py` `stage_pair_history` を target race の `ketto_toroku_bango` 集合で
      先絞り（`race_history` を target 馬に inner join してから self-join）。test（`tests/test_*` 同ディレクトリ）追加。
- [ ] `add-near-miss-features.py`: target parquet の sire_id/damsire_id 集合を temp table 化し、
      sire/damsire cumulative window の前段で history を絞る。test 追加。
- [ ] `add-baba-pedigree-affinity-features.py`: 同上（sire/damsire × baba_cond cohort）。
- [ ] `add-banei-futan-class-features.py`: 同上（sire/damsire × futan_class cohort）。
- [ ] `add-grade-race-lineage-features.py`: target race の grade に紐づく trial 定義のみ評価するよう
      CROSS JOIN を絞る。test 追加。
- [ ] 各 layer 修正で **feature 値が現行 monolithic と bit 一致**することを小サンプルで検証
      （cohort 定義は「同 sire の全馬」なので target に出る sire だけ集めれば値は不変 — これを test で固定）。
- [ ] `_resource_defaults.py` の memory_limit 6GB / 4 threads / disk spill はそのまま（doc 05 済）。
- [ ] JRA + E-top2 leg を held /predict で ≥3 回、Mac parity、SIGTERM 無し、15 分枠内（doc 03 §6 probe）を再測定。
- [ ] Neon read 量が現行と同等（regression 無し）を確認。
- [ ] Python coverage 95%（`pyproject.toml --cov-fail-under=95`）維持。lint/format/tsc。`--no-verify` 禁止。

---

## 5. 代替案比較

| 案                                                         | chunk 単位 | OOM 解消                    | wall-clock                | Neon コスト                   | 工数                      | CF 仕様適合           | 採否                                              |
| ---------------------------------------------------------- | ---------- | --------------------------- | ------------------------- | ----------------------------- | ------------------------- | --------------------- | ------------------------------------------------- |
| **A. per-race container**                                  | 1 レース   | ◎（base build 改修込み）    | ✕（21y scan を N 回）     | ✕✕（数十倍、e84b078 regress） | 大 + 承認要               | △（message 過多）     | **不採用**                                        |
| **B. per-category + 全 layer 内部 per-race scope（推奨）** | カテゴリ   | ◎（h2h 済 + 残 layer 展開） | ○（現行 788s に余裕）     | ◎（現行同等）                 | 中（layer filter + test） | ◎（doc 03/04 のまま） | **推奨**                                          |
| C. per-venue（PREDICT_KEIBAJO）                            | 会場       | ○                           | △（venue fan-out 中程度） | △（venue 倍）                 | 中（filter arg）          | ○                     | 予備（JRA が枠超過した時の最初の lever）          |
| D. per-category 逐次 + 層ごと memory_limit/spill           | カテゴリ   | ○（doc 05 で実証）          | ○                         | ◎                             | 小（済）                  | ◎                     | 既に基盤として採用済（B はこの上に scope を足す） |

---

## 6. 推奨（実時間最短 × CF 仕様適合）

**推奨 = B（per-category 実行単位を維持し、layer 内部の per-target-race scoping を全 global layer に展開）＋ D の memory 規律を土台に。**

根拠:

1. **支配コストは「当日レース数」でなく「21y per-horse history scan」**（base build が history を全 population
   staging する事実 = `finish_position_features_duckdb.py` L421-422/629-630, L827 `history_start = today−21y`）。
   per-race chunk（A）は history scan をレース数分払い直すため wall-clock も Neon コストも悪化する。
   doc 01 §3/§7・doc 04 §3a がこの adjudication を既に記録。
2. **OOM の真因は h2h の `O(全期間²)` pairwise**であり、これは doc 05 で **layer 内部の per-target-race scope**
   により解消済み（NAR 60 races 788s 完走、6GB limit + spill）。**「per-race」の本質的な価値は container 分割でなく
   layer 内 scope にある**。
3. 残る global layer（near-miss / baba-pedigree / banei-futan-class / lineage の cohort 部）も同型 filter で
   bound でき、**Neon read を増やさずに** JRA leg のメモリ余裕を拡大できる。
4. CF 仕様（15 分枠・メモリ枠・held /predict）には doc 03/04 の per-category アーキで既に適合。
   per-race へ砕く必要は無く、砕くと message 過多・Neon regression を招く。

**実時間最短の道筋**: B は新しい container 分割を要さず、既存 14-layer chain に filter を足すだけ。
doc 05 の OOM fix（h2h）と同型の改修を 4 layer に展開し、`pair_history` の先絞りを 1 件足せば、
JRA leg を 15 分枠に安全に収めつつ Mac 非依存の per-category CF 予測が完成する。

> USER への含意: 「レースごとに予測する」という要望は、**実行を 1 レースずつ container に投げる**意味なら
> Neon コスト面で逆効果。一方、**各特徴量の計算を 1 レース（の出走馬と血統）に scope する**意味なら
> 既に正しい方向に進んでおり（h2h で実証済）、残り 4 layer への展開で完了する。後者を推奨として採用する。
