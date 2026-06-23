# nvd_um vs nvd_nu: NAR 血統カバレッジ崩壊の根因 (2026-06-23)

## 発見

着順予測の血統パイプライン (`pedigree_staging.py`) が `nvd_um` (JV-Data mirror) を使用していたが、
NAR 馬の本来の血統ソースは **`nvd_nu`** (N-Data 競走馬マスタ地方) であった。

| テーブル | 説明                             |     行数 (local PG)     |          pipeline 使用状況          |
| -------- | -------------------------------- | :---------------------: | :---------------------------------: |
| `nvd_um` | 競走馬マスタ (JV-Data 経由)      | **0** (2026-06-23 時点) | `pedigree_staging.py` で priority 2 |
| `nvd_nu` | 競走馬マスタ地方 (N-Data native) |       **120,159**       |             **未使用**              |
| `jvd_um` | 競走馬マスタ (JV-Data, JRA 主体) |         ~87,000         | `pedigree_staging.py` で priority 1 |

## なぜ nvd_um は空になったか

`nvd_um` は JV-Data が提供する NAR 馬のミラーテーブル。JV-Data は JRA のデータ配信システムであり、
NAR 馬の登録は JRA に出走登録があるか、JRA 馬の血統に関係する場合のみ行われる。
2023 年以降、NAR 専属馬の新規登録が JV-Data 側で減少し、カバレッジが急落:

- 2022 年: 98.0% → 2023 年: 81.8% → 2024 年: 50.4% → 2025 年: 29.3% → 2026 年: 20.9%

ローカル PG の `nvd_um` が 0 行なのは、N-Data sync が `nvd_nu` のみを対象としていた可能性が高い。
Neon (本番) の `nvd_um` にはデータが存在するが、カバレッジは上記の通り崩壊中。

## 影響: Signal4 SERVE-BLOCKED の根因

`goal-signal4-serve-coverage-gate.md` で文書化された Signal4 の serve-block:

- WF: place2 +0.184pp (LB95 +0.191pp) — 本物の改善
- Serve: カバレッジ ~22% (nvd_um 依存) — train/serve gap -77.7pp で deploy 不可

**nvd_nu を使えばカバレッジ問題が解消する可能性がある。**

## nvd_nu の構造

`nvd_nu` は `nvd_um` / `jvd_um` と同じ `horseMasterColumns` スキーマ (`schema.ts:83`):

- `ketto_toroku_bango`: 血統登録番号 (primary key)
- `ketto_joho_01a`: 父馬 ID (sire)
- `ketto_joho_05a`: 母父馬 ID (damsire)
- `ketto_joho_01b` / `ketto_joho_05b`: 父名 / 母父名

## 修正方針

1. `pedigree_staging.py`: `nvd_nu` を priority 3 で UNION に追加 (jvd_um=1, nvd_um=2, nvd_nu=3)
2. `finish_position_features_duckdb.py`: `stage_um_table(con, "source.nar_nu", "nar_nu", "nvd_nu")` 追加
3. カバレッジ再検証 → Signal4 serve-coverage gate 再評価
4. 特徴量再構築 → blind holdout → deploy 判定

## 教訓

- データパイプラインのテーブル選択は「正しいソースか」を定期的に検証する
- JV-Data (JRA 系) のテーブルで NAR データを参照する構造は、カバレッジが時間とともに劣化する構造的リスクがある
- N-Data native テーブル (`nvd_*`) が存在する場合、NAR データには常にそちらを優先する
