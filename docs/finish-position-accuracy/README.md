# 着順予測精度改善 — Iterative Loop ログ

本ディレクトリは v8 iterative loop の精度改善履歴を体系的に記録します。 詳細プランは `/Users/kkk4oru/.claude/plans/imperative-riding-wave.md`。

## サブディレクトリ

- `history/` — 各 iter の試行結果 (1 iter = 1 MD ファイル)
- `buckets/` — per-bucket weak-spot analysis + delta レポート
- `experiments/` — 重要 lever ごとの研究記録 (TabM 等 research-only 含)
- `runbook/` — 運用 docs (production flip 手順等)
- `_templates/` — auto-record 用テンプレート (`render-iteration-history.ts` が消費)
- `legacy/` — 過去の散在 MD 移行先 (Stage 0A.5 で git mv)

## Leaderboard (auto-generated)

<!-- LEADERBOARD:START -->

| date | iteration | lever | model_version_jra | model_version_nar | jra_top1 | jra_place2 | jra_place3 | nar_top1 | nar_place2 | nar_place3 | status |
| ---- | --------- | ----- | ----------------- | ----------------- | -------- | ---------- | ---------- | -------- | ---------- | ---------- | ------ |

<!-- LEADERBOARD:END -->

## 評価指標 (User v7.1)

- top1: 1 着的中率
- place2: 2 着的中率
- place3: 3 着的中率
- top3_box: 上位 3 着箱推し的中率
- 4 軸すべてを評価軸とし、 top1 単独最適化は禁止

## Legacy MD inventory

Stage 0A.5 で repo root から `legacy/` に移行した既存の着順予測関連 MD ドキュメント (歴史的参照用):

| ファイル                                                                                           | 内容                                                     |
| -------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| [legacy/FINISH_POSITION_PREDICTION_DESIGN.md](legacy/FINISH_POSITION_PREDICTION_DESIGN.md)         | 着順予測パイプラインの設計原則 (精度低下禁止原則等)      |
| [legacy/FINISH_POSITION_MODEL_V6_STACKED.md](legacy/FINISH_POSITION_MODEL_V6_STACKED.md)           | JRA `jra-cb-v6-stacked` 仕様書                           |
| [legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md](legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md)           | v7-lineage (JRA / NAR / Ban-ei) アーキ・walk-forward 21y |
| [legacy/PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md](legacy/PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md) | place2 / place3 改善検証履歴 (empirical 不可行と判定)    |
