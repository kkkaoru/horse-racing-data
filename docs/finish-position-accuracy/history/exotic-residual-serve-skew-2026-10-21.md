# 馬連・3連複 rank residual の学習／配信ずれ (2026-10-21)

[単勝オッズ禁止キャンペーン](win-odds-ban-retrain-campaign-2026-10-16.md) で追加した
`exotic_umaren_p2_rank_residual` / `exotic_sanrenpuku_p3_rank_residual` は、学習では JV-Data の
確定オッズ (`data_kubun='5'`)、配信では hot worker のその時点の最新オッズから計算する。配信時点の値を
再現して、ずれの大きさと精度への影響を測った。

## 計測方法

- 配信時点のオッズ: R2 `pc-keiba-odds-archive` の `odds-snapshots/v1/{jra,nar}/YYYYMMDD/<race_key>/<fetchedAt>.json`
  (JRA は 2026-07-11 以降)。各レースで「発走 − 配信リード」以前で最新のスナップショットを使い、
  配信と同じ `predict_lib.market_deviation.compute_race_features(pools_from_latest(latest))` で residual を計算した。
- 配信リードは `race_finish_position_model_predictions.prediction_generated_at` の実測中央値:
  JRA 58 分、NAR 47 分、ばんえい 44 分。
- 精度は本番の `predict_upcoming.score_races` で採点した (Prophet・MPX など本番構成込み)。
  特徴量 store は tansho-ban キャンペーンの 2026 年分 (JRA は前日までの騎手・血統特徴量に直したもの)。

## ずれの大きさ (配信時点の値と確定値の相関)

| カテゴリ | レース | umaren | sanrenpuku |
| -------- | ------ | ------ | ---------- |
| JRA      | 816    | 0.58   | 0.63       |
| NAR      | 3,240  | 0.37   | 0.46       |
| ばんえい | 401    | 0.25   | 0.31       |

NAR は発走 15 分前の値でも 0.53 / 0.59 で、精度は 47 分前と同じだった。差は締切直前の資金によるもので、
配信を遅らせても縮まらない。

## カテゴリ別の結論

- **JRA: residual を外して配信 (`-noexo1`, 2026-10-21 リリース)**
  - 主力モデル単体の top1 (816 races): 確定値で 27.21%、配信時点の値で 26.10%、residual 無しで再学習 27.82%
    (配信実態比 +1.72pp, CI95 +0.12〜+3.31。月別でも鮮度の高いスナップショットだけでも正)。
  - 本番 39 本を同一レシピ・residual 無しで再学習。全体比較:
    配信時点の値で top1 +0.74pp / 1着馬3着内 +1.10pp / top3box +0.61pp、確定値で現行が最も有利な条件でも
    top1 +0.69pp (CI95 −0.23〜+1.57)。
  - 再混入防止: コンテナの `assert_no_banned_market_features` が JRA モデルの residual を拒否し
    (`MARKET_DEVIATION_DISALLOWED_CATEGORIES`)、`train_jra_cell_models` と learner の JRA 学習データからも除外する。
- **NAR: 現行のまま**
  - 配信時点の値は確定値に比べ top1 −1.57pp だが、residual 無しの再学習 (XGB 13 本 + set-transformer 3 seed) は
    配信時点の値でも top1 −1.70pp、2026 全体で −3.09pp と悪化。
  - 7〜8月のずれを学習データに注入した再学習 (`-exoaug1`) も 9〜10月の 1,291 races で top1 −2.71pp。
  - 対照として同一レシピの再学習は本番比 +0.54pp (有意差なし) で、上の悪化は再学習のばらつきではない。
- **ばんえい: 現行のまま** (401 races で residual 無しは top1 −2.0pp / top3box +2.0pp と割れた)。

## 評価への影響

確定オッズの residual で採点したオフライン評価は、配信実態より楽観的になる (NAR で top1 約 1.5pp、
JRA で約 1pp)。手法間の比較は同じ条件なので結論は変わらないが、絶対値を本番の期待値として使わないこと。

## 付随作業

- JRA clef rerank は配信順位をリクエストに含むため、`-noexo1` の順位で hosted clef-flash を再実行して再承認した
  (`apps/finish-position-clef-rerank/README.md` v4)。
- モデルを差し替えたら、manifest の各 artifact を R2 `pc-keiba-features-archive/ci-artifacts/sha256/<sha256>` に
  登録する (`.github/deploy/README.md`)。未登録だと CI の `models.py` が 404 で止まり、全 Worker のデプロイが止まる。
- 作業ディレクトリ: `.cache/noexotic-retrain` (JRA/NAR/ばんえい の residual 無し再学習)、
  `.cache/exoaug-retrain` (NAR ずれ注入)、`.cache/control-retrain` (NAR 対照)。

## 追記: 2026 年の rs\_\* 欠損

2026 年の特徴量 store では rs*\*（脚質予測確率）が約 98% 欠損している（ローカル PG の
`race_running_style_model_predictions` は 2026-05〜07 の一部のみで、本番は Neon に書く）。上の 2026 年の比較は
両方とも rs*\_ 欠損のまま採点した。Neon の配信済み脚質予測から rs\_\_ を再構築して JRA を再比較しても
（`add-pacestyle-features.py --pg-url duckdb://<served snapshot>`）、`-noexo1` は 1着馬3着内 +1.59pp
（CI95 +0.12〜+3.06）、top1 +0.61pp で結論は変わらない。2026 年の行を学習に含める前に、2026 年の脚質予測を
ローカル PG に取り込むこと。そうしないと学習では rs\_\* が欠損、配信では埋まっている状態になる。
