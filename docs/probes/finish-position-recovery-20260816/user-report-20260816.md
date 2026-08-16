# 2026-08-16 ユーザー向け1枚

本番は触っていない。container `0c76062e`。viewer `06fd3c24`。

## 1. 今日の運用

予測は守れた。体重反映は届かなかった。観測した 5 件（04/01 07/01
01/01 01/02 35/01）のうち着地は **1 件**。それも発走 +24 分。
Ban-ei は 13:54 以降。

| 項目            | 結果                                                                        |
| --------------- | --------------------------------------------------------------------------- |
| 朝の着順予測    | Neon **80/80**（940頭）。Mac host one-shot。container ではない              |
| 体重 rescore    | **5 件中 1 件のみ**着地。それも発走に間に合わない                           |
| 04/01 09:40     | trigger 09:10。Neon 07:07。間に合わず                                       |
| 07/01 09:50     | trigger 09:10。Neon 05:04。間に合わず                                       |
| 01/01 10:00     | trigger 09:20。Neon 05:03。間に合わず                                       |
| 01/02 10:30     | trigger 09:49 → Neon **10:54**（発走 +24 分）。唯一の着地。間に合っていない |
| NAR 35/01 12:35 | trigger 11:44。発走後 12:36 も Neon **05:31**（4 件目）。LIVE 9/10          |
| Ban-ei 14:25    | （13:54 以降に追記）                                                        |

市場オッズは全件欠ける。`predicted_top1/top3_prob` NULL は仕様。

## 2. 分かったこと / 分からないこと

分かったこと

- queue は ACTIVE。08-12 の paused ではない
- Worker は呼ばれている
- 今夜の **9 commit を全部入れても、この体重反映は直る保証がない**
- 乗れるのは条件付きで `85bfba82` と `67440b8b` だけ
- `41676f7c` は予測行を書かない。MISS の層時計だけ
- 01/02 の 503 本文は **max_instances**。起動待ちではない
- NAR 着弾と同じ分の LIVE **9** / cap **10**。枠は満杯ではなかった
- GET は R2 Last-Modified を変えない
- D1 migration 0006 は未適用。適用しても体重 POST は tracking id を付けない
- カテゴリ非依存。JRA 3 件 + NAR 1 件が同じ形

分からないこと

- 不着 4 件の consume が始まったかどうか
- HIT か MISS か
- なぜ 01/02 だけ後から通ったか
- 根治のコード変更

索引: `overnight-fp-index.md`。結論: `weight-rescore-cause-unknown-20260816.md`。9件: `nine-commits-do-not-fix-weight-stall-20260816.md`。

## 3. 明日の順

開催中（〜20:50）は deploy しない。A8 本生成は 21:00–22:00 oversea で **container を使わない**。窓の中身: `deploy-windows-tonight-20260816.md`。既定は明朝（stall を試せる唯一の窓）。手順: `tomorrow-morning-runbook-20260817.md`。

1. ユーザー確認後、枠の窓を開く。既定は明朝
2. **先に** queue consumer が書く per-message 結果（開始 / ack / retry / Python 前に死んだ）。held fetch が返らなくても残す。0006 の表はそれではない
3. 0006 適用は地方終了後か明朝。開催中は canary が5分毎に queue を汚す
4. stall 検証で乗せるなら `85bfba82`、MISS なら `67440b8b`。見え方を足すなら `41676f7c`
5. 他の7件だけ入れて「直らなかった」は判定に使わない
6. `DAY_BASE_SPLIT_ENABLED` は同じ deploy に入れない

窓: `container-deploy-window-20260816.md`。0006: `migration-0006-apply-prep-20260816.md`。

## 4. 判断が要るもの

| 誰        | 何                                             |
| --------- | ---------------------------------------------- |
| ユーザー  | deploy 窓。明朝か、20:50+A8 の後か             |
| ユーザー  | 9件を載せるか。stall 用は2件だけ               |
| team-lead | split を触るか。今夜は触らない                 |
| 誰か      | per-message 結果の実装。今夜の commit には無い |

ロールバック先: `0c76062e`。緊急前: `953d086b`。
