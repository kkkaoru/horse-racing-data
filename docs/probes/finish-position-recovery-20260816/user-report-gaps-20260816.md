# Gaps in what the user has been told (2026-08-16 13:21)

Advisor already said: 80/80 held; 2 of 5 landed after post;
cause unknown; morning deploy; only production write is 80
feat-cache PUTs.

That list is **directionally right** and still too soft in
four places.

## Say these, or the one-pager is the better text

1. **発走前の着地は 0。** 「5件中2件が発走後に着地」は事実だが、
   馬券の時計では **0/5**。01/02 は +65 分（発走+24）。07/01 は
   **+227 分**（12:57）。後着地を「動いている」と足すなら、
   **間に合っていない** を同じ文に置く。
2. **80/80 は本番 container ではない。** Mac host one-shot。
   明朝 deploy しても、今朝の 80 は再現しない。
3. **9 commit を全部入れても直る保証がない。** 乗れるのは
   条件付き 2 件。0006 を入れても体重 POST は tracking id を
   付けない。明朝の作業は **観測の土台** であって修正ではない。
   「0006 と 9 件を入れて 09:10 で検証」は、失敗しても
   パッチが悪いとは言えない。
4. **feat-cache PUT は本番書き込み。** 「操作ゼロ」は撤回済みで
   正しい。加えて、その 80 件は体重 rescore が HIT する対象。
   品質は optimize。fix は PUT していない。

## Smaller, still worth one line

- 市場オッズは全件欠ける。今日の stamp は market-free。
- `predicted_top1/top3_prob` NULL は仕様。欠けではない。
- Ban-ei はまだ空欄。
- 04/01 と 01/01 は 13:11 時点でも朝の stamp のまま。
  「2件着地」で残りが後から揃う保証はない。

## Not a user item

GraphQL の帰属、cron 誤読、Last-Modified＝未読、の撤回は
内部の失敗。ユーザーには「原因はまだ分からない」で足りる。

Use `user-report-20260816.md`. Do not soften the first paragraph.
