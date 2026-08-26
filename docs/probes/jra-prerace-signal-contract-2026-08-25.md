# JRA 実運用時点 signal contract (2026-08-25)

目的はデータ追加ではなく、発走前に再現できるsignalだけをranker/defer modelへ供給すること。欠損・時刻不明値はfail closedし、post-race値で補完しない。

| Signal                     | Current source/status                                                                                                                                                             | Causal contract                                                                                                      | Action                                                                                                                                                    |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| timestamp付きodds snapshot | `sync-realtime-data-hot`が`fetchedAt`付きで分単位取得しR2へ保存。Containerの`realtime_odds_fetcher`もexpected timestamp一致を検証。JVD `jvd_o1`–`jvd_o6`の単独効果は過去probe済み | snapshot timestamp `< scheduled start`; final/post-start oddsは禁止                                                  | servingは既存timestamp contractを利用。joint modelのhistorical trainingは同等snapshotを復元できる期間だけ対象。不一致・欠損は`odds_complete=false`でdefer |
| 馬体重・増減               | `jvd_se.bataiju`、`zogen_sa`。既存featuresの`weight_diff_from_avg`、`weight_trend_5`、`weight_volatility_5`へ反映済み                                                             | prediction snapshot時点で公表済みの場合のみ。未公表を過去値/0で補完しない                                            | 269-feature joint modelへ入力済み。audit列にも`weight_diff_from_avg`を保持                                                                                |
| 騎手変更                   | realtime JRA parserが`騎手変更`を検出。current jockey identityと変更前列はJVDに存在                                                                                               | current jockeyが確定したsnapshotだけを使用。同race後情報は不要                                                       | current jockeyのcareer/recent/venue/distance statsとidentityを利用。変更flag単独は既存probeでredundant                                                    |
| 当日馬場推移               | weather hourly/prior3、track condition、track bias featuresが存在                                                                                                                 | 発走時刻より前の観測だけ。当日future hourlyは禁止                                                                    | causal weather v2はfail-closed shadow。精度gate未通過のためselected modelへ強制採用しない                                                                 |
| パドック／直前気配         | `premium_paddock_bulletins`にhorse number、favorite/value group、comment/evaluation、`fetched_at`を保存。履歴はmigration導入後の期間に限定                                        | `fetched_at < scheduled start`、fetch完了時だけabsenceをnegative selectionとして扱う。free textの推測sentimentは禁止 | `premium-paddock-signals.ts`でtimestamp検証、latest pre-start選択、incomplete時nullのcausal contractを実装。十分な履歴が貯まるまではshadow-only           |

## Serving requirements

1. `odds_complete=false`、時刻不明、部分race snapshotではalternate/defer modelを無効化する。
2. payoutはtraining label/sample weight/evaluation strata限定。current-race inference inputにはしない。
3. 主順位とalternate候補は別contractとし、alternateは`additional_top5_candidates`へ保存する。主順位の`predicted_rank`は変更しない。
4. signal availabilityとsnapshot hashをshadow recordに保持し、同一snapshot比較だけを評価する。

## Unavailable data

パドック映像・歩様の連続量・心拍等は新規外部データ契約が必要。既存premium bulletinはfavorite/value選択とavailabilityだけを構造化し、短い履歴でcomment textの意味を学習したり、欠損を0へcoalesceしない。JVD odds time-seriesは過去に単独効果を検証済みであり、timestamp整合を解決せず再学習しない。
