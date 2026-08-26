# NAR発走前天候・複合cell router再検証（2026-08-24）

## 結論

精度modelの本番デプロイなし。複数の条件範囲を1つの複合cellとして管理し、cellごとに独立weightを学習するrouterを検証したが、事前ゲートを通過するcell/routerは確定できなかった。配信障害を直すWorkerコードだけは本番更新したが、model artifact、container image、MLflow registryは変更していない。

今後の固定採用基準は、高配当レンジのTop1–3平均が独立評価期間で`+1.000pt`以上であることとする。高配当がこの基準を満たしても、全体・人気通り・荒れ・通常、またはTop1〜Top5のいずれかに悪影響があれば、その候補をそのまま採用しない。悪化した範囲を発走前に確定する特徴だけで親cellから分岐できる場合に限り、子cellを追加して各子cellのweightを独立に再学習し、同じ独立評価ゲートを再適用する。分岐後も悪影響が残る候補、support不足の候補、凍結期間で`+1.000pt`を再現しない候補は棄却する。

## 本番routing表示契約の監査（2026-08-25）

2026-08-24のばんえい11Rはlocal `nvd_ra`で`grade_code=E`、発走20:10 JSTだった。Neonにはrouting対象の`banei-cb-v8-window2011-wf-15y`が09:42 JSTに10頭、本線`banei-cb-v9-sim-2011`が19:24 JSTに9頭存在したが、本番共有KV `pred:fp:v1:20260824:83:11`の実値は9頭すべてv9だった。したがってgrade-E cellは生成側で成立しても、KV writerの「最新model_version」選択で本番表示から上書きされていた。

最初の修理では、選択SQLをNARの`iter40-nar-settransformer-blend-v1`と、ばんえいgrade-Eのv8を優先する契約にしてWorker version `db536d81-43b2-4544-b4e1-1de65e7e6c7f`へ配備した。しかし配備直後に精度根拠を再監査すると、`cell_training_evaluations`のgrade-E保存行はmodel identityが空で、560レースのTop1/2/3が6.786%/2.857%/2.143%という壊れたrunner-level集計だった。旧routing採用commitにもgrade-E単独の改善量は無い。2026年の確定済みgrade-Eでv8/v9が両方存在するのは2レースだけで、v8のTop1/2/3=0%/0%/50%（平均16.667%）、v9=50%/50%/50%（平均50.000%）、差は`-33.333pt`だった。n=2なので将来効果の推定には不足するが、少なくとも`+1.000pt`採用根拠にはならない。

このためNAR Transformer優先と配信lane修理は残し、ばんえいv8優先だけを即時撤回した。focused test 22件、package full coverage 1,720件が再通過し、coverageはStatements 97.95%、Branches 95.02%、Functions 98.96%、Lines 98.46%、lint/tsc/formatも通過した。`wrangler deploy --containers-rollout none`でWorker version `968bfb89-89d1-4ca2-ab0d-f7a533567d7a`へ更新し、本番health `ok=true`を確認した。ばんえいKVは独立評価で基準を満たすcellが確定するまで従来の最新model選択、すなわち実運用上のv9を維持する。予測model artifact/imageとMLflow registryは変更していない。

同時にMLflow full syncを2026-08-17〜24へ再実行し、既存run 20件へfinish-position評価を記録した。ただし現行`serve_eval.fetch_fp_prediction_rows`はNeonの全候補行をmodel version別に評価するため、2026-08-24 NAR 45レースをTransformer 45レースとstage1 fallback 45レースとして重複保持する。これはmodel候補別精度であり、本番表示policy精度として合算してはならない。共有KVまたはviewerと同一のrouting選択を1レース1modelへ再現する評価契約を追加するまでは、MLflowのmodel別値のみを事実として扱う。

同syncで2026-08-23 NARの欠損も検出した。local結果は29/29レース確定済みだがNeonのNAR予測は0行、Cloudflare D1ではcoverage gap 950件、enqueue 174件、consume 174件に対してgeneration start 0、completion 0、full DLQ 127件、rescore DLQ 46件だった。ばんえいは同日に12レースのNeon予測が存在した。結果未着ではなく、NAR生成laneだけの一日障害である。

## 比較対象

- MLflowで確認できた実配信NAR iter40（2026-07-08〜2026-07-15、164レース）: Top1 29.878%、Top2 48.780%、Top3 61.585%、Top4 69.512%、Top5 76.829%。これは実配信精度の事実である。
- offline反実仮想の基準: 本番artifact clean188 XGB + clean113正規化 + byte-identicalなTransformer 3 seedをweight 0.50で融合した順位。
- MLflow実配信精度とoffline反実仮想差分は別物として扱い、offline値を本番実配信精度とは呼ばない。

## 天候データ

- Cloudflare R2 SQLからlocalへ2026-07-01〜2026-08-23を差分同期した。
- 54日、32,400行のうち31,800行はunchanged、600行をupdated、insertedは0行。
- 特徴量は発走時刻より前だけを使用し、直前3時間・6時間の気温、気温推移、降水、風速、突風を構成した。発走時刻以降の観測は除外した。
- 本番取得元はCloudflare、local学習・評価はCloudflareから差分同期したlocal DBという契約を維持した。

## loop 1: 従来routerの前方診断

2023〜2025で凍結した従来routerを、2026-07-01〜2026-08-23の結果確定済み2,040レースへ適用した。これは本番artifactによるoffline反実仮想であり、MLflow実配信値ではない。

| 指標                   | weight 0.50比 |
| ---------------------- | ------------: |
| Top1                   |     +0.0980pt |
| Top2                   |     -0.4902pt |
| Top3                   |     -0.1471pt |
| Top4                   |     -0.0490pt |
| Top5                   |     -0.1471pt |
| Top1–3平均（全体）     |     -0.1797pt |
| Top1–3平均（人気通り） |     -0.2144pt |
| Top1–3平均（荒れ）     |     -0.0687pt |

全体Top1–3平均の日付cluster bootstrap 95%区間は[-0.4227, +0.0342]pt。稼働した複合cellのいずれもTop1〜Top5・人気通り・荒れを同時に満たさなかった。

## loop 2: weight選択実装の見直し

従来実装はTop1〜Top3を単一utilityへ圧縮してweightを選んでおり、Top4・Top5の損失を選択時に直接制約していなかった。以下へ変更した。

- 1つのruleはvenueを親に、season/temp/rain/going等の複数component cellの組合せ範囲を持つ。
- NAR全体の単一weightは禁止し、最低fallbackもvenue固有cellとする。
- 各複合cellの候補weightをTop1〜Top5 × 全体/人気通り/荒れの15差分で検査する。
- 15差分の1つでも平均が負なら候補を棄却する。
- 人気通り・荒れは各30レース以上を必要とする。
- Top1〜Top3 utilityの保守下限が設定したfloorを超える場合だけ、親cellからweightを変更する。

2023→2024、2023〜2024→2025のrolling OOSで216設定をColima上で比較した。development通過は0件だった。

最良に近い設定（venue/disagree/market/temp3/distance、minimum 200、z=1.28、depth 5）でも、2024〜2025 pooledでTop5が全体-0.0074pt、荒れ-0.0329pt。2024単年ではTop5が全体-0.0146pt、荒れ-0.0652ptで、固定ゲートを満たさなかった。

通過0件時に古いrouter JSONが残らないよう、成果物を`status: no_development_candidate`、`rules: []`へ置換し、forward評価側もこの状態を拒否するようにした。

## loop 3: venue＋任意条件の複合cell

固定prefix階層を廃止し、venue＋任意1〜2条件を1つのcellとして扱う実装を追加した。2023年には153種類の条件集合、最低100レースかつ人気通り/荒れ各30件を満たすcellが5,162個あった。

- Bonferroni版: 310,560比較、補正後z=5.110。残った14 ruleはvenue親のみで全てw0.50、2024 activation 0。
- 2023候補生成・2024独立screen版: 34 rule、7 weight、2024 raw Top1–3平均は全体+0.0317pt、人気通り-0.0534pt、荒れ+0.3258pt。個別OOS通過childは0。

2025確認なし、本番変更なし。

## loop 4: 複数カテゴリをpoolするrange cell

1つのcellが複数カテゴリの学習範囲を管理できるよう、20種類のrangeを追加した。例はtemp3のmild+warm、rain3のdry+light、wind3のlow+mediumである。同じ元特徴の完全一致条件とrange条件を1つの複合cellへ重複投入しない。

2023から71 ruleを生成。2024 rawではTop1–3平均が全体+0.0122pt、人気通り-0.1445pt、荒れ+0.5539pt。完全な15平均差非負を満たすOOS cellは0で、2025確認なし。

## loop 5: venue別decision-tree range cell

手動binではなく、1つのleaf cellが複数の連続特徴範囲を直接管理するvenue別decision-tree routerを追加した。特徴は発走前に存在する距離、3h/6h天候、市場強度、model disagreement、頭数、surface、馬場、seasonだけで、winner/結果列と発走後天候を除外した。

2023平均差screenで6 leaf ruleを生成したが、2024 rawはTop1–3平均が全体-0.0171pt、人気通り-0.0377pt、荒れ+0.0543pt。venue親weightにも同じOOS screenを適用する階層整合性修正後、通過venue 0、通過leaf 0、activation 0となった。2025確認なし。

以上4方式で、改善を確認できたcellは0。精度改善を示す成果物がないためデプロイしていない。

## loop 6: weight oracleと荒れ確率cell

21 weightのうち結果を知った上で勝馬順位を最小にする非実用oracleを計算した。これはデプロイ可能精度ではなくweight gridの上限である。

| 期間    | w0.50より改善可能なレース | Top1–3平均oracle差 |
| ------- | ------------------------: | -----------------: |
| 2023    |                   14.713% |           +3.479pt |
| 2024    |                   15.237% |           +3.483pt |
| 2025    |                   14.576% |           +3.339pt |
| 2026 H1 |                   14.106% |           +3.143pt |

weight間の理論余地は存在する一方、発走前tree policyの2024 OOS差は全体-0.017pt、人気通り-0.038ptだった。余地の不足ではなく、改善可能レースの事前識別が課題である。

そこで2023だけで発走前特徴から荒れ確率を学習し、venue×固定確率帯をcellとして2024でweightを選んだ。classifierの2024 AUCは0.679、average precision 0.360（荒れ率0.224）。7 cellが選ばれ、2024はTop1〜Top5全体が全て非負、Top1–3平均は全体+0.051pt、人気通り+0.041pt、荒れ+0.087ptで選択を通過した。

classifierを2023〜2024で再学習し、cell/weightを固定して2025確認したところ、全体Top4 -0.0149pt、人気通りTop3 -0.0288pt・Top4/5各-0.0096pt、荒れTop4 -0.0332ptで棄却した。7 cellを個別帰属しても2024/2025双方の固定mean gateを満たすcellは0だった。本番デプロイなし。

## loop 7: direction classifierと複数range管理cell

結果を知らずに改善可能なweight方向を識別するため、本番artifactのXGB/Transformer score geometryを追加した。46,119レースをarm64 Colimaで再scoreし、base/Transformerのtop margin、score相関、score差分標準偏差、最大rank disagreement、weight grid上のtop horse切替回数、市場shareとの関係など13列を作成した。

arm64再scoreと既存amd64 exactを照合すると、21 weightの勝馬順位はw0.50の同順位境界1レースだけが不一致だった。このため正解・評価順位は既存amd64 exactを固定し、arm64出力からは有限性を確認した診断13列だけを`race_id`で結合した。評価順位を新出力へ置換していない。

1つのvenue親cellが、複数の`予測weight方向 × classifier信頼度の連続範囲`を子cellとして管理する実装を追加した。各子cellは複数の隣接confidence binを1つの学習範囲へpoolでき、独立weightを持つ。重複範囲はweighted interval schedulingで排除する。NAR全体で同じweightを使う候補はゲートで拒否する。

2023学習classifierの2024成績はaccuracy 0.646、balanced accuracy 0.561、macro F1 0.446。2024で9子範囲を選び、6 weightを使用した。

| 期間・選択段階                     | 全体Top1–3平均 |  人気通り |      荒れ | 判定                                             |
| ---------------------------------- | -------------: | --------: | --------: | ------------------------------------------------ |
| 2024 selection                     |      +0.1170pt | +0.0943pt | +0.1955pt | Top1〜5全segment非負                             |
| 2025 fixed confirmation            |      +0.0149pt | -0.0192pt | +0.1327pt | 人気通りTop2〜5負、棄却                          |
| 2025 portfolio事後選択（30/36/44） |      +0.0174pt | +0.0160pt | +0.0221pt | 同期間選択のため独立確認ではない                 |
| 2026 H1 fixed                      |      +0.0062pt |  0.0000pt | +0.0284pt | venue 30が人気通りTop3を1件悪化、棄却            |
| 2026 H1 portfolio事後選択（36/44） |      +0.0185pt | +0.0157pt | +0.0284pt | 同期間選択のため独立確認ではない                 |
| 2026-07-01〜08-23 fixed forward    |      -0.0327pt | -0.0429pt |  0.0000pt | venue 44の11 activationでTop2/3を各1件悪化、棄却 |

forward 2,040レースも診断列を再生成し、既存amd64 exactとの21 weight順位差0件、診断非有限値0件を確認した。天候はlocal PostgreSQLの正式発走時刻とCloudflare差分同期済みlocal DuckDBを結合し、発走前3/6時間だけを使用した。

weight過大の可能性に対し、学習利得が最大値の80%以上ある候補から親w0.50に最も近いweightを選ぶ縮小、次に正の学習利得を持つ最短移動weightを選ぶ縮小を検証した。前者はweightが変わらず、後者はvenue 46をw0.85からw0.55へ縮め、2025人気通りTop1–3悪化を-0.0192ptから-0.0064ptへ減らしたが非負にはできなかった。venue 44はw0.55の学習利得が正でなくw0.60が残り、forward悪化も残った。

観測済み期間を使った子cell削除で見かけ上の正値を作ることはmodel-selection leakageになる。固定forwardでも改善しなかったため、direction managed-cell routerは`deployment_eligible: false`、本番デプロイなしとした。

## loop 8: 親cellによる複数子cellの学習範囲管理（ばんえい）

ばんえいの保存済みwalk-forward CatBoost 300/500-tree modelを再学習せず、2023〜2025の5,232レースをweight 0.00〜1.00（0.05刻み）の21点でexact scoreした。weight 0.00のTop1〜Top5は既報baselineと全桁一致し、score対象と比較基準が同一であることを確認した。

1つの`banei_parent` cellが複数の非重複子cellを管理し、各子cellに次を独立して保持する実装を追加した。

- 発走前に決まる条件範囲（気温帯、季節、降水、grade、race session、model disagreement）
- `training_year_from` / `training_year_to`
- 21点から選んだ固有weight
- 学習レース数
- 年×人気区分×Top1〜Top5の最悪差分

子cell単体は各rankと各segmentを非悪化に保ち、親cellが複数子を組み合わせた時点で人気通り・荒れの双方を正に改善する二段制約にした。2023だけで8 schemaをfitし、2024でschemaを選択した後、選ばれたschemaの各子について`2023`、`2024`、`2023〜2024`から学習範囲とweightを個別に再選択し、2025は確認専用とした。

選択schemaは`発走前3時間平均気温帯 × 季節`で、2024は5子cell・5 weight、482 activationだった。

| 期間            | 全体Top1–3平均 |  人気通り |      荒れ | 判定                     |
| --------------- | -------------: | --------: | --------: | ------------------------ |
| 2024 schema選択 |      +0.1119pt | +0.1374pt | +0.0580pt | Top1〜5全segment非負     |
| 2025 独立確認   |      -0.1409pt | -0.2047pt |  0.0000pt | Top1/2と人気側が負、棄却 |

2025では895 activation、weightは0.00/0.10/0.35/0.55/0.65に分かれた。学習範囲を子ごとに持たせる要件とNAR全体・ばんえい全体でweightを統一しない要件は実装上満たしたが、独立確認精度は満たさない。router artifactは`no_confirmed_candidate`、`deployment_eligible: false`とし、本番へは反映していない。

MLflow remote PostgreSQL backendを直接参照すると、NAR championはregistered model version 3、`iter12-nar-xgb-hpo-v8-clean188`、ばんえいchampionはversion 5、`banei-cb-v9-sim-2011`だった。NARの`latest_cell_eval_run_id=7dcc76e8d9e24ed4879947ea82fa380a`は2026-07-16時点の60日窓、152レースでTop1 28.9474%、place2 44.0789%、place3 57.8947%、place4 64.4737%、place5 69.7368%だった。ばんえいの`latest_cell_eval_run_id=519923af29b14f619cc453569491252a`は同じ60日窓、29レースでTop1 24.1379%、place2 34.4828%、place3 48.2759%、place4 62.0690%、place5 75.8621%だった。両aliasとversion tagは本番`model_meta.json`・`production-artifacts.json`とも一致した。ただしMLflowのplace指標は「予測1位馬の実着順がk以内」、本loopのTopKは「実勝馬の予測順位がk以内」であり、数学的に別指標である。両者の百分率を直接減算した精度差は作っていない。候補は同一定義のOOS baselineとの比較で棄却した。

数理的には、平均との差ではなく候補distributionごとのregret最大値を抑えるMinimax Regret Optimizationの考え方を、年×人気区分×rankのmaximin制約として採用した。[Agarwal & Zhang (2022)](https://proceedings.mlr.press/v178/agarwal22b.html)は、通常のworst-case risk最小化が未知のdistribution shift下で一様に小さいregretを保証しない場合を示し、MROを提案している。ただし今回の2025失敗は、2年以下のcell学習範囲ではshiftを十分代表できないというデータ上の限界を示す。

また、同じ期間でcell、学習範囲、weightを選び、その期間の精度を最終値として扱うことは選択バイアスになる。[Cawley & Talbot (2010)](https://www.jmlr.org/papers/v11/cawley10a.html)の指摘どおり選択処理全体を評価対象にする必要があるため、2025の負値を見て子cellを削る操作は行っていない。次の候補ではouter rolling year内にinner期間選択を置き、複数outer foldで同じ親cell構造が再現するまで昇格させない。

## loop 9: NAR子rangeごとの学習範囲管理

loop 7のvenue親cellと9個の非重複`方向 × confidence range`を固定し、各子rangeが`2023`、`2024`、`2023〜2024`のどの結果期間からweightを学習するかを独立して選ぶ実装を追加した。各候補は年×人気区分×Top1〜Top5の差分が全て非負であることを要求し、親cellが子rangeの条件、学習開始/終了年、weightを1つのartifact構造で管理する。

maximin目的で選ばれた9子rangeの学習範囲は全て2024単年だった。2023または2023〜2024へ広げる候補は、古い分布を足すことで最悪stratum利得が下がり採用されなかった。weightは0.55、0.65、0.75、0.85の4種類で、NAR全体のuniform weightにはしていない。

| 期間                | 全体Top1–3平均 |  人気通り |      荒れ | 判定                 |
| ------------------- | -------------: | --------: | --------: | -------------------- |
| 2024 range/期間選択 |      +0.1194pt | +0.0943pt | +0.2064pt | Top1〜5全segment非負 |
| 2025 独立確認       |      +0.0149pt | -0.0192pt | +0.1327pt | 人気Top3〜5負、棄却  |

学習範囲を子range自身へ移した構造は成立したが、最適化結果が短い最新年へ集中し、独立年へ安定しなかった。router artifactは`no_confirmed_candidate`、`deployment_eligible: false`で、本番変更なし。

短期集中への事前正則化として全子に最低2暦年を要求し、`2023〜2024`だけを許す対照も実行した。残ったのは2子range、weight 0.55/0.80、2024は全体+0.0366pt・人気+0.0314pt・荒れ+0.0543ptだった。2025は全体-0.0050pt・人気-0.0224pt・荒れ+0.0553ptで、期間を長くするだけでも人気側悪化は解消せず棄却した。

## loop 10: ばんえい本番学習レシピ訂正とnested outer評価

loop 8で使用した保存fold modelを再監査すると、評価スクリプトは2011年から学習し、foldごとに`20260824 + fold`のseedを使っていた。一方、MLflow champion version 5と本番`model.json`/`metadata.json`が示す事実は、学習開始2016年、固定seed `20260519`、CPU 4 thread、YetiRank、depth 8、learning rate 0.05、L2 3、300 treesだった。したがってloop 8は130特徴契約こそ同じだが、本番学習レシピのbaselineではなく、精度比較の正本として扱えない。

Colima arm64で2016年開始・固定seedの300/500-tree modelを2020〜2025の6 outer foldについて全て再学習し、10,448レース×21 weightを再scoreした。保存した12 modelはtree数、seed、depth、lossを読み戻して照合した。2023〜2025の5,232レースでは旧baselineと852レースの勝馬順位が変わり、単なる浮動小数点差でないことを確認した。

新baselineを使い、各outer年Yについて「Y-2までで子cellをfit、Y-1でschema選択、Yで完全独立評価」を実施した。子cellの学習範囲は利用可能な連続1〜3年から個別選択し、気温・降水・風速・突風・季節・grade・session・市場強度・model disagreement・score geometryを組み合わせた22 schemaを評価した。

| outer年           | inner選択schema             | 全体Top1–3 |  人気通り |      荒れ | 判定       |
| ----------------- | --------------------------- | ---------: | --------: | --------: | ---------- |
| 2022              | なし                        |   0.0000pt |  0.0000pt |  0.0000pt | inner通過0 |
| 2023              | なし                        |   0.0000pt |  0.0000pt |  0.0000pt | inner通過0 |
| 2024              | 市場強度×model disagreement |  -0.0559pt | -0.1924pt | +0.2319pt | outer棄却  |
| 2025              | なし                        |   0.0000pt |  0.0000pt |  0.0000pt | inner通過0 |
| 2022〜2025 pooled | 選択手続全体                |  -0.0144pt | -0.0487pt | +0.0612pt | 棄却       |

pooledのrace-date cluster 95%区間は全体[-0.0620, +0.0287]pt、人気[-0.1061, +0.0069]pt、荒れ[0.0000, +0.1384]ptだった。routerは`no_confirmed_candidate`、本番変更なし。

## loop 11: ばんえいfull-information contextual policy cell

21 weightの順位は各過去レースで全て計算できるため、通常のbandit feedbackではなく全actionの反実仮想rewardが観測できる。そこで、発走前3/6h天候、市場、grade、model disagreement、score geometryから、20個の非baseline weight×Top1〜Top5のpaired gainをmulti-output Ridgeで予測した。全5 rankの予測gainが固定floor以上の場合だけbaselineから離れ、親cellが`weight × predicted-gain range`の複数子cellを管理する。

alpha、gain floor、weight距離penalty、人気/荒れの逆頻度重みpowerをinner年だけで選び、通過modelをrefitせずouter年へ凍結した。2022〜2024はinner通過config 0。2025だけgroup-balanced configが選ばれ48レースへ作動したが、全体Top1〜Top3平均0.0000pt、人気-0.0292pt、荒れ+0.0646ptで、Top3/Top5にも負値があり棄却した。

逆頻度重みなしの対照は2025で68レース作動し、全体+0.0201pt、人気+0.0292pt、荒れ0.0000ptで全rank非悪化だったが、「人気・荒れ双方を正に改善」を満たさない。結果を見て荒れcellだけ追加する操作は行っていない。

このbaseline fallbackは、データが不足するstate/actionでは既存policyへ戻す[Laroche et al. (2019), Safe Policy Improvement with Baseline Bootstrapping](https://proceedings.mlr.press/v97/laroche19a)と同じ安全設計方向である。一方、今回の固定floorは予測不確実性そのものを推定していない。[Wang et al. (2024)](https://proceedings.mlr.press/v238/wang24a.html)のpessimistic offline policy optimization、[Si et al. (2020)](https://proceedings.mlr.press/v119/si20a.html)のdistributionally robust policy learningに照らすと、次の候補は年cluster間の予測分散からlower confidenceを作り、平均予測ではなくその下限が正の場合だけroutingする必要がある。

## loop 12: NAR full-information contextual policy cell

NARでも本番weight 0.50をbaseline、残る20 weightをactionとし、発走前3/6h天候、市場、XGB/Transformer score geometryから各action×Top1〜Top5のpaired gainをmulti-output Ridgeで予測した。親cellは`action weight × predicted-gain range`の複数子cellを管理し、子ごとにweightと予測gain範囲を保持する。

時系列は2023 fit→2024 inner選択→2025 outer確認、2023〜2024 fit→2025 inner選択→2026 H1 outer確認とし、後者と同一凍結modelを2026-07-01〜08-23 forwardへ適用した。いずれもinner gateを通過するconfigは0で、outer/forwardはbaseline fallbackのactivation 0だった。

innerで最良に近いが棄却された設定は、2024選択で全体+0.0146pt・人気通り-0.0723pt・荒れ+0.3150pt（528 activation）、2025選択で全体+0.0199pt・人気通り-0.0416pt・荒れ+0.2322pt（301 activation）だった。荒れ側の大きな正値だけを採用せず、人気側の負値により棄却した。

## loop 13: NAR group-robust policy cell

平均reward headが多数派の人気通りレースへ偏る可能性を切り分けるため、人気通り／荒れを別々のRidge headで学習し、両headの予測Top1〜Top5利得の最小値が正の場合だけactionを許すgroup-robust版も追加した。2023→2024、2023〜2024→2025のどちらもinner通過configは0で、outer/forward activationは0だった。

これは「正の結果だけ」を得るために観測後の負cellを消す実装ではなく、発走前に計算できる両groupのpessimistic gainを事前制約にした検証である。独立期間で正の候補を作れなかったため、本番modelは変更していない。

## loop 14: 発走前6時間のweather trajectory cell（ばんえい）

直前3時間平均だけでは、同じ平均値でも「雨が降り始めた」「止んだ」「降り続いた」を区別できない。そこで発走前6時間を前半3時間と後半3時間へ分け、結果列を使わず次のphase cellを追加した。

- `cell_temp_phase`: 前半3時間平均から後半3時間平均への差が`cooling` / `stable` / `warming`
- `cell_rain_phase`: `dry` / `onset` / `stopped` / `continuing`
- `cell_weather_phase`: temperature phaseとrain phaseの直積

新しい8 schemaを既存22 schemaへ追加し、本番学習レシピの2020〜2025 exact scoreに対して同じnested outer手続を再実行した。trajectory schemaは全outer foldでinnerのTop1〜Top5×全体/人気/荒れgateを通過しなかった。例として2024 innerの`weather phase × disagreement`は全体+0.0373pt、人気-0.0536pt、荒れ+0.2451pt、2025 innerの`temp phase × session`は全体+0.0373pt、人気+0.0275pt、荒れ+0.0580ptだったがrank別負値が残った。

選択された既存schemaとouter結果はloop 10から変わらず、pooledは全体-0.0144pt、人気-0.0487pt、荒れ+0.0612pt。本番変更なし。

## loop 15: NAR trajectory子cellの厳密0–1 portfolio最適化

NARでは各venue親cellがtrajectory条件、model disagreement、市場強度を持つ複数子cellを管理し、各子cellは独立した学習年範囲とweightを持つようにした。inner年で全子を常時使うのではなく、子cell採否をbinary変数とするmixed-integer linear programを解いた。

- 制約: 全体/人気通り/荒れ × Top1〜Top5の15 paired count差を全て0以上
- 制約: 3 segmentそれぞれのTop1〜Top3合計差を1 count以上
- 目的: 3 segmentのTop1〜Top3平均差の最小値を最大化
- tie-break: 同じworst gainなら子cell数を減らす
- 選択に使用するのはinner年だけで、選ばれた子集合・学習期間・weightをouter年へ凍結

SciPy `milp`/HiGHSによる0–1整数解を使用し、連続緩和後の丸めではない。公式仕様上、`milp`はbinary boundsとintegralityを持つ線形制約問題を解き、成功時は最適解を返す。[SciPy milp documentation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.milp.html)

| 選択→独立評価             | schema                        | 子cell / weight | 全体Top1–3 |  人気通り |      荒れ |
| ------------------------- | ----------------------------- | --------------: | ---------: | --------: | --------: |
| 2024 inner                | venue×temp phase×disagreement |      10 / 6種類 |  +0.0585pt | +0.0566pt | +0.0652pt |
| 2025 outer                | 同上を凍結                    |      10 / 6種類 |  -0.0025pt | -0.0320pt | +0.0995pt |
| 2025 inner                | venue×weather phase×market    |     21 / 12種類 |  +0.0720pt | +0.0704pt | +0.0774pt |
| 2026 H1 outer             | 同上を凍結                    |     21 / 12種類 |  -0.0185pt | -0.0236pt |  0.0000pt |
| 2026-07-01〜08-23 forward | 同上を凍結                    |  517 activation |  -0.5392pt | -0.7288pt | +0.0687pt |

forwardの日付cluster 95%区間は全体[-1.1417, -0.0157]pt、人気[-1.5432, -0.0226]pt、荒れ[-0.3381, +0.4752]pt。innerでは数学的に制約を満たす最適組合せが存在しても、翌年とforwardへ一般化しなかった。これはsolver不足ではなくdistribution shift下の選択過適合である。

[Wang et al. (2025)](https://proceedings.mlr.press/v267/wang25cm.html)は、将来変化を共変量分布だけでなく条件付きoutcomeのconcept driftとして扱うdistributionally robust policy learningを定式化している。今回のtrajectory cellはcovariateを細分化したが、cell内のweight gain自体の年変化をモデル化していないため、次は年をuncertainty distributionとして直接持つ必要がある。また、[Almeida et al. (2025)](https://proceedings.mlr.press/v266/almeida25a.html)のhigh-probability Learn-Then-Test under covariate shiftの方向に沿い、cell選択とrisk controlを同じinner sampleの経験平均だけで完結させない設計が必要である。

## 天候利用方法の再レビュー

NAR OOF 40,710レースを再監査し、`prior3_temperature_mean/max/trend`、`prior3_precipitation_sum`、`prior3_wind_mean`、`prior3_gust_max`の欠損はすべて0件だった。研究routerのSQLは発走時刻より前の3時間だけを使用しており、発走hourと発走後の観測を含まない。この点はleakage-safeである。

一方、共通feature builderでは厳密な発走前windowとして出力されていたのは`venue_temperature_prior3`だけで、降水・風速・突風は09:00〜17:00の全日集約だけだった。本番では事前forecastとして利用できるが、学習時は実測値なので早いレースに発走後観測が混ざり、将来weather modelを学習するとtrain/serve semantic mismatchになる。

共通builderへ以下を追加した。既存production modelのfeature listは変更していないため、現在のscoreには影響しない。

- `venue_precipitation_prior3`
- `venue_wind_speed_prior3_max`
- `venue_wind_gusts_prior3_max`

これらは既存の`venue_temperature_prior3`と同じく、発走hourより前の完全な3 hourly observationsだけで計算する。今後のNAR/JRA weather候補では全日実測列を学習特徴として使用せず、このcausal windowを契約にする。

論文・数理面ではtemperature単独thresholdより、humidityを含むWBGT、距離・運動強度とのinteraction、venue/month anomalyが妥当である。しかし現在のOpen-Meteo archiveにはhumidityが無く、WBGTを正しく構成できない。temperature-only gateをWBGTと呼ばず、humidity取得・backfill完了前はproxyとして明示する。

Open-Meteoのforecast APIとhistorical archive APIを実リクエストして、`relative_humidity_2m`、`dew_point_2m`、`wet_bulb_temperature_2m`、`shortwave_radiation`がいずれも24 hourly valuesで取得可能なことを確認した。したがって取得自体は実現可能。ただし現行R2 payload/catalog stream/local DuckDBはschema v1で4列を保持しないため、単にAPI queryへ追加するだけでは不十分である。安全な導入順は、v2 schema/prefix、dual-read、local yearly backfill、prior3 WBGT集約、walk-forward確認、最後にv1停止とする。既存v1 tableへ意味の違うnullable列を混在させて「欠損=低湿度」と扱うことは禁止する。

HTTP responseの数値`weather_type`が実際にはOpen-Meteo weather codeであり、forecast/actual種別のように読める命名不整合も発見した。互換性のため旧fieldを残したまま、同じ値を明示的な`weather_code`として追加し、Worker version `cbf2b874-6d33-45d8-9fe8-b22bb261fe03`へデプロイした。2026-08-25のKV response 600行で新旧field一致を確認済み。

Cloudflare公式仕様も再確認した。structured stream schemaは作成後に変更できず、Pipeline SQLも変更にはdelete/recreateが必要である。このためv1を変更せず、以下の独立v2資源を作成した。

- stream `venue_weather_ingest_stream_v2`: `e031c664c15249f7ba1d8d3bf0376290`
- sink `venue_weather_hourly_v2_sink`: `79c55459e1af4e36a2c28ee831d1537a`
- pipeline `venue_weather_hourly_ingest_pipeline_v2`: `f42853b3fd5f443990cfad6eb420481a`
- table: `weather.venue_weather_hourly_v2`
- runtime objects: `venue-weather-live/v2` / `venue-weather-snapshots/v2`

V2はrelative humidity、dew point、wet-bulb temperature、shortwave radiationの4列が全て揃った24時間だけ書き込み、不完全ならv2を0行として捨てる。V2失敗はv1 upsert成功後にcatchされるため、現行weather servingを停止させない。Worker version `e730c86e-3b78-4dd1-b804-e800eed21f24`ではdual-writeに加えてcomplete-key dual-readも有効化した。V1とv2のvenue-hour集合が完全一致するときだけ4列をHTTP responseへ追加し、部分的なv2はv1 responseへ完全fallbackする。Containerも600行全てに4列がある場合だけ別の`venue_weather_v2_YYYY.duckdb`を作り、不完全/書込失敗時はv1 sidecarを維持する。

### 完了日降水とv2 cell再評価

指摘どおり、降水は発走直前値だけでなく、レース日前日までに完了した履歴が安全に使える。共通builderへD-1、D-3、D-7の降水量を追加した。windowは必ずD-1で終了し、年初レース用に前年weather fileも読む。January 1でDecember 31だけがD-1/D-3/D-7へ入り、当日100mmが混入しないcross-year testを追加した。

2023-2025の25会場v2 hourly backfillを作成し、NAR 40,710レースでprior3 humidity/wet-bulbとD-1降水の欠損0を確認した。2023選択→2024独立screenでは次の2 cellが通過した。

| cell                | selected weight | 2024 Top1 |     Top2 |     Top3 |     Top4 |    Top5 | objective |
| ------------------- | --------------: | --------: | -------: | -------: | -------: | ------: | --------: |
| prior-day-rain      |            0.95 |  +0.412pp | +0.387pp | +0.090pp | +0.168pp | 0.000pp |  +0.276pp |
| dirt+prior-day-rain |            0.95 |  +0.413pp | +0.387pp | +0.090pp | +0.181pp | 0.000pp |  +0.277pp |

ただし、既に他探索で参照済みの2025ではTop1がそれぞれ-0.102ppとなった。2025からのpromotionは禁止し、本番weightは0.50のまま変更しない。候補は2026-08-25以降の未観測forward期間でTop1-5非負、日付cluster LB、人気通り/荒れ別gateを満たすまでshadow扱いとする。

Builder/Containerのcausal weather contractはfinish-position image `49ec97ec` / Worker `49ec97ec-e9f6-4ef2-977e-f5e07c7a6b5a`へ反映済み。現行selected modelは追加列を使用しないため、予測weightは変化しない。

V2 backfill経路は2025-01-01だけで限定検証した。事前validation 600行、2 batch送信後、R2 SQLで600行・25会場・各hour 0–23・600 unique venue-hour keys・humidity/dew-point/wet-bulb/radiation全600行非NULLを確認した。Checkpointは成功batch後だけ更新され、v1 table/objectは変更していない。これはデータ経路の閉鎖確認であり、精度改善がない限り全履歴backfillやmodel採用を正当化しない。`prior-day-rain`候補は引き続きshadow-onlyで、参照済み2025からのpromotionおよび本番weight 0.50の変更は行わない。

humidity/dew point/wet-bulb/radiationのv2は、v1を配信元のまま維持し、独立R2 prefix・Pipelineへdual-writeする構造でWorker version `4a4e16a3-a1d7-4f1b-902c-05315472bb77`に配備済みであることをWranglerの本番version metadataで確認した。本番binding、作成時刻、初回監査は次のとおり。

- `WEATHER_CATALOG_STREAM_V2`: stream `e031c664c15249f7ba1d8d3bf0376290`
- Pipeline `venue_weather_hourly_ingest_pipeline_v2`: `running`、2026-08-25 01:58:37 JST作成
- Worker version 13: 2026-08-25 02:01:26 JST作成、100%配信
- 同日の予報cron: 01:30 JST。version配備はcronの31分後だった
- 02:03 JST時点のv2 R2 forecast key: 未作成。R2 SQLのv2当日行: 0

したがって02時台のv2欠測は、まだ「配備後にjobが成功したのに書けなかった」という失敗事実ではない。次の配備後scheduled eventは20:00 JSTのactual取得であり、その完了後に25会場×24時間=600行、v2 4列の非null、R2 live object、Pipeline catalog行を同時に検査する。v1は同日600行・25会場・全数非nullで、local DuckDBとの差分0を確認済みである。v2をread pathや学習へ昇格するのはこの完全性確認とhistorical backfill後に限定する。

local差分同期にはv2経路が存在せず、feature builderが期待する`venue_weather_v2_YYYY.duckdb`を作れない欠落があった。既存の`sync:weather-local`をv1→v2の順に同期するよう拡張し、v2は独立table/fileへ保存、humidity/dew point/wet-bulb/radiationを必須数値として検証し、欠損を0へ変換しない契約にした。

2026-08-25をCloudflare R2 SQLから実同期した結果は、v1が600 remote rows・600 unchanged・insert/update 0、v2が0 remote rowsだった。localにはschemaだけを持つ0行の`venue_weather_v2_2026.duckdb`を作成した。これにより「v2が未取得」と「湿度0」を区別したまま、次回以降は同じ主キーでinsert/update/unchangedを差分計数できる。

## loop 16: 実配当を用いたNAR高配当cell（+1pt gate）

`apd_se_nv`は2025年1,160レース、2026年1,362レースでscoreとのjoinが欠けていたため、評価用配当を補完推測せず、raw NAR `nvd_hr.haraimodoshi_sanrentan_*b`からrace単位で直接取得した。2025、2026 H1、2026-07-01〜08-23 forwardでは三連単配当の欠損0。2023〜2024の欠損6件は出走2〜3頭で三連単自体が発売不能だったため、0円ではなく`unavailable`として除外した。高配当は各fit期間の三連単90 percentile以上と定義した。

具体的な最高配当例は次のとおり。これは評価結果であり、routing条件には使用しない。

| race                          |       三連単 | 本番weight 0.50での勝馬順位 |
| ----------------------------- | -----------: | --------------------------: |
| 2025-03-11 venue 36 R4        |  8,171,760円 |                        11位 |
| 2025-01-03 川崎 R1 おみくじ賞 |  7,913,850円 |                        13位 |
| 2026-05-23 venue 54 R9        |  5,866,480円 |                         9位 |
| 2026-03-04 川崎 R7 鳴門賞     |  5,193,650円 |                        11位 |
| 2026-07-23 venue 50 R9        | 12,394,160円 |                         8位 |
| 2026-08-12 大井 R8            |  6,767,160円 |                        11位 |

重み範囲を0.00〜1.00から0.00〜2.00（0.05刻み）へ拡張し、旧範囲46,119レースとforward 2,040レースで既存rankとの不一致0を確認した。高配当結果を見た年別oracleでは、Top1〜3平均が2024年weight 1.90で+2.393pt、2025年weight 2.00で+2.876pt、2026 H1 weight 1.95で+1.356ptだった。一方、同じweightの常時適用は通常群・全体を悪化させるため、これは実装可能な改善値ではなく、routerが識別できた場合の上限確認に限る。

採用条件は、高配当群のTop1〜3平均を各calendar environmentで+1.0pt以上、全体・人気通り・荒れ・通常配当群のTop1〜5を各rankで非悪化、全体Top1〜3平均を正とした。MILPの+1ptは表示値の後判定ではなく、高配当レース数を`N`として必要paired hit差を`ceil(0.01 × 3 × N)`以上とする整数制約にした。

推論時cellには実配当を使わず、発走前市場、XGB/Transformer disagreement、発走前3/6時間天候、venueだけを使用した。次の3段階をnested outerで検証した。

1. shallow high-payout risk classifierと3段階risk cell
2. venueをcategoryとして扱う5分位・10分位risk cell
3. weight 2.00のTop1〜3 paired utilityをfit年だけで学習するhigh-payout uplift cell

合計27 schema、各子cell独自の学習年範囲とweight 0.00〜2.00を探索したが、2025 outer用selection、2026 H1 outer用selectionとも+1ptと非悪化制約を満たすportfolioは0だった。fail-closedによりouter 18,835レースとforward 2,040レースはactivation 0、全区分の精度変化0.000pt。risk classifier自体のROC-AUC/APは2025が0.6869/0.2027、2026 H1が0.6939/0.2104、forwardが0.7286/0.2462だったが、配当riskの識別力はweight効果の年越し再現性を保証しなかった。

`nar_high_payout_nested_outer_cell_report.json`の`confirmed`と`deployment_eligible`はいずれもfalse。本番model、MLflow registry、production artifactは変更していない。cell追加だけを続けると既参照年へのmodel-selection overfittingを増やすため、このrouter系列は棄却する。次段階は高配当riskをsample weightまたはmulti-task objectiveとして学習自体へ入れ、同じnested outer gateで比較する。

## loop 17: 配当sample weightと市場cell

clean188 XGBoostの`rank:pairwise`は固定し、fit期間だけで算出した三連単90 percentile以上のrace-query weightを1.25、1.50、2.00、3.00倍にした。欠損配当は補完せず1.00倍のまま、2024年13,677レースをselection foldとした。高配当群は1,337レースだった。

| multiplier | 全体Top1–3平均 | 人気通り |     荒れ |   高配当 | 通常配当 | gate |
| ---------: | -------------: | -------: | -------: | -------: | -------: | :--- |
|       1.25 |       +0.017pt | -0.057pt | +0.272pt | +0.125pt | +0.005pt | fail |
|       1.50 |       +0.002pt | -0.066pt | +0.239pt | +0.299pt | -0.030pt | fail |
|       2.00 |       +0.054pt | +0.195pt | -0.434pt | +0.299pt | +0.027pt | fail |
|       3.00 |       -0.078pt | -0.380pt | +0.967pt | +0.249pt | -0.113pt | fail |

高配当Top1–3平均+1.0ptと、全体・人気通り・荒れ・通常配当のTop1〜5各rank非悪化を同時に満たす候補は0だった。選択候補0の時点で2025モデル学習を停止し、2025年13,426レースは現行baseline fallbackとして全区分0.000ptを記録した。

レース単位のpaired Top1〜3 utility差から算出した95%区間も併記した。高配当群で最大だった1.50/2.00倍の+0.299ptはどちらも95%区間[-0.121, +0.719]ptで、+1ptには点推定でも区間上限でも届かない。3.00倍の荒れ群+0.967ptは[+0.653, +1.280]ptだった一方、人気通りは-0.380pt[-0.589, -0.171]ptで明確な悪化だった。

3.00倍で荒れと人気通りの効果方向が逆だったため、発走前に観測できる単勝市場だけで親cellを追加した。2024年を7月2日まで6,553レースのselectionと、その後7,124レースのconfirmationに時系列分割し、各3 cellへbaseline/1.25/1.50/2.00/3.00倍modelを独立割当した。cell境界はselectionだけのtercileで凍結した。

- favorite market share: 0.3574 / 0.4846
- normalized market entropy: 0.6583 / 0.7523
- field size: 9 / 11頭
- favorite odds: 1.6 / 2.2倍

各schema 125通り、合計500 mappingのうちselection gate通過は0、confirmation通過も0だった。結果や配当はrouting入力に使っていない。cellを増やしても1pt条件を満たさなかったため、2025 outer学習と本番デプロイは行っていない。

## loop 18: ばんえい高配当cell・拡張weight・uplift policy

ばんえいのproduction-recipe OOS 2020〜2025は10,448レースで、local PostgreSQL raw `nvd_hr`の三連単配当も10,448件すべて取得できた。会場コード83、発走前3/6時間weather joinも10,448件で欠損0だった。

まず親cellが各子cellの連続1〜3年学習範囲とweightを独立管理する16 schemaを追加した。routing入力はmarket share、market entropy、頭数、300木/500木model disagreement、発走前temperature/rain/wind/gustとその3/6時間phase、季節、race sessionだけである。2023/2024/2025 outerの直前年selectionで、高配当Top1〜3平均の最大は+0.379/+0.208/+0.330ptで、+1pt通過は0だった。

NARと同じくweightを0.00〜1.00から0.00〜2.00へ拡張した。元の300木/500木CatBoost OOS scoreから10,448レース×41 weightをColima ARM64で再生成した。この際、`int(weight * 100)`が1.15を114へ切り捨てる列名bugを検出し、`round(weight * 100)`へ修正、`w115` contract testを追加した。拡張後のselection最大は+0.379/+0.208/+0.495ptで、なお+1pt未達だった。

年ごとの同一weightと、結果を見てraceごとに最良weightを選ぶ禁止oracleを比較した。

| year |  fit q90 | best global weight | 高配当Top1–3 | 全体Top1–3 | outcome oracle高配当 |
| ---: | -------: | -----------------: | -----------: | ---------: | -------------------: |
| 2022 | 75,550円 |               1.95 |     +1.894pt |   -0.653pt |             +2.652pt |
| 2023 | 75,845円 |               1.75 |     +0.625pt |   -0.261pt |             +1.875pt |
| 2024 | 74,018円 |               1.50 |     +1.485pt |   -0.186pt |             +2.145pt |
| 2025 | 75,790円 |               0.40 |     +0.379pt |   +0.302pt |             +1.136pt |

安全gateを通るglobal weightは全年度0だった。一方で禁止oracleは全年度+1ptを超えるため、blend面に改善余地はあるが静的weightでは取り出せないことが分かった。

既存contextual Ridgeは全raceのTop1〜5だけを目的としており、高配当目標と不整合だった。このため、全raceでarm別Top1〜5安全性を学ぶRidgeと、fit期間q90以上だけでarm別Top1〜3 upliftを学ぶRidgeを分離した。安全予測が5 rankすべて閾値以上のarmだけから高配当upliftを最大化し、alpha・安全floor・uplift floor・weight距離penaltyをY-1で選択、Yで一度だけ評価した。

この二目的policyのselection高配当最大は、2022が+0.568pt、2023/2024が0.000ptだった。2022の最大候補は全体-0.058pt、人気通り-0.168pt、通常配当-0.128ptで安全gate不通過。全outerはactivation 0、差分0.000ptで、本番model/MLflow registry/artifactを変更していない。

41 actionの選択分散を抑えるため、fit期間の各weight×Top1〜5 paired effect profileを用いて隣接weightだけを凝集し、5/8/12/20 groupへ縮約した。各groupはeffect profile平均との差が最小の実weightをmedoidとし、群形成、medoid、Ridge学習はY-2以前だけ、群数と安全/uplift thresholdはY-1だけで選択した。

fusion policyのselection高配当最大は2022/2023/2024が+0.568/+0.000/+0.165ptで、gate通過は0。2024 selectionの最大候補は全体+0.093pt、人気通り+0.055pt、荒れ+0.174pt、通常+0.084ptだったが、全体Top4 -0.168pt、Top5 -0.224ptで棄却した。縮約後も全outerはbaseline fallback 0.000ptである。

### ばんえいCatBoost配当group-weightとmodel-arm cell

後段blendだけでなくproduction-recipe CatBoost 300木の学習目的へfit-only配当weightを入れた。CatBoost `Pool.group_weight`の公式契約に合わせ、同一raceの全runnerへ同一倍率を設定した。2016〜2025 runner storeは17,052レースで、三連単配当欠損は2019-07-29帯広R10の1レースだけだった。raw払戻は登録6頭・出走3頭で三連単欄が空のため、推測せず`unavailable`、倍率1.0とした。2024/2025評価集合の欠損は0。

2024 selection 1,788レース、fit q90以上231レースの結果は次のとおり。

| group multiplier | 全体Top1–3 | 人気通り |     荒れ |   高配当 | 通常配当 | gate |
| ---------------: | ---------: | -------: | -------: | -------: | -------: | :--- |
|             1.25 |   +0.019pt | +0.137pt | -0.232pt | +0.144pt |  0.000pt | fail |
|             1.50 |   -0.168pt | -0.330pt | +0.174pt | -0.144pt | -0.171pt | fail |
|             2.00 |   +0.056pt | -0.275pt | +0.754pt | +0.577pt | -0.021pt | fail |
|             3.00 |   -0.037pt | -0.495pt | +0.928pt | +0.289pt | -0.086pt | fail |

選択候補0のため2025 weighted modelは学習していない。ただしcell間の効果方向が異なるので、学習済み4 modelとbaselineのrace別順位を保存し、2024年7月13日まで900レースでcell mappingを選び、その後888レースを確認期間にした。

市場、entropy、頭数、temperature/rain/wind/gust、季節、session、model disagreementおよび最大6子cellの複合13 schema、合計48,325 mappingを全列挙したが、selection gate通過は0。単純cellの高配当最大はsessionの+0.971ptだったが人気通り-0.212pt、field size/wind/gustの+0.647ptも他区分を悪化させた。market×rainは全体+0.407pt、人気通り+0.477pt、荒れ+0.246pt、通常+0.376ptでも高配当+0.647ptに留まりrank別制約も満たさなかった。

さらに子cell×model armを0–1整数変数とし、高配当+1ptを`ceil(0.01 × 3 × N_high)`のpaired hit、全体/人気通り/荒れ/通常のTop1〜5を各0以上、各子cellちょうど1 armとしてMILPへ直接符号化した。race番号、session×市場/entropy/rain、市場×temperature/rain phase/weather phase、entropy×weather、頭数×weatherの10 schemaはすべてinfeasibleだった。したがってconfirmationへ渡るmappingは0、2025学習と本番変更はない。

高配当riskを補助taskにしたexpert routingも追加した。2020〜2023の市場、model geometry、発走前3/6時間temperature/rain/wind/gustだけでshallow HistGradientBoosting classifierを学習し、fit予測の50/80 percentileをrisk3、20/40/60/80 percentileをrisk5境界として固定した。2024のROC-AUCは0.6307、Average Precisionは0.1668。risk3/risk5単独とsession、市場、entropy、rain、temperature、disagreementとの11複合schemaを同じMILP制約で解いたが全てinfeasibleだった。2024配当はAUC/AP診断と評価だけに使い、routing cell生成には使っていない。

方針はpolicy learningを「covariateからactionへの写像としてwelfareを最大化する」問題として扱う一方、今回の全41 arm順位は同じraceで完全観測されるためbandit propensity補正を入れていない。policy classを拡大すると有限標本regretとselection overfitが増えるため、正則化linear policyとnested年分割に限定した。[Zhu et al. (2025)](https://proceedings.mlr.press/v267/zhu25e.html)のmany-treatment fusionは、次に41個の近接weightを効果同等groupへ縮約する根拠になる。[Swaminathan & Joachims (2015)](https://arxiv.org/abs/1502.02362)と[Jagerman et al. (2020)](https://arxiv.org/abs/2002.00467)が強調するvariance-aware/safe baseline比較は、今回のfail-closed outer gateと整合する。ただし両者はlogged bandit設定であり、全arm反実仮想が計算できる今回へpropensity部分をそのまま適用しない。

### ばんえい連続配当group-weight expertと41-weight routing

二値q90倍率では高配当効果が弱かったため、fit期間のrace別三連単配当に`log1p`を適用し、fit中央値からp99までを0〜1へclipした。race group weightは`1 + strength × tail`とし、strength 0.25/0.50/1.00/2.00をColima ARM64上のproduction-recipe CatBoost 300木へ適用した。

2024年のstrength 0.50は高配当Top1〜3平均+1.010ptに達したが、全体+0.112pt、人気通り-0.027pt、荒れ+0.406pt、通常配当-0.021ptで安全gateを通らなかった。production baselineとstrength 0.50 expertのrace内z-scoreをweight 0.00〜2.00、0.05刻みで融合すると、weight 0.95は高配当+1.010pt、全体+0.186pt、人気通り+0.027pt、荒れ+0.522pt、通常配当+0.064ptまで改善した。ただし人気通りTop3 -0.330pt、通常配当Top2 -0.064pt、Top4 -0.642ptなどrank別悪化があり棄却した。

2024年を前半900レース、後半888レースへ分け、race番号、市場、entropy、頭数、session、発走前3/6時間temperature/rain/wind/gust、model disagreement、3軸交差、selection-only tercileのmodel effect cellまで36 schemaをMILPで検証した。しかし前半103件の高配当raceについて、結果を参照して41 weightからraceごとに最良rankを選ぶ禁止oracleでもTop1〜3 paired hit差は3件だけだった。改善率は`100 × 3 / (3 × 103) = 0.971pt`で、+1pt整数制約に必要な`ceil(0.01 × 3 × 103) = 4`件へ届かない。したがって全schema infeasibleはcell不足ではなく、この時系列分割における数学的到達不能である。

恣意的にgateを下げず、2024年全1,788レースをselection、2025年1,656レースをouter confirmationとした。2016〜2025 runner 153,700行・17,052レースを再利用し、2025 foldのstrength 0.50 expertを2024年までで再学習した。raw配当欠損は既知の2019年1レースだけ、2025年は0だった。2025のglobal 41 weightはすべて不合格で、高配当Top1〜3の最大は0.000ptだった。一方、禁止oracleは2024が高配当+2.020pt・全体+2.647pt、2025が高配当+2.281pt・全体+2.617ptであり、改善余地は存在するが事前識別できていない。

「1つのcellで複数cellの学習範囲とweightを管理する」実装として、2024だけで高配当Top1〜3改善可能性を学ぶ浅いdecision treeを作り、各leafをcell、各leafの41 weightを同じMILPで割り当てた。28設定中14設定が2024 gateを通り、高配当は+1.154〜+1.876ptだったが、2025では全候補が-0.351〜-1.053ptとなり再現しなかった。固定cell 36 schemaでは9 schemaが2024 gateを通ったが、2025の高配当最大はsession×market×weather phaseの+0.175ptで、その全体は-0.262ptだった。tree cellと固定cellの双方を棄却し、本番model、MLflow registry、production artifactは変更していない。本番精度差は0.000ptである。

### 2026ばんえいデータ欠落監査とdistributionally robust outer

研究feature storeが2025-12-30で終了していたため、local PostgreSQL rawを再監査した。2026-01-02〜08-24には`nvd_ra` 1,140レース、`nvd_se` 10,679 runner行、`nvd_hr` 1,111レースが存在した。払戻がない29レースは2026-01-03の12レース、07-12 R4〜R8の5レース、08-08の12レースで、全runnerの`kakutei_chakujun='00'`、勝馬なしだった。これらは通常配当0円へ補完せず、outcome unavailableとして精度評価から除外した。勝馬と払戻の両方が観測された外部確認集合は1,111レースである。

venue-weather v1は帯広83について2026-01-01〜08-25の237日×24時間=5,688行、気温欠損0だった。現行8段pipelineをColimaで再実行し、base 211列、lineage 218、H2H 224、baba 232、futan 242、grade 260、similar race 279、sire venue bias 284列、最終10,648 runner行・1,140レースを生成した。本番metadataの130特徴は列欠落0、発走前3時間のtemperature/precipitation/wind/gustも欠損0だった。v2はlocal tableが0行のためhumidity/dew point/wet-bulb/radiationを全nullのまま未使用とし、0補完していない。

Colima研究imageのDuckDB 1.4.4では、既存index付きtemp tableをscratch tableへrenameした後の再indexで`Cannot create non-temporary entry ... in temporary catalog`が再現した。scratchをrenameせずfinal temp tableとして明示的に再作成するようbuilderを修正し、DuckDB 1.4.4とlocal 1.5.5の双方で回帰testを通した。production containerはDuckDB 1.5.5固定で旧処理でもこの障害は再現しないため、現時点の本番欠落事故とは判定していない。

2025まででbaseline 300木とcontinuous-payout strength 0.50 expertを学習し、2026の41 blend weightを作成した。global weightの高配当Top1〜3最大はweight 0.40/0.60などの+0.273ptで、weight 0.40の全体は-0.030ptだった。禁止oracleの上限も高配当122レースで+1.093pt、すなわち366個のTop1〜3 paired thresholdに対する改善4 hitだけであり、+1ptには4 hitすべての事前識別が必要だった。

2026をhyperparameter選択へ使わないため、固定36 schemaとtree 28設定について、2024と2025を別environmentとして各年ごとに高配当+1pt、全体/人気通り/荒れ/通常配当Top1〜5非悪化をMILP制約にした。18候補が両年gateを通り、worst-year高配当、次にworst-year全体の順でtree depth 8・min leaf 10・38 leafを1候補だけ選択した。この候補は2024/2025で高配当+1.876/+2.105pt、全体+0.671/+0.543ptだったが、凍結後の2026は高配当-0.546pt、全体-0.240pt、人気通り-0.397pt、荒れ+0.094pt、通常配当-0.202ptで反転した。外部確認gate不通過のため棄却し、デプロイしていない。

### 多重検定補正済みhigh-confidence昇格gate

平均+1ptだけを通さず、悪影響が疑われる範囲ではbaselineへroutingするための共通昇格gateを実装した。raceごとのpaired差を使い、同一開催日の相関をrace-day clusterとしてsandwich標準誤差を計算する。高配当Top1〜3平均の片側下限を+1pt以上、全体Top1〜3平均の片側下限を0以上、さらに全体・人気通り・荒れ・高配当・通常配当それぞれのTop1〜Top5の片側下限を0以上とする計27条件である。27条件を同時に探索するため、各条件の有意水準を`0.05 / 27`とするBonferroni補正でfamily-wise error rateを5%以下にした。

NARのnested high-payout routingを再実行すると、2025 outer 13,426レース、2026 outer 5,409レースの双方でselection安全条件を満たすcellは0だった。このため全18,835レースが本番weight 0.50へfallbackし、高配当1,982レースを含む全指標の差は0.000ptだった。27条件中、非悪化26条件は差が恒等的に0なので通ったが、高配当Top1〜3は推定0.000pt・下限0.000ptで必要な+1ptに届かず棄却した。

ばんえいの凍結38-leafルータも同じgateで独立2026年1,111レースを診断した。高配当122レースのTop1〜3は推定-0.546pt、Bonferroni補正済み片側下限-1.689ptであり、必要な+1ptを大きく下回った。全体Top1〜3も推定-0.240pt・下限-0.759ptで、27条件中25条件が不合格だった。したがってcell数やcell別weightを増やしたこと自体は時系列分布変化への安全性を保証せず、未知期間で下限を確認できないcellはbaselineへfail closedする。

### NAR weather-v2階層range routing

NARで実績のあった発走前天候を、前日降雨量を親cell、発走前3時間の相対湿度・湿球温度・日射、季節、馬場種別、競馬場を子cellとする18 schemaへ拡張した。各子cellは独立したweightを持ち、100レース未満のrange、未知range、または安全条件を満たさないrangeは本番weight 0.50へfallbackする。2023年だけで子cell weightを生成し、2024年で子cellごとの全体/人気通り/荒れTop1〜Top5の効果方向を校正し、2025年には選択した1 schemaだけを適用した。

最初のaggregate校正では「競馬場×前日降雨×湿球温度」の18子cell、13種類weightが2024 gateを通ったが、2025年は全体Top1〜3 -0.030pt、人気通り-0.048pt、荒れ+0.033ptへ反転した。そこで2024年に子cell単位で方向が一致しないweightを0.50へ戻す二段階routingを追加した。2023年の15候補rangeから3 rangeだけが残り、選択schemaは「前日降雨×相対湿度×季節」、weightは`light rain/humidity 70–80%/spring=0.30`、`light rain/humidity 80–90%/autumn=0.55`、`moderate rain/humidity <70%/autumn=0.55`となった。

凍結後の2025年13,426レースでは572レースをactivateし、全体Top1〜3 +0.005pt、人気通り+0.006pt、荒れ0.000ptだった。Top1〜Top5の点推定は全区分で非負になり、最初のroutingで生じた悪影響は除去できた。ただし荒れTop1〜3が正ではなく、16条件の同時信頼下限も不通過である。2023年の三連単払戻q90=82,522円を固定した高配当1,427レースではTop1〜Top3が0.000pt、Top5だけ+0.070ptであり、要求する+1ptを満たさなかった。このためデプロイしていない。

払戻joinで2023年3レース、2024年3レースが三連単利用不能だったためrawを監査した。6レースとも`nvd_hr`行、確定着順、勝馬は存在し、出走5〜6頭で単勝・馬連・馬単払戻は記録されていたが、三連複・三連単払戻欄自体がnullだった。したがってraw同期欠落ではなく、該当賭式が発売・成立していないraceとして高配当評価から除外し、0円へ補完していない。weather-v2結合は40,710レースで発走前3時間観測不足0、湿度欠損0、湿球温度欠損0、前日降雨欠損0だった。

### NAR weather-v2 multi-environment capacity control

NARでも2023/2024を別environmentとし、前日降雨、湿度、湿球温度、日射、季節、馬場、競馬場からなる18 schemaへ、各年ごとの高配当Top1〜3 +1pt、全体/人気通り/荒れ/高配当/通常のTop1〜Top5非悪化を同時制約として課した。2023年三連単q90=82,522円を両年とouter 2025へ固定し、supported subsetの高配当件数ではなく各年の全高配当母数から必要paired hit数を計算した。年別minimum supportは10/20/30/50/100/200レースである。

108設定中10候補が両年gateを通った。最初のMDL最小候補は前日降雨×日射×季節、年support 50、49 cell・19 weightだったが、約75%のraceを変更し、2025は高配当+0.491pt、人気通り-0.074ptだった。目的関数をレビューすると、weight indexの小ささをpenaltyにしており、本番weight 0.50ではなく0.00側をtie-breakで優先する誤りがあった。baselineからの距離へ修正し、+1pt制約を満たす中で変更race数を最小化するsparse activation目的へ変更した。

activation最小候補は競馬場×前日降雨×湿球温度、年support 10、154 cell・18 weightで、2023/2024の変更率を最大23.30%まで縮小した。2023/2024高配当は+1.004/+1.005ptで、全区分Top1〜Top5も非負だった。凍結2025年13,426レースでは3,145レースをactivateし、全体Top1〜3 +0.067pt、人気通り+0.016pt、荒れ+0.243ptだったが、人気通りTop3 -0.029pt、Top4 -0.077pt、Top5 -0.010ptが残った。高配当1,427レースは-0.093pt、補正済み片側下限-0.428ptであり、棄却した。

2026 weather-v2 shadow coverageも監査した。historical v2 cacheは25競馬場・657,600 hourly行、2023-01-01〜2025-12-31までである。一方、`apps/venue-weather/data/venue_weather_v2_2026.duckdb`の`venue_weather_v2`は現時点で0行だった。したがって2026 NARへ湿度・湿球温度・日射cellを適用できるという証拠はなく、0補完やv1からの推測は行わない。Cloudflare v2 cron後のproduction 600件監査とlocal差分同期が完了するまで、2026 v2 shadow評価は保留する。

### ばんえいtree leafの階層subcell routing

ばんえいにも同じ方向安定化を適用した。2024年だけで選んだdepth 7・min leaf 10・18 leaf treeは、高配当Top1〜3 +1.876pt、全体+0.447pt、人気通り+0.357pt、荒れ+0.638ptだった。各leafを2025年で校正すると、15 leafはbaselineまたは標本不足、3 leafは効果方向不一致となり、そのまま安全に残るleafは0だった。

ただしleaf 2・weight 0.40は2025年の高配当14レースで+2.381pt、通常配当+0.392pt、全体+0.673ptだった一方、荒れTop4だけ-2.439ptだった。そこでこのleafだけを市場、entropy、session、発走前temperature/rain/wind/gust、model disagreementなど12 schemaへ分割し、baselineとweight 0.40の二択をMILPで再配置した。選択された子cellはsessionで、early/middleを0.40、lateをbaselineとするroutingだった。

この階層routingは2025年校正で全体+0.060pt、人気通り+0.058pt、荒れ+0.065pt、荒れTop4+0.194ptとなり、元の悪影響を除去した。しかし全高配当190レースへ戻したTop1〜3改善は+0.175ptに留まり、+1pt gateを満たさなかった。凍結後2026年では高配当0.000pt、全体-0.030pt、人気通り-0.044pt、荒れ0.000ptへ反転した。したがって「改善leafを害のあるsubcellから分離する」処理自体は校正期間で機能したが、未知年での改善再現性はなく、本番へ適用していない。

weightの自由度も段階的に検証した。baseline/0.40の二択から0.00〜0.40の9段階へ増やすと、選択はearly=0.30、middle=0.10、late=0.00となり、2026年の全区分差は0.000ptまで縮小した。すなわちweight shrinkageは外部期間の悪影響を除去したが、改善も残らなかった。さらに全41 weightを許可すると、同じleaf 2では市場×weather phaseが選ばれ、2025高配当は+0.702ptまで上昇した一方、2026高配当は-0.546ptへ反転した。weight自由度拡大によるselection variance増大が確認できる。

親treeでbaselineだったleafも含め、2025年に標本のある全leafへ12種類の子cellと41 weightを許可した。安全なrouteは3本で、leaf 2の市場×weather phase、leaf 26のentropy×発走前3時間temperature、leaf 4の市場×weather phaseだった。2025年校正では高配当+1.228pt、全体+0.644pt、人気通り+0.643pt、荒れ+0.646ptとなり、Top1〜Top5も全区分非負でpoint gateを通過した。しかし凍結2026年は高配当-0.546pt、全体-0.120pt、人気通り-0.132pt、荒れ-0.094ptだったため棄却した。

子cell mappingそのものを2024/2025の両environmentで同一にし、各年へ別々に高配当+1ptとrank非悪化制約を課した。leafごとに独立制約を置く方式は、年別supportを満たした4 leaf×12 schema=48通りがすべてinfeasibleだった。そこで複数leafのpaired hitを1つの親policyで合算する`tree leaf×market/entropy/session/temperature/weather phase`の7 global schemaへ変更した。support制限なしでは7 schemaすべて通ったが、選択policyは143子cell・33 weightまで膨張し、2026全体-0.330ptとなった。

構造リスクを抑えるため、各子cellの最低年別supportを1/2/3/5/10/20/30/50/100レースで検証し、条件を満たすpolicyのうち`key_count × log2(weight_symbols + 1) + weight_symbols`というMDL型記述長を最小化した。年10レース以上では7 schemaすべてinfeasibleで、改善が低頻度cellへ依存することが分かった。最大の安定supportは年5レースで、選択は`tree leaf×発走前3時間temperature`、33子cell・17 weight、記述長154.61 bitだった。2024/2025の高配当は+1.154/+1.228pt、全体+0.298/+0.342ptだったが、2026は高配当-0.820pt、全体-0.360ptとなり棄却した。補正済み高配当片側下限は-2.645ptだった。

この過程で既存`optimize_banei_high_payout_cells.safety_gate`が、高配当Top1〜3の+1ptは要求する一方、高配当stratum内のTop4/Top5悪化を非悪化制約から外していることを検出した。実際、旧gateでは2025高配当Top5 -1.053ptの候補が通過していた。高配当を含む全5 strataのTop1〜Top5を非負とするよう共通research gateを修正し、再選択後の候補でも上記2026外部結果は不合格だった。

この反転についてaction分布を監査した。routing activationは2025年8.514%から2026年8.551%、action分布のtotal variationは0.0201、Jensen-Shannon divergenceは0.00551 bitに留まった。入力から選ばれるaction構成はほぼ同じなのに効果符号が反転しているため、単純なcovariate/OOD driftではなく`P(outcome | cell, action)`のconcept driftである。したがって入力分布だけのdrift gateでは安全性を証明できず、新しい払戻・着順を伴う将来shadow評価が必要である。

探索中、2026研究contextにはnumericの`market_entropy`と`field_size`は存在する一方、固定境界から導出できる`cell_entropy`と`cell_field_size`列が保存されていない不整合を検出した。2024/2025と同じ固定境界をnumeric列へ適用して両cellを再生成し、欠損値や任意文字列では補完していない。これらは本番model featureではなく研究router用派生列なので、本番推論の欠落事故ではないが、2026 research routingでschema適用を失敗させるlocal pipeline bugとしてconsumer側を修正した。

### cell-local Pareto安定化とbounded compensation

multi-environment MILPは年全体の制約を満たす一方、ある子cellの悪化を別cellの改善で相殺できる。この相殺が外部年反転の原因かを切り分けるため、NARへ市場集中度、market entropy、model rank disagreement、頭数、発走時間帯を追加し、既存の前日降雨・発走前3時間湿度・湿球温度・日射・競馬場との17複合schemaを作った。市場・entropy・不一致境界は2023年だけで三分位をfitし、2024/2025へ固定した。

最初に、各子cell・各weightが2023/2024双方で全体・人気通り・荒れ・高配当・通常配当のTop1〜Top5を1 hitも悪化させず、高配当Top1〜3を改善する場合だけactivateした。204設定中+1pt候補は0で、最良は`湿球w2 × 市場集中low × model不一致middle`のweight 0.55だけだった。fit worst高配当は+0.024pt、凍結2025高配当は+0.023ptで、具体的には2025-05-22園田5R「不撓不屈スプリント」（三連単186,970円）がbaseline4位から3位へ改善した。ただし人気通りTop1〜3が0.000ptのため全体gateを満たさない。

局所悪化を各指標最大1〜2 hitまで許し、年全体では従来どおり全指標非負とするbounded-compensation greedyを612設定で検証した。最良でもfit worst高配当+0.196ptで、2025は全体-0.010pt、人気通り-0.032ptだった。さらにgreedyの見逃しを排除するため、局所harm上限で不安定armを除外した後、各年の高配当+1ptと全25 rank-stratum非悪化を同時に解く厳密0–1 MILPを153設定で実行したが、feasible候補は0だった。

ばんえいにも2024/2025を別environmentとして、市場・entropy・頭数・model disagreement・session・発走前temperature/rain/weatherを組み合わせた20 schema、support 20/30/50/100、zero-prior shrinkage 0/10/30の240設定を適用した。両年でcell-local Top1〜Top5非悪化となる非baseline weightは1つもなく、全raceがbaselineへfallbackした。したがって既存の2024/2025高配当+1.154/+1.228ptはcell間相殺なしには成立せず、2026での負方向反転を安全なsubcell分岐だけでは救済できない。

### honest weather policy tree

「1pt以上改善し、他範囲の悪影響はrouting/cellで隔離」を過去期間の同時最適化にしないため、NARの2023年をrace date単位で前後半に分離した。前半6,403レースで発走前weather v2、市場集中度、model disagreement、頭数、競馬場・track・gradeからmulti-output decision treeの分岐構造だけを学習し、後半7,204レースで各leafのweight 0.00〜1.00を0–1 MILPで割り当てた。三連単高配当の閾値は前半だけで決めたq90=87,641円である。

tree深さ2/3/4/5、最小leaf 100/200/400、高配当sample weight 3/9、局所悪化許容0/1/2 hitの72構成を検証した。各構成に、高配当Top1〜3平均+1pt、全体/人気通り/荒れ/高配当/通常配当のTop1〜5非悪化、全体Top1〜3正の同時制約を課したが、2023年後半の較正で全72構成がinfeasibleだった。よって2024の構成選択と2025 outer評価へ進む候補は0で、結果を見た分岐追加もデプロイも行っていない。

ばんえいは2024年前半900レースの禁止oracle上限が高配当+0.971pt（3 paired hit、+1pt制約には4 hit必要）である。routing入力に関係なく全41 weightから結果後にレース別最良値を選んでも到達不可能なため、前半でtree構造を学習した後に別sampleでweightを較正するhonest化は、この分割では数学的に+1ptを満たせない。gateを下げず、既存の2024全体→2025確認と2024/2025→2026 outerの結果を棄却根拠として維持した。

### venue-month anomaly・trajectory・weather regime cell

raw気温の同じ閾値が競馬場と季節で異なる生理的意味を持つ問題に対し、NARは2023年だけで競馬場×月ごとの中央値とMADをfitした。発走前3時間の気温、湿度、湿球温度、露点、日射を平年差へ変換し、各項目の最初の時間値から最後の時間値へのtrajectoryも追加した。参照は145競馬場月group、全レースで発走前3観測が完備し、未知groupは2023全体の参照値へfallbackする。

手動の平年差/trajectory 19 schemaに加え、天候だけの多変量ベクトルをK-means K=3/5/8でweather regime化し、市場集中度、model uncertainty、前日降雨と交差させた12 schemaも追加した。regime境界に着順・配当は使用していない。31 schema×年support 10/20/30/50/100/200の186設定に対し、2023/2024それぞれの高配当+1ptと全25非悪化制約を課したが、全設定がinfeasibleだった。よって2025 outerは未参照のまま候補0で終了した。

ばんえいは単一競馬場のため、2024年だけで月別の気温・降雨・風・突風の中央値/MADをfitし、発走前3時間平均と6時間平均の気温差、降雨の時間集中、K=3/5/8 weather regimeを作った。市場・entropy・model disagreement・sessionとの交差を含む29 schemaに、2024/2025の高配当+1ptと全25非悪化制約を別々に課したが、feasible候補は0だった。2026は選択に使わず、デプロイも行っていない。

Journal of Equine Scienceの国内JRA調査は、暑熱症頻度が競馬場×月で大きく異なり、南北の競馬場で夏季平均気温に約6℃差があると報告している。これはvenue-month anomalyの生理学的根拠だが、同論文の目的変数は暑熱症であり着順ではない。今回の独立年制約不成立により、その知見から着順精度改善を推測していない。

### ばんえい因果的天候modelとcell別weight

Apple Container上のlocal PostgreSQLを使い、ばんえい2016〜2025を現行feature builderで再生成した。基礎storeは156,401 runner・17,152 race、後段の血統、直接対戦、馬場、負担重量class、grade career、類似raceの6 layer適用後は280列で、本番130 featureをすべて含む。発走前3時間の気温、降水、風、突風と前1/3/7日降水のv1 7列は156,401 runnerすべて非nullだった。一方、Cloudflare v2由来の湿度、露点、湿球温度、日射6列は全17,152 raceでnullだったため、候補modelから明示的に除外した。

本番130 featureだけのCatBoost YetiRank 300 treeを同じ再生成dataからbaselineとして学習し、(1) causal v1 7 feature追加、(2) それらと馬のspeed、逃げ率、weight z-score、同track成績、sire track成績、発走時馬場を掛けた8 interaction追加の2 expertを作った。各expertはbaselineとのrace内z-score blendを0.00〜1.00、0.05刻みで持つ。これによりNAR/ばんえい全体共通weightではなく、各子cellがbaseline/raw-weather/interaction expertと固有weightを持てる。

単一weightではraw expertの高配当Top1〜3平均が2024年weight 0.65で+1.443ptだったが、通常配当・人気区分のTop1〜5に負値があり、2025年最大も+0.526ptだったため棄却した。interaction expertも2024年+0.577pt、2025年+0.702ptで1pt未満だった。

次に市場集中度、market entropy、頭数、session、発走前気温・雨・突風、baselineとのscore相関から20 schemaを構成し、親cellが最大27個の排他的子cellと子cell別expert/weightを管理する0–1 MILPを実行した。2024/2025それぞれに高配当Top1〜3平均+1pt、全体/人気通り/荒れ/通常配当のTop1〜Top5非悪化、全体Top1〜3正を別制約として置いた。20 schema中3つだけがfeasibleで、maximin選択は`market entropy × 発走前気温 × 直前3時間雨`、27子cell・24 active actionだった。選択期間の高配当は2024年+1.299pt、2025年+1.579pt、全体は+0.447/+0.624ptで、全rank-stratum制約を満たした。

この1 policyだけを凍結して2026年1月2日〜8月24日の結果確定1,111 raceへ適用した。高配当122 raceのTop1〜3平均は+0.273pt、全体+0.060pt、人気通り+0.132pt、荒れ-0.094ptだった。Top5は全体-0.360pt、荒れ-1.685pt、高配当-2.459ptである。高配当raceではwinner rank改善4件、悪化4件だった。同時27制約の日付cluster・Bonferroni片側下限は高配当Top1〜3が-0.541pt（必要値+1pt）で、point gateとconfidence gateの双方を不通過とした。router artifactは`rejected_by_outer_2026`であり、本番modelとMLflow registryを変更していない。

本番feature contractも監査した。MLflow champion artifactのばんえい130 featureとNAR 188 featureはいずれもrace record由来`weather_normalized`だけを使用し、Cloudflare/Open-Meteoのvenue weather列をscore projectionへ含めていない。したがって現在のCloudflare天候取得処理は本番予測値に影響しておらず、今回の比較は「既存本番modelへrouterだけ足す」ものではなく、同一現行dataでbaselineと天候feature modelを再学習した比較である。過去のweather-v9 artifactは09:00〜17:00実績集計を含み、早いraceでは発走後時刻を含むため、因果的比較やdeploymentへ再利用していない。

## 統計的レビュー

216設定から最良値を選ぶこと自体がmodel-selection overfittingを生む。[Cawley & Talbot (2010)](https://www.jmlr.org/papers/v11/cawley10a.html)は、有限標本上の選択基準の分散によってmodel selection自体がoverfitし、アルゴリズム間の差に匹敵する性能劣化が起こり得ることを示している。[Varma & Simon (2006)](https://pmc.ncbi.nlm.nih.gov/articles/PMC1397873/)も、hyperparameter選択と誤差推定に同じCVを使うバイアスを示し、nested CVが独立testに近い推定を与えると報告している。

したがって、今回既に観測した2026年7〜8月を次候補の昇格判定へ再利用しない。次に凍結する候補は、新たに結果が確定する将来期間でshadow評価し、Top1〜Top5非負、人気通り/荒れのTop1〜Top3平均正、日付cluster区間下限正を満たす場合だけデプロイ対象にする。

この昇格条件はhigh-confidence policy improvementの「baselineより悪いpolicyを高い確率で排除する」考え方に対応する。[Thomas et al. (2015)](https://proceedings.mlr.press/v37/thomas15.pdf)のcandidate generationとconfidence evaluationの分離を採用し、既に候補作成へ使った期間を信頼下限の確認へ再利用しない。また分布変化に対して平均値だけを最適化しない点はdistributionally robust policy evaluation/learningの考え方とも整合する。[Si et al. (2020)](https://proceedings.mlr.press/v119/si20a.html) ただし本件は全weight armの順位が同じraceで観測できるため、logged-banditのimportance weightは使っていない。

support不足・未知cellを本番baselineへ戻す実装は、[Laroche et al. (2019)](https://proceedings.mlr.press/v97/laroche19a.html)のSafe Policy Improvement with Baseline Bootstrappingに対応する。ただし同論文の保証を本件へ直接主張せず、ここではfull-information paired outcomeと年別制約を使った保守的類推として採用した。また、同じcell/weightが複数年で選ばれることを要求する方針は、subsampling間で安定して選択される構造を残す[Meinshausen & Bühlmann (2010)](https://www.research-collection.ethz.ch/entities/publication/b0d78be4-0747-4807-990e-04ef448344b8)のstability selectionに着想を得ている。本件では年をsubsampleと同一視せず、時間environment間の符号安定性という、より直接的な制約として実装した。

[Li et al. (2023)](https://proceedings.mlr.press/v202/li23ay.html)は平均utilityだけでは個体のcounterfactual harmを防げないとして、害を受ける割合の上限を制約するpolicy learningを提案している。本件では各raceについて全候補weightの順位を同じ確定着順に対して計算できるため、観測policyしか見えない通常の因果推論とは異なり、baselineよりwinner rankが悪化するraceの割合を推定上限ではなくpaired full-informationで直接数えられる。正規化チェーンの再構築後は、高配当Top1–3 `+1.000pt`と25平均非悪化制約に加え、年×競馬場environmentごとのexact harm fractionを最小化するlexicographic目的を候補にする。ただし既観測outer期間へ再適合せず、新しい将来shadowでのみ昇格判定する。

### NAR clean188因果天候model・payout expert・cell別signed weight

Apple Containerのlocal PostgreSQLから現行feature builderでNAR 2006〜2025を再生成し、2,673,394 runner・269,741 raceの確定着順行を得た。v1の発走前3時間気温・降水・風・突風と前1/3/7日降水は2014〜2025で全race完備だった。2006〜2012と2013年福山272 raceは観測期間外なので欠損のままにし、0補完していない。v2の湿度・露点・湿球・日射は全期間nullのため候補modelから除外した。

local scoring経路の再監査で、`score_nar_causal_weather_models.py`が既存frame cacheを既定で再利用し、feature storeを再生成してもcacheを自動無効化しないstale-data経路を確認した。既定を毎回materializeへ変更し、古いcacheの再利用は`--reuse-frame-cache`の明示時だけにした。また`--current-store`と`--frame-cache`を追加し、今回のcanonical chainを曖昧な固定pathではなく実行引数で指定できるようにした。回帰テスト10件とbasedpyright 0 errors/warningsを確認した。本番NAR metadataの実読値は`feature_count=188`かつ`feature_names` 188件であり、canonical最終storeはこの188列契約に対して監査する。

後段290列storeと現行builderを監査すると、旧storeの45 raceで`ketto_toroku_bango='0000000000'`の未登録馬1頭が同日別馬の履歴へ2〜23通り結合され、888 extra runner行になっていた。完全同一duplicateではなくfeature hashも異なったため、任意の先頭行を採らず45 race全体をscreeningから除外した。正規horse idの行は現行builderと一対一だった。2024は2 race、2025は43 raceが除外対象であり、候補がouterを通った場合でも現行builderから全後段layerを再生成し、これらを戻した完全評価をdeploy前必須とした。

MLflow champion v3 `iter12-nar-xgb-hpo-v8-clean188`の188 feature、rank:pairwise、seed 2068、metadata best iteration 62を固定し、同じdata・63 roundでbaseline、因果天候raw 7 feature追加、馬×天候interaction 8 feature追加を学習した。0〜1 blendの禁止oracle上限は2023高配当+0.358pt、2024+0.573pt、0〜2でも2023+0.639pt、2024+1.072ptであり、cell追加だけでは両年+1ptが数学的に不可能だった。weather残差を有界[-1, 3]で補正するとoracleは2023+1.892pt、2024+1.920ptになったが、会場・subclass・月・市場・天候・model geometryを含む34 schemaは全て安全制約付きでinfeasibleだった。

候補modelの誤り多様性を増やすため、学習期間内三連単q90以上のrace groupだけを1.25/1.5/2/3倍するclean188 expertと、2倍のweather-interaction expertを追加した。欠損配当は1倍のままで、本番routing入力へ配当を使っていない。単独高配当差は2023最大+0.051pt、2024最大+0.100ptに留まったが、7 expert×signed weight集合の禁止oracleは2023+3.349pt、2024+3.017ptへ拡大した。

親cellは最大300個の排他的子cellを管理し、各子がbaselineまたは7 expertの独立signed weightを持つ。2023/2024を別environmentとして、各年の高配当Top1〜3平均+1pt、全体・人気通り・荒れ・高配当・通常配当のTop1〜5全25指標非悪化、全体Top1〜3正をhard constraintにした。年30 race未満の子はbaselineへ固定した。初期実装では高配当Top4/Top5行が制約行列から欠け、2023高配当Top5 -0.230pt、2024高配当Top4 -0.524ptの候補が通る不整合を検出したため、そのrouteを無効化し、25指標すべてを生成するよう修正した。

0.25刻みのcoarse構造選択では4 schemaが完全gateを通り、`subclass × 発走前3時間気温 × 発走前3時間降水 × session`を選択した。親構造とexpertを固定し、各active子weightの周囲±0.20だけを0.05刻みでrefineした。最終selectionは2023高配当+1.201pt・全体+0.370pt・人気通り+0.332pt・荒れ+0.502pt・通常配当+0.282pt、2024高配当+1.147pt・全体+0.239pt・人気通り+0.195pt・荒れ+0.391pt・通常配当+0.141ptで、両年の25指標が全て非負だった。

このmappingを凍結して初めて2025を学習外outerとしてscoreした。2006〜2024の256,313 raceで学習し、fit-only q90=86,230円、上記43 raceを除く13,383 raceを評価した。outerは高配当1,363 raceでTop1 -0.220pt、Top2 -0.073pt、Top3 +0.220pt、Top1〜3平均-0.024ptだった。全体-0.095pt、人気通り-0.193pt、荒れ+0.244pt、通常配当-0.103ptであり、荒れ以外へ悪影響が出た。race-day cluster・Bonferroni同時片側下限は高配当-0.423pt（必要+1pt）、全体-0.253pt（必要0pt）だったため棄却した。model、router、MLflow aliasはデプロイしておらず、本番精度差は0.000ptである。

具体例では、2025-10-16川崎6R（三連単2,220,330円）はbaseline勝馬10位から8位、2025-05-22大井11R（696,090円）は4位から3位へ改善した。一方、2025-06-03盛岡3R（1,185,660円）は2位から3位、2025-04-10名古屋4R（990,910円）は8位から10位へ悪化した。最大配当の2025-03-11水沢4R（8,171,760円）など上位12件は全て順位不変だった。改善例だけでなく悪化例と不変例が併存し、aggregateの外部不合格と整合する。

実装上は、561 actionのMILPを直接解く前に安全制約なし高配当hitの厳密上界を計算し、+1pt未満schemaを除外した。cell別paired hit集計は参照Python loopと一致する疎/ベクトル化契約で検証し、coarse-to-fineで構造選択とweight微調整を分離した。これによりweight自由度は増やしたが、2025結果を見てrouteを再調整していない。

## 2026-08-25 Cloudflare v2安定化と差分監査

Cloudflare R2 SQLへ2026-01-01〜2026-08-25を集約照会した結果、`weather.venue_weather_hourly_v2`は0行、日付数0、競馬場数0だった。同日のv2専用local syncも`rows_from_cloudflare=0 / inserted=0 / updated=0 / unchanged=0`で、local `venue_weather_v2_2026.duckdb`も0行だった。最新v2 Workerの配信は2026-08-25 02:46 JSTより後で、監査時刻05:42 JSTにはactual cron（20:00 JST）が未到来だったため、2026 weather-v2をrouter評価へ補完・代入していない。

安定化配信後の公開API監査では`/ping=ok`、2026-08-25のv1はR2由来600行・25競馬場、v2項目付き0行だった。したがって既存天候配信は稼働しているが、v2初回生成は未確認として区別する。

キュー処理では、v2 runtime R2保存またはv2 Pipeline送信の失敗を`catch`してそのままmessageをackする経路を検出した。失敗を伝播するよう変更し、既存Queue設定の`max_retries=5`、`retry_delay=60`、dead-letter queueへ確実に接続した。runtime失敗とcatalog失敗の双方でackされないテストを追加し、Worker version `6ecdc91c-9c8f-4c13-b485-f52f34a276df`として配信した。これは天候欠落防止だけであり、予測model/MLflow registry/順位は変更していない。

2023〜2025履歴v2は657,600行、送信予定1,316 batch、全venue-date 24時間完備として本番送信なしのdry-runを通過した。実送信に必要な`VENUE_WEATHER_V2_BACKFILL_TOKEN`はlocal環境に存在せず、Cloudflare secretは読み戻せないため、tokenを推測・再設定せず停止した。

## NAR未登録馬IDの行増幅監査

旧`nar-full-regen/s11-pacestyle-FINAL`と新しいstage-1出力をrace key・馬番・登録番号で照合した。旧storeだけで`ketto_toroku_bango='0000000000'`が同一race内に複数あり、finalized対象は2024年2race、2025年43raceの計45race、2024-09-06は2倍、2025-04-05は20倍、2025-04-06は23倍だった。重複行間で変化したのは`career_place2_rate`、`career_place2_to_win_ratio`、`recent_place2_count_5`のnear-miss馬履歴特徴で、pace-style予測値は同一だった。local PostgreSQLの`race_entry_corner_features`とraw `nvd_se`では各raceの該当runnerは1行であり、source側重複ではない。

生成時点をGit履歴で照合すると、旧storeは2026-07-04生成、near-missの等値joinからASOF joinへの修正は2026-07-11だった。旧実装は`(source, ketto_toroku_bango, race_date)`で結合したため、未登録馬共通placeholder `0000000000`を同一馬とみなし、同日race数だけ行を増幅した。現行ASOFで行数増幅は解消済みだが、同じplaceholderの過去履歴を別の未登録馬へ流用し得る残存誤りがあった。

near-missの馬履歴・馬context・血統context・距離×gradeの4結合にknown-horse guardを追加した。`0000000000`ではこれらをNULLとし、race内情報と騎手履歴は維持する。実DuckDB回帰テストで、誤った馬履歴行を4系統すべて用意しても出力1行、馬特徴4系統NULL、騎手2着率0.2を確認した。全Python suiteは4,957件pass、coverage 97.56%で基準95%を満たした。

さらにraw `nvd_se`を同race・同馬番で照合すると、finalized 45/45件でplaceholder行と実登録番号行が1対1で併存し、着順・単勝オッズも完全一致した。これは未知の別馬ではなく、同一runnerの未解決版と解決版である。stage-1 source SQLに、同race・同馬番の実IDが存在するときだけplaceholderを除去するcanonicalizationを追加した。実IDのないplaceholderは保持し、実ID同士が複数ある別の異常は勝手に1件へ縮約しない。全stage-1天候付きstoreでは2,723,183行から46行（finalized 45 + non-finalized 1）を除去し、canonical store 2,723,137行、`(race_id, umaban)`重複0、placeholder残存0を確認した。

同じcanonicalizationで45 finalized raceを除外せず2025 outerを再学習した。13,426race、高配当1,366raceで、凍結済みrouterのTop1–3平均差は高配当+0.122pt、全体-0.022pt、人気通り-0.102pt、荒れ+0.255pt、通常配当-0.039ptだった。旧の45race除外評価（高配当-0.024pt、全体-0.095pt、人気通り-0.193pt、荒れ+0.244pt）より改善方向だが、要求の高配当+1ptと他範囲非悪化を満たさず、同時信頼下限も高配当-0.248pt、全体-0.182ptだった。routerは引き続き棄却、本番model変更なし。

2026前向storeには別形状の重複が3件あった。local `nvd_se`では同一race・馬番・馬名について、`data_kubun=2`の事前行と`data_kubun=7`の確定訂正行が異なる実登録番号で併存し、事前行だけが着順NULL、訂正行だけが着順3/1/2だった。`canonicalize_placeholder_runners_sql`を一般化し、同じrace・馬番に着順非NULL行が存在する場合だけ着順NULL行を除外するようにした。未確定同士または確定同士の複数実IDは自動選択せず残し、uniqueness監査をfailさせる。旧storeへのread-only適用では20,645行・一意runner 20,642から旧事前行3件だけが落ち、20,642行=`(race_id, umaban)`一意20,642となった。対象365テストとbasedpyrightは通過した。

全期間near-miss stageの旧版と最適化版を並行比較した際、`_resource_defaults.py`が全processへ固定`/tmp/duckdb-spill`を設定していたため、DuckDBの同名temp fileが衝突し双方がshort-readで停止するlocal並行実行bugも実観測した。model出力はCOPY前で0件だった。既定spillを`/tmp/duckdb-spill-<pid>`へ変更し、任意の絶対pathを`PIPELINE_SPILL_TEMP_DIR`で指定できるようにした。相対pathは拒否し、DuckDB文字列はquote escapeする。52テストとbasedpyrightが通過し、再実行は専用spillだけをopenしている。中断jobの8.8GB一時directoryはopen handleが無いことを確認してTrashへ移動した。

## 次期routerの論文・数学・統計レビュー

次期候補は2025を再びouterとして最適化しない。2025は既に観測済みなので、今後はtraining/calibration環境としてのみ扱い、promotionの外部評価を2026に固定する。

- 数学: hard child lookupではなく、1つのparent cellが複数childの学習範囲を所有するhierarchical mixture-of-expertsとする。child効果は`n/(n+tau)`でparentへ部分プーリングし、少数cellの極端weightを抑える。ICML 2024のMVMoEがOODでhierarchical gatingを検証しているが、競馬ではdeep gateをそのまま移植せず、説明可能なparent-child shrinkageとして実装する（https://proceedings.mlr.press/v235/zhou24c.html）。
- 統計: group DROのworst-group目的をyear×venue×payout-training-labelへ適用し、平均改善ではなく最悪environmentを最大化する。Sagawa et al.が指摘する通り、group DROだけでなくregularization/early stoppingを組み合わせる（https://arxiv.org/abs/1911.08731）。評価はrace内paired hit、day-cluster sandwich、27制約Bonferroniを維持する。
- policy learning: 各raceでbaselineと全expert actionを同時scoreできるため、action outcomeは反実仮想欠測ではなくfull-informationである。Athey–Wagerのdoubly robust policy learning（https://www.econometricsociety.org/publications/econometrica/2021/01/01/policy-learning-observational-data）は一般論として有用だが、この設定へpropensity/DR補正を足すと識別上の利益がなく分散だけ増やすため採用しない。代わりにbounded-depth policyとcross-year regretを直接最適化する。
- CS/安全性: action選択後に、2025 calibrationだけでabstention強度を決める。lossを「いずれかの非悪化制約違反」としたConformal Risk Controlの単調thresholdを用い、保証対象と探索対象を分離する（https://arxiv.org/abs/2208.02814）。最終2026はthreshold調整にも使わない。

実装順序は、(1) parent-child empirical-Bayes shrinkage、(2) year/venue group-DRO action utility、(3) 2025 CRC abstention calibration、(4) untouched 2026評価である。高配当Top1–3平均+1ptと全25非悪化制約の両方を通らない限りdeployment対象にしない。

parent=`subclass×temperature`、child=`rain×session`として最初のpartial-pooling実装を評価した。child action効果をtau=`30/100/300`でparentへ縮約し、各child上位30 actionだけを厳密MILPへ渡した。tau選択は2023/2024だけで行い、`tau=100`を選択した。選択年は2023高配当+1.099pt・全体+0.277pt、2024高配当+1.047pt・全体+0.178ptで全制約を通過した。しかし凍結後2025は高配当+0.098pt、全体-0.007pt、人気通り-0.067pt、荒れ+0.199pt、通常-0.019pt、高配当同時下限-0.318pt、全体同時下限-0.166ptで棄却した。hard routerより全体の悪化幅は縮んだが、高配当の外部一般化は改善しなかった。外部結果を使ったtau再選択はしていない。具体例では`nar:2025:0216:55:02`（三連単2,652,870円）がwinner rank 10→9、`nar:2025:0522:44:11`（696,090円）が4→3へ改善した一方、`nar:2025:0905:48:01`（718,050円）は9→10へ悪化した。最高配当`nar:2025:0311:36:04`（8,171,760円）を含む高配当上位は多くが不変で、+1ptへ届かない主因だった。

ばんえいにも同じpartial-poolingを適用した。parent=`market entropy×発走前3時間気温`、child=`降水cell`、tau=`30/100/300`、各child上位20 actionを候補にし、その後は各選択年の高配当+1ptと25非悪化制約を厳密MILPで課した。tauは2024/2025だけで選び、`tau=30`を凍結した。選択年のworst高配当は+1.299pt、worst全体は+0.466ptだったが、未使用の2026では高配当+0.273pt、全体+0.030pt、人気通り+0.088pt、荒れ-0.094pt、通常±0.000ptだった。高配当同時下限は-0.541pt（要件+1pt）、全体同時下限は-0.531ptで棄却した。荒れへの悪影響を親子cellで隔離しても外部高配当+1ptへ届かないため、2026結果を見てcellを追加する再最適化は行わない。

NARでは次にyear×venueをenvironmentとするgroup-DRO型shortlistを評価した。各child actionのTop1–3 paired utilityをparent=`subclass×temperature`へ部分プーリングし、各environmentの標準誤差ペナルティ後の最小値でactionを順位付けした。tau=`30/100/300`、標準誤差係数=`0/1/2`、各child上位action数=`30/60/90`は2023/2024だけで選択した。30/60 actionでは厳密+1pt・25非悪化MILPが全てinfeasibleで、90 actionだけがfeasibleだった。選択された`tau=300`、係数0、上位90はworst高配当+1.097pt、worst全体+0.232ptだった。しかし凍結後2025は高配当+0.073pt、全体-0.020pt、人気通り-0.086pt、荒れ+0.210pt、通常-0.030pt、高配当同時下限-0.337pt、全体同時下限-0.185ptで棄却した。外部一般化はhard/partial-pooling routerを上回らず、標準誤差ペナルティ0が選ばれた事実からも、現行score面ではvenue worst-group正則化の有効性を確認できなかった。

`high_confidence_cell_gate.py`には外部評価のdispositionも追加した。外部高配当の点推定が`+1.000pt`以上で、かつ他制約の点推定に負値がある場合だけ`split_on_prerace_features_then_revalidate_on_new_holdout`を返す。+1pt未達は`reject_to_baseline`、点推定は通るが同時下限だけ未達なら`insufficient_simultaneous_evidence`、全同時制約通過時だけ`promote`である。split候補も同じ外部期間で修理・再採用せず、発走前特徴だけで子cellを学習した後に新しい未使用期間を要求する。既存NAR group-DROを再集計すると、2025の負値は全体Top1/4、人気通りTop1/3/4、荒れTop4、高配当Top1、通常Top1/4/5だったが、高配当自体が+0.073ptなのでdispositionは`reject_to_baseline`となった。

ばんえいでは`docs/journals/papers/12_1_1-body-size-conformation-racing-performance-banei.md`の、一般体格・体重PC1と成績の有意な関連を新しいrouting仮説へ落とした。各モデルが発走前に選ぶ先頭馬について、過去5走平均体重のfield平均との差、自己平均との差、z-score、性別をrace-level score面へ追加した。親cellが2軸、子cellがさらに2軸を管理し、各子はbaseline/raw-weather/horse-weather interactionの独立weightを持つ。2024/2025だけでparent-child部分プーリングのtauとmappingを選ぶと、親=`baseline先頭馬の体重変化×直前3時間降雨`、子=`直前3時間気温×session`、tau=30、active child=50が選ばれ、最悪年の高配当Top1〜3平均は+1.404pt、全体は+0.805ptだった。既に他探索で観測済みの2026はpromotion holdoutとして扱わずshadow監査だけを行った。高配当±0.000pt、全体+0.060pt、人気通り+0.088pt、荒れ±0.000pt、通常+0.067ptで、全体Top2/4、人気通りTop2、荒れTop4/5、高配当Top5、通常Top2/4に点推定悪化があった。dispositionは`reject_to_baseline`であり、本番へは採用しない。成果物は`banei_body_weather_shadow_cell_report.json`と`banei_body_weather_shadow_cell_router.json`で、router自体も`deployment_eligible=false`へ固定した。

near-miss全期間再生成では、距離±200mの範囲joinを直接multi-million-row履歴へ適用していた実行計画をsamplingで確認した。対象距離と履歴のdistinct距離（実データ73値）の小さなbridgeを先にmaterializeし、以後を等値joinへ変えた。馬context・父距離・母父距離・馬距離×gradeの4か所を同じ形へ統一した。結果意味はstrict prior-dateのままで、51 focused testとbasedpyrightを通過した。bridge追加前は36分時点でも前処理中だったのに対し、追加後は約4分で最終ASOF/COPY段階へ到達した。最終完了時間と旧不等号joinとの数値同値性は出力完了後に別途確定する。

2026-08-25 10:05 JST時点の天候取得を本番APIとlocal R2 SQL差分同期で再監査した。本番v1は当日600行=`25会場×24時間`、直近7日のlocalも各日600行で、temperature/precipitation/wind_speed/wind_gustsの欠損は0だった。当日差分同期はv1 `inserted=0, updated=0, unchanged=600`、v2 `rows_from_cloudflare=0`である。v2 actual初回cronは同日20:00 JSTなので、10時時点の0行を障害とは判定しない。一方、Workerの`/weather`はKVとは別にCache APIへR2応答を60秒保存しており、25会場のQueue更新途中に作られた応答が最終jobのKV invalidation後も残る経路があった。共有応答Cacheを除去してKV/R2を唯一のread-through経路とし、version `407dabed-1583-4bb9-9164-adedc2b0ffab`へdeployした。deploy後3回の本番readはいずれも600行・25会場・24時間だった。v2は20:00以降に実データとlocal差分を改めて監査する。

発走前3時間降水だけでなく、前日降雨と7日累積降水をroutingへ明示した。`rain_phase={active_heavy (>0.5mm/3h), active_light, antecedent (3h dry/1d wet), dry}`、`soil7d`は2023だけでfitした50/80 percentile=`20.9/50.0mm`の3区分とし、temperature/subclass/session/venue/entropyとの7 schemaを厳密MILPで評価した。3 schemaが2023/2024の+1pt・25非悪化制約を満たし、選択された`temperature×rain_phase×soil7d×session`は2023高配当+1.483pt・全体+0.402pt、2024高配当+1.496pt・全体+0.405ptだった。しかし凍結後2025は高配当-0.220pt、全体-0.146pt、人気通り-0.259pt、荒れ+0.243pt、通常-0.138pt、高配当同時下限-0.642pt、全体同時下限-0.322ptで棄却した。前日・7日降水を追加しても2025のregime反転を防げず、2025は既に観測済みなので、この結果を使ったboundary/cell再選択はせずfuture shadow対象とする。

## 2026 `time_sa`修正・再学習XGBの分離評価

2026-07-01〜08-23のforward研究chainをclean188 metadataと照合すると、`trainer_grade_career_starts`、`trainer_grade_top3_rate`、`trainer_target_race_*`の6列が欠けていた。原因は研究用`run_nar_forward_holdout_regen.sh`が本番NAR `LAYER_CHAIN`にあるtrainer layerを省略していたためで、本番containerのchain欠落ではない。local PostgreSQLをread-onlyで短時間起動し、trainer layerと確定`nvd_hr`配当を生成した。研究runnerにもtrainer stepを追加し、以後同じ欠落を再発させない。

同race・馬番に確定行がある事前行3件を除外した結果、2026入力は20,642一意runner、2,057 race、うち結果確定2,040 raceとなった。確定2,040 raceは三連単配当も全件取得できた。Apple Container/PostgreSQLは抽出直後に停止した。

XGBoost 3.2 `ExtMemQuantileDMatrix`で2006〜2025を年batchとして学習し、old/fixedを同じ63 round・clean188 HPOで比較した。最大RSSは4,906,074,112 byte（4.57GiB）、wall 241.76秒だった。旧全件DMatrixで観測した約13.95GiBから約67%削減している。

修正featureのgainは`career_avg_2nd_margin_decisec=14.782`、`recent_2nd_margin_avg_5=12.273`だった。しかしfixed-vs-oldのTop1–3平均は全体+0.065pt（95%区間[-0.114,+0.245]）、人気通り+0.043pt、荒れ+0.075pt、高配当-0.163ptだった。荒れTop4は-0.676pt、高配当Top1/Top5は各-0.488ptであり、`time_sa`修正単独は外部年でも非悪化gateを通らない。

効果を分解すると、2006〜2025へ更新したold再学習XGB自体は現行0.50融合比で全体Top1–3+6.389pt、人気通り+10.535ptだったが、荒れ-7.432ptだった。fixed再学習XGBも全体+6.454pt、人気通り+10.579pt、荒れ-7.357ptである。したがって大きな全体差は`time_sa`修正ではなく学習期間更新によるもので、全race置換はできない。

この1pt超改善を安全なcellへ分離できるか、2024/2025の双方で同一child mappingを要求するrouterを追加した。22 schema×tau 3種×下限4種を評価し、各childは年50 race以上、各年のcell-local Top1〜Top5非悪化を要求した。policy全体にも各年の全体/人気通り/荒れ/通常/高配当Top1〜Top5非悪化と、高配当Top1〜3+1ptを課した。development候補は0件で、routerは`no_development_candidate`、`outer_2026_read=false`となった。2026 aggregateを見て2024/2025 cellを修理しておらず、精度model・MLflow registry・本番routingは変更していない。

## 検証

- NAR research focused tests: 36 passed
- Ban-ei exact scorer/managed-cell focused tests: 8 passed
- NAR managed-training-cell + direction-cell focused tests: 10 passed
- NAR high-payout +1pt/risk/uplift cell focused tests: 6 passed、Ruff/basedpyright pass
- NAR payout-weighted objective/cell focused tests: 6 passed、Ruff/basedpyright pass
- Ban-ei high-payout cell/extended surface/oracle/uplift policy focused tests: 12 passed、Ruff/basedpyright pass
- Ban-ei adjacent-weight fusion policy focused tests: 2 passed、Ruff/basedpyright pass
- Ban-ei payout-weighted CatBoost/model-cell/MILP focused tests: 4 passed、Ruff/basedpyright pass
- Ban-ei payout-risk auxiliary expert focused tests: 1 passed、Ruff/basedpyright pass
- Ban-ei continuous-payout objective/blend/MILP/tree/outer-2025 focused tests: 12 passed、Ruff/basedpyright pass
- Ban-ei 2026 scorer/environment-robust routing focused tests: 4 passed、Ruff/basedpyright pass
- simultaneous high-confidence gate + NAR/Ban-ei weather-v2 multi-environment integration focused tests: 32 passed、Ruff/basedpyright pass
- NAR/Ban-ei local-Pareto + bounded-compensation MILP focused tests: 10 passed、Ruff/basedpyright pass
- NAR honest weather policy tree focused tests: 3 passed、Ruff/basedpyright pass
- NAR/Ban-ei weather anomaly・trajectory・regime focused tests: 8 passed、Ruff/basedpyright pass
- Ban-ei causal weather model/per-cell router focused tests: 5 passed、Ruff/basedpyright pass
- Ban-ei hierarchical partial-pooling router focused tests: 6 passed、basedpyright 0 errors/warnings
- NAR year×venue group-DRO shortlist focused tests: 4 passed、basedpyright 0 errors/warnings
- NAR `time_sa` multi-year robust router focused tests: 3 passed、Ruff/basedpyright pass
- NAR antecedent-rain/7-day soil-load router focused tests: 3 passed、Ruff/basedpyright pass
- NAR causal weather model/payout expert/per-cell signed-weight router focused tests: 14 passed、Ruff/basedpyright pass
- Ban-ei production-recipe scorer/nested outer focused tests: 12 passed
- Ban-ei contextual policy focused tests: 4 passed
- NAR contextual policy focused tests: 3 passed
- NAR group-robust policy focused tests: 2 passed
- venue-weather TypeScript: 93 tests passed、statements 98.67%、branches 96.77%、functions 98.05%、lines 99.70%、lint/tsc/format pass
- venue-weather Python: 21 tests passed
- venue-weather v1/v2 local sync Python: 15 tests passed、changed source coverage 93%、Ruff/basedpyright pass
- Ban-ei/NAR trajectory and MILP focused tests: 14 passed
- Ruff lint: 0 errors
- pc-keiba-viewer Python: basedpyright 0 errors/warnings、4,957 tests passed、coverage 97.56%（基準95%）
- pc-keiba-viewer TypeScript: format、lint、tsc pass。coverage実行は4,757件中4,756件pass、既存`finish-prediction-inputs-cache.server.test.ts`のKV cacheテスト1件がlocalhost:3000接続拒否を伴って再現失敗したため未完了
- 精度条件を通過したmodelはなく、本番model・MLflow registryの変更なし。Cloudflare Workerは天候v1互換維持、v2 dual-write、失敗時Queue retry、可変日付応答のstale Cache API除去を含むversion `407dabed-1583-4bb9-9164-adedc2b0ffab`が配信中

## NAR荒れ −7.357ptの発走前棄却ゲート（2026-08-25）

`fixed` 再学習XGBを現行0.50融合と比較した2026年7月1日〜8月23日の荒れ
Top1–3平均は −7.357ptだった。勝ち馬人気や払戻を入力に使わず、発走前に観測できる
値だけで候補を棄却できるかを追加検証した。

- 候補XGBの予測1位馬が単勝1番人気という単純な市場合意ゲートは棄却。該当率が
  2024年95.1%、2025年94.4%、2026年94.6%と高すぎ、2026年の荒れは
  −6.982ptまでしか縮まらなかった。2024/2025の全体Top1–3も
  −0.656/−0.588ptだった。
- 2026年7月を開発、8月1日〜23日を時系列確認に分け、現行融合が持つ発走前の
  市場エントロピーを探索した。7月の低エントロピー閾値
  `market_entropy <= 1.1245109265794249`では、候補XGBへ切り替えるのは
  全2,040レース中186レースだった。
- 同ゲートの2026年全期間差は、全体Top1–3 +0.474pt
  （race bootstrap 95%区間 +0.245〜+0.719）、人気通り +0.561pt
  （+0.302〜+0.842）、荒れ 0.000pt（全Top1〜Top3が現行と同値）、通常払戻
  +0.509pt（+0.254〜+0.781）、高払戻 +0.163pt（0.000〜+0.488）。
  荒れのTop4/Top5も +0.676/+0.450ptで負ではなかった。
- 時系列分割では全体Top1–3が7月 +0.240pt、8月 +0.843pt、荒れは両期間
  0.000ptだった。一方、同じ固定閾値の2024/2025全体は −0.015/−0.007pt、
  荒れは −0.043/−0.044ptで、年をまたぐ厳格な点推定non-harmを満たさない。
- 結論: 発走前データだけで2026年観測区間の −7.357ptを0へ抑える
  abstention cellは作れる。ただし2026年7月で探索した事後候補であり、過去年の
  点推定も完全non-harmではないため、本番デプロイは禁止。新しい未観測期間での
  shadow確認、または同一学習期限で再学習したTransformerとの公平な融合再評価が必要。

実装上、`score_nar_timesa_fix_extmem.py` のrace-level出力へ
`old_top_market_rank` / `fixed_top_market_rank` を追加した。これらは候補モデルの
予測1位馬に対応する発走前人気順位であり、結果由来の `winner_market_rank` は
ルーティングに使用しない。年別再計算の最大RSSは2024年4.07GiB、2025年4.29GiB、
2026年4.18GiBだった。

## 同一期限Transformer融合とcell別weight（2026-08-25）

荒れ −7.357ptは、修正版XGB単体を現行XGB＋Transformer融合と比較した
architecture mismatchを含んでいた。Transformer本番artifactの生成コードは
2006〜2025全履歴学習を明記しており、2026修正版XGBも2006〜2025学習なので、
同じ3-seed Transformerを旧／修正版XGBの両方へ融合した。

- 修正版0.50融合の旧再学習0.50融合比は、全体Top1–3 +0.163pt、荒れ
  +0.300pt。XGB単体で観測された −7.357ptは再現しなかった。
- 本番artifact順位との直接比較では全体 −0.016pt、荒れ −0.075pt、荒れTop4
  −1.577ptで、0.50固定はnon-harmを満たさない。
- Transformer weight 0.60では荒れTop1–3 +0.751pt、荒れTop1〜5は全て正に
  なったが、人気通りTop1–3 −2.915pt。反対に0.45では全体 +0.964pt、人気通り
  +1.533ptだが、荒れ −0.901ptとなり、一律weightは禁止すべきtrade-offを確認。
- 2024/2025だけを使うMILPで、会場ごとのweightは両年全25指標non-harm、全体
  +0.765/+0.752pt、荒れ +2.510/+2.269ptだったが、2026外部年では全体
  −0.343pt、Top3 −0.931ptで棄却。
- 会場×市場エントロピーは2024/2025全体 +1.311/+1.294pt、荒れ
  +4.563/+4.216ptだったが、2026全体 −1.438pt、荒れTop4 −0.450ptで棄却。
  2026を見てmappingを再最適化していない。

結論は、本番融合と同じarchitectureに揃えることで荒れ −7.357ptの大半は解消
するが、固定weight・過去年cellとも2026の完全non-harmには届かない。本番変更なし。

## ばんえい高配当Top5制約修正（2026-08-25）

ばんえい因果天候routerのMILP安全行から高配当Top4/Top5が漏れていたため、
全5 strata×Top1〜5の25制約へ修正して再評価した。2024/2025選定候補は残ったが、
2026年1,111レースでは荒れTop3 −0.281pt、荒れTop5 −1.685pt、高配当Top5
−2.459pt。高配当Top1–3は +0.273pt、日cluster Bonferroni同時下限
−0.541ptで必要な+1ptを満たさず、deployment eligible=false。

## Cloudflare天候local差分取得の運用修理（2026-08-25）

- 正規local v1 DBは2026-01-01〜2026-08-25、237日×25会場×24時間の
  142,200 unique venue-hourを保持。
- v2 DBが0行で、既存LaunchAgentもCloudflare同期ではなく旧Open-Meteoを参照し、
  launchdへ未登録だったことを確認。
- R2 SQLから2026-08-19〜25を同期し、v1 4,200行（初回1,200更新）、v2 600行
  を取得。再実行はv1 4,200、v2 600が全件unchangedで冪等だった。
- v2はdual-write開始日の2026-08-25だけ存在し、25会場×24時間=600 unique key。
  過去日は未生成であり、0埋めやv1からの推測補完はしていない。
- 日次LaunchAgentをR2 SQL v1/v2の7日bounded差分同期へ変更し、06:00 JSTで登録。
  repository root `.env`からtokenを読み、plistへsecretを保存しない。
- `bun --filter`のpackage cwd契約に合わせ `--data-dir data` を固定。誤った相対pathで
  今回生成したnested directory 1.5MBはTrashへ移動済みで復元可能。

## 年をenvironmentとしたNARセル×weight最適化（2026-08-25追試）

2024/2025を別々のenvironmentとし、各年について全体・人気通り・荒れ・通常払戻・
高払戻のTop1〜5を全て非悪化とするMILPを追加した。baselineを選ぶactionを明示的な
abstentionとし、各child cellが独立したTransformer weightを持つ。

- 会場×市場entropy×発走前3時間気温のcellは、2024/2025全体Top1–3平均
  +1.813/+1.765pt、荒れ +6.312/+5.998pt。両年の全25指標は非負だった。
- 2026年7月1日〜8月23日の未使用2,040レースへmappingを固定適用すると、全体
  −1.601pt、人気通り −2.310pt、高払戻 −0.488pt。荒れだけは +0.976ptだった。
  年をまたいだ符号反転であり、deployment eligible=false。
- 2024〜2026を同時制約にして得た会場×entropy方策は、2026全体 +0.229pt、荒れ
  +0.150pt、高払戻 +0.163ptで全指標非負。ただし2026を設計に再利用したin-sample
  結果なので、将来shadow専用とし本番根拠には使わない。
- 計算は7.45秒、最大RSS約420MiB。vectorized contribution matrixと早期projectionで、
  XGBoost再学習を伴わず年別制約を解いた。

理論上はGroup DROの「年をgroup/environmentとしてworst-group riskを抑える」考え方と、
Selective Classificationの「確信がなければbaselineへabstainする」考え方を組み合わせた。
Conformal Risk Controlは、新しい未使用calibration期間が得られるまで適用しない。

- Sagawa et al., Group DRO: https://arxiv.org/abs/1911.08731
- Gangrade et al., Selective Classification via One-Sided Prediction:
  https://proceedings.mlr.press/v130/gangrade21a.html
- Angelopoulos et al., Conformal Risk Control:
  https://openreview.net/pdf?id=33XGfHLtZg

追加レビューでは、Safe Policy Improvement with Baseline Bootstrappingの「低supportの
state-actionではbaselineをそのまま使う」制約が、cell×weight routerの過適合対策に最も
近い。ただし同論文の保証はbatch RLのMDP仮定に基づくため、本件へ保証をそのまま移植せず、
各年のcell-action supportが閾値未満なら`w000`へ固定する設計原理だけを採用候補とする。
また、Hyperbox Searchは解釈可能なoff-policyをMILPで探索するため、固定binの代わりに
連続した市場entropy×気温範囲をcolumn generationで追加する次候補になる。本件は全action
のwinner rankを再計算できるpaired full-information評価なので、propensity補正は不要だが、
年別・日cluster下側信頼限界は引き続き必要。

- Laroche et al., Safe Policy Improvement with Baseline Bootstrapping:
  https://proceedings.mlr.press/v97/laroche19a.html
- Tschernutter et al., Interpretable Off-Policy Learning via Hyperbox Search:
  https://proceedings.mlr.press/v162/tschernutter22a.html

### 評価コンテキスト欠損の修正

初回実行で2024が13,677件ではなく13,675件になった。欠損は
`nar:2024:0906:48:02`と`nar:2024:0906:48:04`で、名古屋のlocal天候DBには当日
24時間が揃い、runner-level weather feature frameにも存在した。一方、古い
`nar_causal_weather_model_race_rows.parquet`だけが2レースを落としていたため、天候取得
欠損ではなく派生cacheの鮮度不整合と判定した。

optimizerはrace-levelモデル出力をcontextとして再利用せず、全13,677件を持つexact
market contextとrunner-level causal weather frameから必要列だけを投影し、一対一left
join後に必須天候欠損をfail-closedする実装へ変更した。別の1レース
`nar:2024:1025:42:01`は単勝市場値自体が欠損していたため、推測補完せず`missing` cellへ
分岐した。

古いweather model出力を再生成しようとした処理は、指定4GBに反してXGBoost学習が最大
RSS 25.56GiBへ達した時点で中断した。DuckDBのmemory limitはXGBoostへ効かないため、
通常`DMatrix`経路は採用していない。中断後に該当プロセスが残っていないことを確認した。

その後、学習frameを年単位で読む`DataIter`＋`ExtMemQuantileDMatrix`へ置換し、複数armの
model／matrixを同時保持せず1 armずつ学習・予測・解放するよう修正した。Parquet scan時点で
指定armのfeature、race key、label、payoutだけをprojectionし、`--train-only-arms`は必ず
単一foldへ制限した。

- cached baseline確認: 最大RSS 9.73GiB→約353MiB、1.52秒→0.78秒。
- cacheなし`payout300`の2024 fold実学習: 63 roundを94.13秒で完走、最大RSS
  3,485,876,224 bytes（約3.25GiB）。旧経路の25.56GiBから約87%削減。
- 学習artifact `fold-2024-payout300-0f74ea45d1a6.json`（984KiB）を生成したが、これは
  experimental cacheであり、本番modelやregistryには反映していない。

## ばんえいcontinuous blendの高払戻Top4/Top5制約修正（2026-08-25追試）

先に修正した因果天候routerとは別に、continuous blend共通MILPでも高払戻stratumの
contribution matrixがTop1〜3までしか生成されていなかった。さらに環境ロバストsolverの
safety rowsから高払戻が除外されていた。このため契約文と異なり、旧選択結果は2024
高払戻Top4 −0.433pt、2025 Top5 −0.526ptを許容していた。

共通matrixを全5 strata×Top1〜5へ拡張し、高払戻Top1〜5も年別安全制約へ追加した。
安全制約はTop1〜5、+1pt目標とobjectiveはTop1〜3だけへ明示的に分離した。再評価後の
2024/2025選択結果は全体 +0.671/+0.523pt、高払戻Top1–3平均 +1.876/+1.930ptで、
高払戻Top4/Top5も両年非負になった。しかし2026外部1,111レースは全体 −0.270pt、
人気通り −0.442pt、高払戻 −0.546ptで、同時信頼下限も不合格。multi-environment
subcellも2026全体 −0.300pt、人気通り −0.530ptで棄却した。

2024〜2026の3年全てに同じ完全制約を課した将来shadow設計では、固定cell 36 schemaと
tree 28設定のいずれにもfeasible candidateが0件だった。2026を見た後でも高払戻
Top1–3 +1ptと全strata Top1〜5非悪化を同時に実現できないため、ばんえいrouterは保存
のみで選択候補なし、本番変更なし。

## SPIBB型support下限と発走前hyperbox（2026-08-25追試）

NARでは固定binだけでなく、2024年だけを使って市場集中度・market entropy・発走前3時間
気温・頭数のaxis-aligned decision treeを学習し、leafを解釈可能なhyperbox cellとした。
各leafのTransformer weightは2024/2025を別environmentとしてMILPで割り当て、各年
30/50/100 race未満のleaf-actionはbaselineへ固定した。tree depth 2〜6、最小leaf
100/250/500の45構成中33構成が両年の全5 strata×Top1〜5非悪化を満たした。

選択はdepth 6、最小leaf 100、年別support 30、active leaf 11だった。2024/2025は全体
Top1–3 +0.380/+0.353pt、荒れ +1.467/+1.417pt、高払戻 +0.449/+0.830ptで全25指標
非負。しかし凍結後2026年2,040 raceでは全体 −0.458pt、人気通り −0.648pt、荒れ
+0.225ptとなり、荒れTop3 −0.225ptを含むrank別悪化が出た。日付cluster・Bonferroni
同時下限も全体Top1–3 −0.949ptで不合格だった。2024〜2026の三environment全45構成を
将来shadow設計として再計算してもfeasible候補は0件で、デプロイ対象なし。

ばんえいのenvironment solverには、従来treeの`min_samples_leaf`とは独立に「各年で
同一cellがsupport下限未満なら非baseline weightを選択不能にする」上限制約を追加した。
20/50/100 raceを探索するとfeasible候補は4件で、年20 race下限の
`session × market × rank-shift`を選択した。2024/2025は全体 +0.261/+0.302pt、
高払戻 +1.010/+1.228ptで全rank非負だったが、2026は全体 +0.090ptに対してTop4
−0.810pt、人気通り −0.132pt、高払戻Top4 −1.639ptとなり同時gateを不通過。旧方策より
全体Top1–3の点推定は改善方向でも、rank別non-harmを満たさないため本番へは採用しない。

これはSPIBBのMDP保証を転用したものではなく、低support領域でbaselineを維持する設計原理
だけをpaired full-information routerへ適用した。NAR hyperboxは11.65秒・最大RSS約481MiB、
ばんえいsupport探索は34.73秒・最大RSS約339MiB。関連テストはNAR 7件、ばんえい4件、
Ruff・basedpyrightともに通過した。

## model非合意common-supportと詳細親子cell（2026-08-25追試）

NARの発走前model geometryを監査すると、欠損は0件だった一方、`model_disagrees`は
2024/2025の13.7/12.8%から2026外部区間45.6%、`rank_disagreement_mean`は
0.547/0.542から1.701、`score_correlation`は0.970/0.971から0.645へ変化していた。
このdistribution shiftを無視した天候cellが外部年で反転するため、2024だけで
rank非合意上限とscore相関下限をfitし、joint common-support外をbaselineへ戻した。

2024/2025だけで選択したhonest policyは全体 +2.394pt以上、荒れ +8.344pt以上だったが、
2026は全体 −0.114pt、人気通り −0.086pt、高払戻 −0.163ptで棄却した。2026を含む
将来shadow設計では親=`会場`、子=`rank非合意×score相関`、年別support 10、active子cell
20、各cell独立weight 0.30〜0.95を選択した。Top1–3平均は次の通りである。

- 2024: 全体 +0.258pt、人気通り +0.075pt、荒れ +0.891pt、通常 +0.189pt、
  高払戻 +0.898pt。
- 2025: 全体 +0.340pt、人気通り +0.144pt、荒れ +1.018pt、通常 +0.265pt、
  高払戻 +1.000pt。
- 2026: 全体 +0.049pt、人気通り +0.043pt、荒れ +0.150pt、通常 +0.036pt、
  高払戻 +0.163pt。

全5 strata×Top1〜5の点推定最小値は3年とも0以上で、符号条件だけは満たした。しかし
日付cluster・Bonferroni同時下限は全体Top1–3が2024 +0.123pt、2025 +0.186pt、
2026 −0.244ptで、family全体の最小下限も −0.329/−0.448/−0.783ptだった。
2026を設計に使用しsupport 10のため、次期shadow routerとしてのみ保存し本番採用しない。

ばんえいでは2026 context cacheだけ`cell_entropy`と`cell_field_size`が欠けていた。原数値の
`market_entropy`と`field_size`は欠損0だったため、推測補完せず共通の決定的`add_cells`を
2024/2025/2026全てへ再適用し、派生列契約を一致させた。さらに2026で
`score_difference_std`が2025の0.0522から0.0819、直前3時間降雨が0.331mmから
0.488mmへ変化していたため、rank非合意・score相関・score差・降雨のcommon-supportを
2024だけでfitした。

3年同時shadowの90構成中78件が点推定安全gateを通り、親子schema=
`rank非合意×score相関×発走前3時間気温×発走前3時間降雨`、年別support 10、active子cell
21、cell別weight 0.15〜1.90を選択した。

- 2024: 全体 +0.410pt、人気通り +0.412pt、荒れ +0.406pt、通常 +0.343pt、
  高払戻 +0.866pt。
- 2025: 全体 +0.443pt、人気通り +0.292pt、荒れ +0.775pt、通常 +0.341pt、
  高払戻 +1.228pt。
- 2026: 全体 +0.210pt、人気通り +0.221pt、荒れ +0.187pt、通常 +0.202pt、
  高払戻 +0.273pt。

全Top1〜5点推定は非負だが、2026の同時下限は全体 −0.317pt、高払戻 −0.555ptで、
各年の同時gateはいずれも不通過だった。高払戻+1pt制約を外した安全shadow層の結果であり、
従来の+1pt探索契約は変更していない。新しい未観測期間で同じ20/21子cellを凍結評価するまで
NAR・ばんえいとも`deployment_eligible=false`、本番model・MLflow alias変更なし。

## 暦半期environmentによる期間ロバスト化（2026-08-25追試）

年平均で隠れる時系列反転を抑えるため、結果を見て期間を切らず、暦上の1〜6月・7〜12月を
固定environmentにした。NARは2024上/下期、2025上/下期、利用可能な2026年7〜8月の
5 environment、ばんえいは2024〜2026各上/下期の6 environmentである。

NARは各期間について全5 strata×Top1〜5非悪化と最低1 hit改善を要求した。common-support
90/95/99%、年期間support 5/10/20、詳細schema 10種から61候補がfeasibleとなり、
親=`会場`、子=`rank非合意×score相関`、support 5、active子cell 47を選択した。

- 2024: 全体 +1.021pt、人気通り +0.261pt、荒れ +3.651pt、通常 +0.794pt、
  高払戻 +3.116pt。
- 2025: 全体 +1.043pt、人気通り +0.355pt、荒れ +3.420pt、通常 +0.826pt、
  高払戻 +2.953pt。
- 2026: 全体 +0.065pt、人気通り +0.043pt、荒れ +0.225pt、通常 +0.054pt、
  高払戻 +0.163pt。

全期間・全rank点推定は非負で、全体Top1–3の同時下限は2024 +0.772pt、2025
+0.776ptまで改善した。2026下限も年environment版の−0.244ptから−0.163ptへ改善したが
まだ負で、family全体も不合格。2026を設計に使用しsupport 5のためshadow専用である。

ばんえいは各半期で改善hitまで強制すると90構成すべてinfeasibleだった。そこで既存の
年別+1pt契約は変更せず、期間shadowだけ「全rank非悪化、改善0ならbaseline abstention可」
を追加した。90候補全てが非悪化となったが、選択は
`rank非合意×score相関×entropy`のactive子cell 1、weight 0.15まで縮退した。

- 年別全体は2024 +0.037pt、2025 +0.020pt、2026 +0.030pt。
- 荒れは0.000/+0.065/+0.094pt、高払戻は全て0.000pt。
- 全体同時下限は−0.040/−0.039/−0.059ptで不合格。

期間をまたいで安定するばんえい改善情報は現データではほぼ1子cellしかなく、年別詳細routerの
大きな点推定を安定効果とは扱えない。NAR半期router、ばんえい半期abstention routerとも
`deployment_eligible=false`で保存した。

## 月別非悪化制約と季節cellの追加検証（2026-08-25）

「欲しい結果だけ」を過去データ上で選別するため、環境を年・半期から暦月まで細分化した。
各暦月について全体、人気通り、荒れ、通常配当、高配当の各層とTop1〜5をすべて0pt以上に
制約し、改善の根拠がないcellは現行モデルへabstainするようにした。支持件数不足のcellは
非baseline weightを選択できない。NARは共通支持範囲2種、詳細cell 7種、月別支持件数
2/5/10の42構成、ばんえいは共通支持範囲3種、詳細cell 10種、支持件数10/20/30の90構成を
実測した。

- NARは42構成すべてのworst scoreが0.0ptで、選択routerのactive ruleは0件だった。
  2024、2025、2026の全5層もすべて差分0.0pt。26暦月の全層・全順位で非悪化を要求すると、
  過去に共通して正方向だったcell/weightは残らなかった。
- ばんえいも90構成すべてのworst scoreが0.0pt、active rule 0件で、3年・全5層が0.0pt。
  特に月別高配当は4〜31レースしかなく、支持件数と全順位非悪化を満たす改善cellがなかった。
- NAR月別探索は16.95秒、最大RSS 551,256,064 bytes。ばんえい月別探索は438.15秒、
  最大RSS 385,269,760 bytesで、単一Pythonプロセス、swap 0だった。

さらに将来時点でも既知の月番号（01〜12）をrouting cellへ追加し、NARを2024/2025だけで
学習して2026を未使用outerに戻した。63候補から選んだ季節routerは学習年では全体
+2.598/+2.453pt、荒れ+9.583/+8.842ptだったが、2026では全体-0.049pt、人気通り
-0.216pt、高配当-0.325pt。63候補中、2026の全5層・Top1〜5を点推定で非悪化にできた
候補は0件だった。同時信頼下限も全体Top1〜3で-0.468ptのため棄却した。季節routerの探索は
92.56秒、最大RSS 600,227,840 bytesだった。

したがって、分類詳細度を上げるだけでは未来の正方向を保証できず、今回は「改善cellだけを
残す」条件が全baseline abstentionへ収束した。点推定の良い学習cellを本番へ入れることは
過適合になるため、全routerを`deployment_eligible=false`として本番・MLflowを変更していない。

## Prequential階層LCB router（2026-08-25）

固定cellの追加だけでは時間変化を吸収できなかったため、各対象月を予測する際に対象月以降を
完全に除外し、過去12/24か月と直近3/6か月の両方で効果を推定するprequential routerを追加した。
子cell効果を親cellへ有効標本数`tau`で縮約し、race-day cluster標準誤差を引いたlower-confidence
scoreが正のactionだけを許可する。共通支持範囲外、支持不足、recent/full historyのどちらかが
負のcellはbaselineへabstainする。

設計は、逐次的に予測と観測を繰り返す[Dawidのprequential原則](https://rss.onlinelibrary.wiley.com/doi/10.2307/2981683)、
不確実なstate-actionをbaselineへ戻す[SPIBB](https://proceedings.mlr.press/v97/laroche19a.html)、
逐次監視下でも有効な区間を扱う[Howard et al.のconfidence sequence](https://doi.org/10.1214/20-AOS1991)
を参照した。ただし、本実装の階層LCBは経験Bayes shortlistであり、MDP上のSPIBB保証や
time-uniform confidence sequenceそのものではない。最終昇格は別の未使用期間に対する既存の
day-cluster Bonferroni同時信頼ゲートで判定する。

NARは発走前のmodel score相関、rank非合意、entropy、発走前3時間気温、頭数、会場を組み合わせた
5 schema×7設定=35構成を2025 prequentialで選択した。

- 厳格点推定ゲート通過は0/35。
- 参考上位は`会場×score相関×発走前3時間気温`、24か月履歴、直近6か月、`tau=100`、
  child支持100、SE penalty 1.0。
- 2025は全体+1.157pt、荒れ+6.031pt、高配当+4.295ptだが人気通り-0.253ptで不合格。
- 2026年7〜8月は変更42/2,040レース、全体-0.016pt、人気通り-0.130pt、荒れ+0.450pt、
  高配当0.000pt。全体Top1〜3同時下限-0.221ptで不合格。
- 実行121.41秒、最大RSS 562,937,856 bytes、swap 0。

ばんえいはmodel score相関、rank shift、entropy、発走前3時間気温・降水を組み合わせた
5 schema×5設定=25構成を同じ契約で評価した。

- 2025安全ゲート通過は0/25。参考上位も全体-0.101pt、人気通り-0.175pt、荒れ+0.065pt。
- 2026は全体-0.030pt、人気通り-0.088pt、荒れ+0.094pt、高配当0.000pt。
- 全体Top1〜3同時下限-0.268pt、実行24.92秒、最大RSS 324,091,904 bytes、swap 0。

両方で「荒れ正、人気通り負」が残ったため、2026結果から生成したshadow仮説としてNARに
発走前favorite share/entropyのmarket child vetoを追加した。20構成中2025厳格ゲート通過は0。
参考上位は2025全体+0.074pt、人気通り+0.029ptまで改善したが、人気通りTop2/Top3が
-0.058/-0.019ptで不合格。再利用済み2026では変更2レースだけとなり、全体-0.016pt、人気通り
-0.022pt、荒れ0.000pt、同時下限-0.063ptだった。市場childは害を縮小したが改善情報もほぼ
baselineへ縮退した。2026を見て作った仮説なので、結果にかかわらず新しい未使用期間なしには
昇格しない。探索は125.84秒、最大RSS 590,659,584 bytes、swap 0だった。

## Weather取得経路の再監査（prequential探索後）

- 公開Workerを実運用相当の明示User-Agentで再取得し、08-24/25ともHTTP 200、600行、
  25会場×24時間、重複0。08-24は`source=r2`、08-25はcache後の`source=kv`だった。
- 08-25のhumidity、dew point、wet-bulb、shortwave radiationは600行全て完全。08-24は
  dual-write開始前なのでv2を推測補完していない。
- R2 SQL→local差分同期はv1 1,200/1,200行、v2 600/600行がunchanged、insert/update 0。
- 本番predict containerと同じfetcherをlive接続し、明示User-Agent、3回retry、10秒timeout、
  日付・25会場・24時間・重複検証を通して600行を取得。一時DuckDBのv1/v2双方へ
  600行・25会場・24時間を書き込めた。Python標準のdefault User-AgentだけはWAFで403となるが、
  本番fetcherには既に`horse-racing-data-predict/1.0`が設定されており本番経路では再現しない。
- venue-weather TypeScript 93件、同期Python 21件、本番fetcher 16件がpass。

新routerは全て精度・同時信頼ゲート不通過のため、本番model、router、MLflow registryは変更して
いない。

同時にCloudflare本番endpointを正式な8桁`race_date=20260825`で再監査した。本番は
`source=r2`、600行、25会場×24時間、venue-hour重複0、v2追加指標600行だった。local
DuckDBもv1/v2各600行・600 unique key・25会場・24時間で一致した。最初のハイフン付き
queryは仕様通りHTTP 400であり、データ障害ではない。

## Cloudflare本番・local完全性再監査（2026-08-25）

- R2 SQLの2026-08-24〜25を再同期し、v1は1,200/1,200行、v2は600/600行が全件
  unchanged。v1は両日とも25会場×24時間=600 unique key、重複0。
- v2はdual-write開始日の08-25のみ600 unique keyで、humidity、dew point、wet-bulb、
  shortwave radiationは各600件全て非null。08-24はremoteにもv2行がなく、推測補完なし。
- 公開本番endpointを直接確認し、08-24/25とも`source=r2`、600行、25会場、24時間。
  08-25だけv2追加4指標が600行全て付与されていた。
- 本番predict containerは3回retry、10秒timeout、25会場×24時間、日付、時刻範囲、
  venue-hour重複を検証し、不完全応答をDuckDBへ書かずNULL-weather fallbackへ閉じる。
- LaunchAgentは06:00 JSTに登録済みで、この時点ではscheduled run前のためruns=0、
  state=not running。手動同期の冪等性は確認済み。

検証はvenue-weather TypeScript 93件、同期Python 21件、production weather fetcher 16件、
ばんえい関連15件、NAR環境ロバスト4件がpass。精度ゲートを通過する候補はなく、本番
model・MLflow registryは変更していない。

## 成果物

- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_per_cell_weight_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_per_cell_weight_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_forward_20260701_20260823_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_arbitrary_composite_weight_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_range_composite_weight_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_tree_cell_weight_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_weight_oracle_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_upset_probability_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_current_exact_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_forward_20260701_20260823_exact_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_direction_managed_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_direction_managed_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_current_exact_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_managed_training_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_managed_training_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_managed_training_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_managed_training_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_managed_training_cell_min2y_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_production_recipe_exact_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_nested_outer_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_contextual_policy_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_contextual_policy_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_group_robust_policy_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_trajectory_nested_outer_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_trajectory_nested_outer_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_extended_w000_w200_exact_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_forward_extended_w000_w200_exact_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_high_payout_nested_outer_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_high_payout_nested_outer_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_payout_weighted_xgb_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_payout_objective_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_production_recipe_extended_w000_w200.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_high_payout_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_high_payout_oracle_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_high_payout_policy_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_fused_weight_policy_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_payout_weighted_catboost_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_payout_weighted_model_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_payout_weighted_model_milp_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_payout_risk_expert_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous_payout_catboost_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_milp_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_tree_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_2025_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_tree_outer2025_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_milp_outer2025_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei-2026-final/race_year=2026/data_0.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_2026_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_robust_outer2026_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/high_confidence_cell_gate.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/test_high_confidence_cell_gate.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_nar_weather_v2_range_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_weather_v2_range_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_nar_weather_v2_multi_environment_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_weather_v2_multi_environment_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_calibrated_leaf_outer2026_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_leaf_subcell_outer2026_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_banei_leaf_subcell_outer2026.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_banei_multi_environment_subcells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_continuous05_blend_multi_environment_subcell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_pareto_stability_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_pareto_stability_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_bounded_compensation_milp_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_honest_weather_policy_tree_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_weather_anomaly_trajectory_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_weather_anomaly_regime_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_causal_weather_model_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_causal_weather_model_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_causal_weather_model_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_causal_weather_model_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_causal_weather_model_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_causal_weather_model_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_hierarchical_shrinkage_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_hierarchical_shrinkage_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_hierarchical_shrinkage_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_hierarchical_shrinkage_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_group_dro_shortlist_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_group_dro_shortlist_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_antecedent_weather_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_antecedent_weather_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2026_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2024_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2025_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2026_race_rows_v2.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2024_report_v2.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2025_report_v2.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_fix_extmem_2026_report_v2.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/score_nar_timesa_transformer_fusion.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_transformer_fusion_2024_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_transformer_fusion_2025_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_transformer_fusion_2026_race_rows.parquet`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_multi_year_router_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_timesa_multi_year_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_environment_robust_fusion_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_environment_robust_fusion_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_environment_robust_shadow_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_environment_robust_shadow_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_nar_spibb_hyperbox_fusion_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_spibb_hyperbox_fusion_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_spibb_hyperbox_fusion_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_nar_common_support_fusion_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_common_support_fusion_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_common_support_fusion_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_banei_common_support_detail_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_common_support_detail_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_common_support_detail_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_nar_period_robust_detail_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_period_robust_detail_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_period_robust_detail_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_banei_period_robust_abstention_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_period_robust_abstention_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_period_robust_abstention_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_nar_month_robust_detail_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_month_robust_detail_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_month_robust_detail_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_nar_seasonal_detail_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_seasonal_detail_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_seasonal_detail_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_banei_month_robust_abstention_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_month_robust_abstention_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_month_robust_abstention_cell_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_nar_prequential_hierarchical_lcb_router.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_prequential_hierarchical_lcb_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_prequential_hierarchical_lcb_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/optimize_banei_prequential_hierarchical_lcb_router.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_prequential_hierarchical_lcb_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/banei_prequential_hierarchical_lcb_router.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/design_nar_prequential_market_veto_cells.py`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_prequential_market_veto_cell_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_prequential_market_veto_cell_router.json`

## 2026-08-25: local-only 2026-08-24 production holdout

Neon の履歴全量 scan は禁止し、実行中だった remote feature build を停止した。以後の
特徴生成・払戻取得は Apple Container 上の local PostgreSQL のみを使用した。local の
`nvd_ra / nvd_se / nvd_hr` は 2026-08-24 をそれぞれ 57 / 596 / 57 行保持していた。

1 日限定 (`from_date = to_date = 20260824`)、4 thread、DuckDB memory limit 4GB で
45 NAR レース・476 feature rows を 36.9 秒で生成した。最大 RSS は 4.24GB。その後、
確定着順を持つ473頭を production clean188 XGBoost と production iter40 Set Transformer
artifact で score し、45 race rows を作成した。score は0.4秒、最大 RSS 276MB。
天候は Cloudflare 差分同期済み local DuckDB から、発走時刻より前の3h/6hだけを join
した。

2024–2026-08-23で設計済みの粗い `venue × market entropy` routerを未使用の
2026-08-24へ適用すると、変更2/45レースで以下となった。

- 全体 Top1–3平均: **−1.481pt**
- 人気通り: **−3.704pt**
- 荒れ: **−0.926pt**
- 高配当（2023 q90固定82,522円以上、3レース）: **−11.111pt**

よって粗いrouterは棄却した。

次に親cellを `venue × entropy`、子cellを score correlation、直前3h気温、頭数、
rank disagreement、市場集中、Top3 overlap、およびそれらの交互作用とする9 schemaを
追加した。1つの親cellが複数の子cellを管理し、子cellごとに独立した5%刻みweightを
持つ。各子weightは2024・2025・2026-08-23までの各環境でTop1–Top5非悪化を満たす時
だけ有効にし、未知・不安定な子cellはproduction weight 50へabstainする。

この条件で安全候補は **0/9 schema**。したがってholdoutでは45/45レースをproduction
50%へ戻し、全体・人気通り・荒れ・通常配当・高配当の全指標が0.000ptとなった。
悪化は除去できたが改善ではなく、独立race-day clusterも1日だけなのでデプロイ対象外。
2026-08-25はlocal PG上で56レースすべて `data_kubun=2`、確定勝馬0件であり、第二の
holdoutにはまだ使用できない。

実装・証跡:

- `evaluate_nar_production_holdout.py`
- `nar_production_20260824_router_report.json`
- `optimize_nar_nested_managed_holdout_router.py`
- `nar_nested_managed_holdout_report.json`
- `nar_nested_managed_holdout_router.json`
