# JRA high-payout accuracy and routing review — 2026-08-25

## Decision

No production routing change. The odds-free Stage-1 expert clearly moves longshot winners upward in parts of 2026, especially ranks 3–5, but a pre-race classifier could not identify those races without reducing another tracked segment/rank. Payout columns are used only as post-race labels and evaluation strata; they are never model inputs.

## Evaluation populations

- 2025: leakage-safe walk-forward runner scores, 3,461 scored races.
- 2026: current routed CatBoost and the Stage-1 iter500 expert rescored over 1,663 settled races from January 4 through July 12.
- High payout: win payout >=1,000 yen **or** trifecta payout >=500,000 yen.
- Local `jvd_hr` is current only through 2026-08-16, while `jvd_ra`/`jvd_se` reach August 23. Therefore August 22–23 production predictions cannot yet be honestly payout-stratified. This is an evaluation-data freshness gap, not a prediction feature gap.

## Aggregate accuracy

### 2025

| Segment                 | Model           |       Top1 |       Top2 |        Top3 |        Top4 |        Top5 |
| ----------------------- | --------------- | ---------: | ---------: | ----------: | ----------: | ----------: |
| win >=1,000 (n=794)     | current routed  |     0.000% |     0.252% |      2.267% |     10.327% |     26.322% |
|                         | iter500 Stage-1 | **0.504%** | **2.015%** |  **7.683%** | **17.758%** | **29.723%** |
| trifecta >=500k (n=171) | current routed  | **3.509%** | **5.848%** |      9.357% |     13.450% |     22.222% |
|                         | iter500 Stage-1 |     2.924% |     4.678% | **11.111%** | **19.883%** | **25.146%** |

The odds-free expert consistently improves deep longshot capture. For broad exotic-payout races it trades some Top1/Top2 for Top3–Top5, which is why rank-prefix preservation was tested.

### 2026 through July 12

| Segment                | Model             |   Top1 |        Top2 |        Top3 |        Top4 |        Top5 |
| ---------------------- | ----------------- | -----: | ----------: | ----------: | ----------: | ----------: |
| win >=1,000 (n=403)    | current routed    | 0.744% |      0.993% |      3.474% |     11.663% |     29.777% |
|                        | odds-free Stage-1 | 0.248% |      1.737% |  **8.189%** | **16.129%** | **30.521%** |
| win >=2,000 (n=164)    | current routed    | 0.000% |      0.000% |      1.220% |      1.829% |      4.268% |
|                        | odds-free Stage-1 | 0.000% |  **0.610%** |  **5.488%** |  **9.756%** | **13.415%** |
| trifecta >=500k (n=78) | current routed    | 0.000% |      6.410% |     12.821% | **21.795%** |     29.487% |
|                        | odds-free Stage-1 | 0.000% | **11.538%** | **16.667%** |     20.513% | **33.333%** |

This is a real expert specialization, but replacing the current model globally would reduce overall 2026 Top1 from 29.826% to 26.158%.

## Concrete high-payout races

| Race                  | Win payout | Winner popularity | Trifecta payout | Current winner rank | Stage-1 winner rank |
| --------------------- | ---------: | ----------------: | --------------: | ------------------: | ------------------: |
| `jra:2025:1005:05:09` |     24,430 |                16 |      11,483,780 |                  16 |                  17 |
| `jra:2025:0830:01:06` |     10,830 |                13 |       5,395,300 |                  13 |                  13 |
| `jra:2025:0316:09:05` |     22,340 |                15 |       4,944,140 |                  15 |                  11 |
| `jra:2025:0719:03:07` |      1,140 |                 6 |       4,385,500 |                   6 |                   5 |
| `jra:2026:0131:10:06` |     38,820 |                18 |      58,367,060 |                  18 |                  18 |
| `jra:2026:0125:06:05` |     19,820 |                16 |       9,254,630 |                  15 |                  11 |
| `jra:2026:0613:09:12` |      6,970 |                11 |       2,755,710 |                  12 |                   6 |
| `jra:2026:0308:06:05` |      1,990 |                 8 |       3,075,040 |                   8 |                   7 |

The extreme `2026-01-31 Kokura R6` remains rank 18 under both experts, showing that routing alone cannot recover every tail event. Conversely, `2026-06-13 Hanshin R12` moves from rank 12 to rank 6 and is the kind of race a safe gate should target.

## Routing experiments

Two CatBoost gates were trained on 2023 only from pre-race market shape, field size, route context, current/Stage-1 confidence, margins, correlation, and top-horse disagreement.

1. High-payout probability: 2024 AUC 0.6160, average precision 0.2954 at a 0.2244 base rate.
2. Direct counterfactual utility—whether Stage-1 moves the actual winner upward by at least two ranks below rank1: 2024 AUC 0.6400, average precision 0.4492 at a 0.3437 base rate.

For each gate, 128 policies were tested: 8 probability thresholds × 4 Stage-1 weights × 4 frozen-prefix depths.

- The frozen-prefix variants preserved current Top1, Top1–2, Top1–3, or Top1–4 and reordered only the tail with Stage-1.
- Promotion gate: all-race Top1–Top5 nonnegative, ordinary-race Top1–Top5 nonnegative, and positive high-payout objective on 2024.
- Passing policies: **0**.

A second search removed the learned gate and tested fixed production-eligible cells: 10 venues, class groups, surface, distance band, field-size bands, and surface×distance, each with four weights and four frozen-prefix depths. Cells needed at least 30 high-payout and 100 ordinary races in both 2023 and 2024 and had to satisfy the same no-regression gate in both years. Passing cells: **0**.

Thus the observed expert advantage cannot yet be converted into a safe pre-race route. Selecting a route from payout itself would be leakage, and accepting regressions in ordinary races would violate the requested isolation.

## Payout-weighted specialist v10

A bounded odds-free Stage-1 ranker was then trained with payout used only as a constant race-group training weight. No payout field was included in the 235 model inputs. Multipliers 2× and 4× were trained on data strictly before 2024; 4× had the stronger 2024 high-payout capture and was retrained through 2023 for 2025 and through 2024 for the observed 2026 diagnostic.

The specialist strongly improved cumulative longshot capture:

| period                   | high-payout Top1 |     Top2 |     Top3 |     Top4 |     Top5 | worst ordinary metric |
| ------------------------ | ---------------: | -------: | -------: | -------: | -------: | --------------------: |
| 2024 selection           |         +0.516pp | +3.484pp | +7.226pp | +7.871pp | +2.839pp |             -11.273pp |
| 2025 diagnostic          |         +0.122pp | +1.834pp | +6.968pp | +8.435pp | +3.056pp |             -10.694pp |
| observed 2026 diagnostic |         -0.476pp | +1.190pp | +6.667pp | +6.905pp | +0.476pp |              -4.911pp |

Exact-position deltas were mixed rather than uniformly positive. For example, 2025 exact2 improved +0.733pp and exact4/exact5 improved +1.222/+1.345pp, but exact3 fell -1.467pp. In observed 2026, exact1/exact2/exact3 fell -0.476/-3.333/-1.190pp.

Protected global routing exhaustively tested both payout multipliers, four score weights, and current-prefix freezes at Top1, Top1–2, Top1–3, and Top1–4. A route had to keep **both exact1–5 and cumulative Top1–Top5 nonnegative** for all races and ordinary-payout races on 2024 while improving the high-payout objective. No global route passed.

Fixed-cell routing then tested venue, class, surface, distance, field size, and surface×distance with minimum 30 high-payout and 100 ordinary races. Three 2024 cells passed. The strongest was multiplier 4×, specialist weight 0.25, freeze Top1–4, dirt extended-distance: it improved high-payout exact5 +2.941pp while every other metric was unchanged.

Frozen confirmation failed:

- 2025: ordinary Top5 capture -0.935pp;
- observed 2026: all-race exact5 -2.941pp and high-payout exact5 -14.286pp.

The specialist proves that training emphasis can move longshot winners into Top3–Top5, but the selected cell does not generalize. It is therefore not deployed and is not eligible for alias/routing changes without a new gate and an untouched future confirmation window.

Artifacts: `train_high_payout_specialist.py`, `evaluate_high_payout_specialist.py`, and `high-payout-specialist-v10/` under the JRA experiment directory.

### Conservative rank-5 tail swap

通常レースへの影響をさらに限定するため、現行Top1–4を完全固定し、現行5位と6位だけを専門rankerで交換するpolicyを2024だけで選択した。推論時に払戻は使わない。固定gridはcell、specialist gap、current-score gap、tail depthで構成した。

2024で選ばれたpolicyは `class=005`、specialist z-gap `>=1.0`、current z-gap `<=0.25`、6位だけを候補とするものだった。3,443レース中26レースだけで発火し、高配当exact5 `+0.258pp`、Top5 `+0.258pp`、通常Top5 `+0.037pp`だった。

具体的な改善例:

- `jra:2024:0804:01:12`、単勝1,260円: 勝馬を6位→5位。
- `jra:2024:0811:01:12`、単勝1,490円: 勝馬を6位→5位。
- `jra:2024:1109:03:06`、単勝1,170円・3連単819,120円: 実着5位馬を予測6位→5位。
- `jra:2025:1018:05:07`、単勝1,420円: 勝馬を6位→5位。
- `jra:2025:0713:03:09`、単勝2,840円: 実着5位馬を予測6位→5位。

2025診断では全体exact5/Top5が各`+0.029pp`、高配当が各`+0.122pp`で、通常指標は非悪化だった。一方、観測済み2026では以下の回帰が発生した。

- `jra:2026:0412:06:06`、単勝1,460円: 勝馬を5位→6位。
- `jra:2026:0523:05:07`、単勝2,150円: 実着5位馬を予測5位→6位。
- 高配当exact5/Top5はいずれも`-0.238pp`、全体exact5は`-0.060pp`。

Top1–4は構造的に不変だが、Top5保護gateに失敗したためこのpolicyもREJECTとする。2025/観測済み2026を使った再選択は行わず、production routingは変更しない。

Artifact: `high_payout_tail_swap.py`、`high-payout-specialist-v10/tail-swap-report.json`。

### Two-specialist consensus tail swap

単一specialistの誤交換を減らす仮説として、払戻weight 2×と4×の両rankerが同じ6位馬を支持するときだけ5位と交換した。2024だけで固定したpolicyは、両specialist z-gap `>=0.75`、current z-gap `<=0.10`、cell制限なし、Top1–4固定である。

2024では114レースで発火し、全体exact5 `+0.174pp`、高配当exact5 `+0.517pp`、高配当Top5 `+0.129pp`、通常exact5/Top5 `+0.075pp`だった。2025診断でも全体exact5 `+0.203pp`、高配当exact5 `+0.245pp`、通常exact5 `+0.191pp`で、Top1–4とTop5 captureは非悪化だった。

ただし、効果は統計的に安定しなかった。2024高配当exact5+Top5のdate-cluster bootstrapは点推定`+0.646pp`に対してLB95 `-0.259pp`、UB95 `+1.567pp`だった。観測済み2026では全体exact5 `-0.060pp`、高配当Top5 `-0.476pp`、通常exact5 `-0.081pp`へ反転した。

具体的には2026の`jra:2026:0118:08:09`と`jra:2026:0614:02:05`で勝馬を6位→5位に改善した一方、`jra:2026:0110:06:04`、`jra:2026:0111:06:11`、`jra:2026:0215:05:02`、`jra:2026:0228:10:07`では勝馬を5位→6位へ悪化させた。合意条件だけでは誤交換を隔離できないためREJECTとし、本番へ反映しない。

Artifact: `high-payout-specialist-v10/consensus-tail-swap-report.json`。

### Long-history rough-race classifier + cell separation

荒れるレース判定を強化するため、2013–2023の37,998レースを使うrace-level classifierを新設した。払戻は `high_payout` 学習labelにだけ使用し、入力は発走前オッズ分布（favorite/top2 share、entropy、上位odds gap、longshot count）、頭数、会場、class、surface、距離、月に限定した。2024 selectionのAUCは`0.6575`、average precisionは`0.3436`で、旧classifierのAUC `0.6160`から改善した。

classifier probabilityとvenue/class/surface/distance/field-size/surface×distance cellを組み合わせ、specialist weightとTop1–4 frozen prefixを探索した。2024でcell内1pp以上かつ全体・通常race exact1–5/Top1–5非悪化を通過したrouteは4件だった。

最良route:

- cell: `class=005`
- rough probability: `>=0.27697`
- specialist weight: `0.25`
- frozen prefix: Top1–4
- routed races: 263（高配当88、通常175）
- routed high-payout Top5: `+3.409pp`
- routed high-payout exact5: `+2.273pp`
- routed ordinary exact5: `+0.571pp`
- 全体Top5/exact5: 各`+0.087pp`

これにより、cell内では要求された1pp以上の効果を2024 selectionで得た。しかし一般化gateには失敗した。2025ではrouted high-payout Top5 `+2.885pp`に対しexact5 `-2.885pp`、2026観測分ではTop5 `-4.348pp`、exact5 `-2.174pp`だった。全体でも2026 Top5 `-0.120pp`となる。

したがってclassifier自体の識別力改善とcell内3.4ppのsignalは確認できたが、production routeはREJECTする。特にhistorical `tansho_odds`と実運用時点のodds snapshot差も未解消であり、future untouched windowなしでの反映は行わない。

Artifacts: `high_payout_long_history_router.py`、`high-payout-specialist-v10/long-history-router.cbm`、`high-payout-specialist-v10/long-history-router-report.json`。

### 2026 regression root cause and year-invariant stability gate

`race_year`はclassifier入力にもrouting条件にも使用していない。年はwalk-forward splitにだけ使用する。

2026の大幅低下は単一の会場・surface・classifier probability帯では説明できなかった。`class=005` routeの高配当raceで、2024は勝馬Top5がnet `+3`、exact5がnet `+2`だったのに対し、2026は勝馬Top5がnet `-2`、exact5がnet `-1`だった。2026の悪化は会場05/06/10、turf/dirt、probability `0.277–0.335`に分散していた。つまり「荒れる確率」は高配当発生を識別しても、現行5位とspecialist候補のどちらが正しいかという**rerank方向**を識別していない。

専用swap-utility classifierも検証した。2023の2,772 changed racesで学習し、2024 AUCは`0.8775`だったがpositive rateは`3.79%`、average precisionは`0.1259`に留まった。2024 cell内Top5は`+11.111pp`だった一方、2026は`-15.385pp`で、rare-event classifierの見かけ上のAUCは一般化を保証しなかった。

未知年への安定性を選択段階で要求するため、独立OOFを追加した。2023用rough classifierは2013–2022だけ、2024用classifierは2013–2023だけで学習し、同一のyear-independent cell/threshold/weight/frozen-prefixを両foldへ適用した。次を同時要求した。

- 2023/2024両方で全体・通常raceのexact1–5/Top1–5非負
- routed high-payoutで2024 peak `>=1pp`
- routed high-payoutで2023も正

通過候補は**0**だった。したがって2024の3.4ppは少数イベントに依存したselection noiseであり、2026だけを除外するcellや年filterで修正してはならない。全既知年を後から見てcellを選び直すことも未知race精度を保証しないため行わない。

Artifacts: `high_payout_swap_utility_router.py`、`high-payout-specialist-v10/swap-utility-router-report.json`、`high-payout-specialist-v10/year-invariant-stability-report.json`。

### Interpreting +11.111pp versus -15.385pp

大きな符号反転をrace単位に分解した。2024の`+11.111pp`は27高配当race中、Top5 gain 4件・loss 1件のnet `+3`、2026の`-15.385pp`は13 race中、gain 1件・loss 3件のnet `-2`である。race bootstrap 95% intervalは2024が`[-3.704,+25.926]pp`、2026が`[-46.154,+15.385]pp`で、点推定ほど確定的な差ではない。年差の点推定は`26.496pp`だが、bootstrap 95% intervalは`[-4.843,+60.684]pp`で0を含む。

一方で、境界に改善余地があるという解釈は一部正しい。結果を知るoracleがbeneficial swapだけを選べればTop5は2024 `+14.815pp`、2026 `+7.692pp`となる。ただし2026では1 gainを残し3 lossesを捨てる発走前gateが必要であり、現行utility classifierはこれを分離できない。gainのutility probabilityは`0.066`、lossesは`0.026/0.228/0.409`で単調なthresholdが存在しない。

2026ではcurrent/specialist score correlation中央値が2024の`0.863`から`0.750`へ低下し、specialist gap中央値は`0.549`から`0.949`へ拡大した。毎年再学習されたspecialistのmeta-feature calibrationがfold間で不変ではなく、強いdisagreementが正しさではなく過信として現れている。

さらにdata-contract不備を発見した。2026 merged featureには単勝odds欠損が3,109 runners／291 racesあり、`jra:2026:0523:05:07`はrace全体のoddsが欠損したままrough classifier routeへ入ってexact5を悪化させていた。実験routeへ`odds_complete` fail-closed条件を追加した結果、2026高配当exact5は`-0.238pp`から`0.000pp`へ回復した。ただしTop5は依然`-0.476pp`であり、主要なdirection-selection問題は残る。

したがって2026の負方向は「+15ppをそのまま回収できる証拠」ではなく、「Top5境界にoracle上の+7.7pp余地はあるが、現在の発走前特徴では方向を識別できない」という証拠である。次に必要なのは年filterではなく、複数OOF foldでcalibrateしたrank-only disagreement、fold-invariant probability calibration、十分なpositive utility件数である。

Artifact: `high-payout-specialist-v10/2024-vs-2026-route-decomposition.json`。

### Signed utility / harm-aware gate

binary helpful classifierがneutralとharmfulを同じnegativeへまとめる問題を避けるため、rerank utilityを`harmful / neutral / beneficial`の3 classで直接学習し、`P(beneficial)-P(harmful)`が高い場合だけrouteする手法も検証した。入力からrough-payout probabilityを除外し、payout labelのfold leakageを避けた。Top1–4固定、odds完全性、全体・通常race保護、2024 cell内1pp以上を同時要求したが、通過routeは0だった。単純threshold、binary utility、signed utility、2-specialist consensusのいずれでもdirectionを安定識別できていない。

Artifact: `high_payout_signed_utility_router.py`、`high-payout-specialist-v10/signed-utility-router-report.json`。

### Rich horse/jockey/trainer rank-5 decision models

現行5位とspecialist候補のどちらを残すかを直接判定するため、2018–2022のwalk-forward OOFを追加した。baseline championとpayout-weighted specialistは各foldの前年までで学習し、2018–2022だけで約23.7万runnerを確保した。入力は235の発走前特徴についてincumbent値、candidate値、candidate-minus-incumbent差分を作り、馬の近走・脚質・上がり、騎手の直近/会場/距離/脚質成績、調教師成績、父/母父、馬場/コース適性、調教、負担重量、race pace/contextを含む705 pair featuresとした。

以下を比較した。

1. harmful/neutral/beneficialのrich 3-class utility classifier
2. candidateがincumbentより先着するrich pairwise classifier
3. Top5 relevance (`5,4,3,2,1`)を学ぶOOF-stacked YetiRank meta-ranker
4. winner probabilityとexact-fifth probabilityを別々に学ぶdual probability gate
5. critical rank1/rank5 sample weighting
6. 各予測年の前年まででgateを再学習するwalk-forward refit
7. class/surface/distance/field-size cell分離

独立2022 calibrationでは、sparse signed utility classifierのAUCは`0.515`、pairwise先着classifierは`0.538`だった。OOF meta-rankerが候補を選んだ場合のpairwise正答率は`58.7%`まで改善した。dual modelはwinner AUC `0.839`、exact-fifth AUC `0.633`だったため、複合データから個別確率を推定する能力自体は確認できた。

しかし同一のthreshold/cellを2023・2024へ適用し、全体・通常race exact1–5/Top1–5非負、高配当改善を要求すると、固定gate・walk-forward refitとも通過routeは0だった。winnerは比較的予測可能だが、「ちょうど5着」は不確実性が高く、Top5 gainとexact5 harmを同時に安全化できないことが残課題である。本番変更は行わない。

Artifacts: `rich_rank5_swap_gate.py`、`rich_rank5_pairwise_gate.py`、`rich_rank5_meta_ranker.py`、`rich_rank5_dual_probability_gate.py`、`rich_rank5_walkforward_dual_gate.py`、`high-payout-specialist-v10/rich-rank5-*/`。

### Direct Top5-inclusion and asymmetric decision utility

exact-fifthだけを直接予測するのではなく、全runnerを使って`P(winner)`、`P(finish<=5)`、`P(exactly fifth)`を別々に学習し、candidateとincumbentの確率差から非対称utilityを構成した。Top5 inclusion modelの独立2022 calibrationはAUC `0.810`、average precision `0.713`で、Top5に入る馬の識別自体は高精度だった。

utilityはwinner差を2倍、exact-fifth差を1倍、Top5-inclusion差を`0.25/0.5/1/2`倍として探索し、threshold、class/surface/distance/field cellを組み合わせた。さらに各年の前年まででwinner/fifth gateを再学習するwalk-forward refitも比較した。しかし2023・2024両方で全体・通常raceのexact1–5/Top1–5を非負にし、高配当を1pp以上改善するrouteは0だった。

これはTop5 inclusion確率の識別不足ではなく、予測順位5位という単一slotでwinner captureとexact-fifthを同時最適化するdecision constraintに起因する。candidateのTop5 inclusion確率が高くても、incumbentが実際に5着または勝馬であるrare harmを十分低い誤差で除外できない。

Artifact: `rich_rank5_top5_inclusion_gate.py`、`high-payout-specialist-v10/rich-rank5-top5-inclusion/`。

### Conformal abstention and expanded candidate set

2023だけをcalibrationに使い、date-cluster bootstrap LB95が正のpolicyだけを2024へ送るconformal abstentionを追加した。winner/exact-fifth/Top5 inclusion確率からutilityを作り、cellとthresholdを探索したが、最良calibration policyは`class=005`、threshold `0.1`、62 races、点推定`0.0pp`、LB95 `0.0pp`で、正のLBを持つpolicyは0だった。

specialistが提示した単一candidate自体が悪い可能性も検証した。現行rank 5–8の全馬をcandidate poolとし、rich runner probability utilityが最大の馬を5位候補にする手法をclass/surface/distance/field cellと組み合わせた。しかし2023/2024両方の保護gateを通るrouteは0だった。

したがって現時点の失敗は単一specialist candidateやthreshold選択だけには帰着せず、rank-5 slotのsmall-sample utilityを正のlower boundで識別できない点にある。

Artifacts: `rank5_conformal_abstention.py`、`rank5_candidate_set_gate.py`、`high-payout-specialist-v10/rank5-conformal-abstention/`、`high-payout-specialist-v10/rank5-candidate-set/`。

### Pareto frontier and explicit harm budgets

winner/exact-fifth/Top5 inclusion utilityについて、4 weights×11 cells×7 thresholdsの308固定policyを2023/2024で全列挙した。両foldの高配当Top5が正でなかったpolicyは305/308、all exact5 regressionは152、ordinary exact5 regressionは141、all Top5 regressionは254、ordinary Top5 regressionは150だった。支配的な問題はexact5だけではなく、高配当Top5 signal自体が両foldで同符号にならないことである。

両foldで高配当Top5が正だった最も低害なpolicyでも、minimum改善は`+0.119pp`に留まり、all exact5 `-0.058pp`、ordinary Top5 `-0.037pp`、ordinary exact5 `-0.075pp`だった。要求する1ppには達していない。

2023だけでharm budget `0/0.05/0.1pp`を設定するlearning-to-deferも実施した。budget 0で選ばれたdistance=2 policyは2023高配当Top5 `+0.119pp`だったが、2024では`0.000pp`かつall/ordinary `-0.029/-0.037pp`へ反転した。budgetを緩めたpolicyも2024高配当Top5 `-0.258pp`となり、asymmetric budgetは一般化を改善しなかった。

market-free specialistのsystematic biasを避けるため、market-aware champion 250 featuresをpayout group-weight 2×で再学習したspecialistも2023/2024 OOFで検証した。しかしweight/frozen-prefix/cellの保護gateを通るrouteは0だった。

Artifacts: `rank5_pareto_frontier.py`、`rank5_asymmetric_defer.py`、`train_market_aware_payout_specialist.py`、`rank5-pareto-frontier.json`、`rank5-asymmetric-defer.json`。

### Hierarchical shrinkage and causal online expert state

2018–2023の6 OOF foldsをpoolし、gain/neutral/lossをDirichlet posteriorで階層shrinkした。31 cells中、最も有望な`dirt+distance=2`は高配当Top5 posterior mean `+1.73pp`、positive probability `97.4%`だったが、LB95は`-0.013pp`だった。year-cluster LBはwinner Top5 `+0.311pp`だった一方、exact5は`-0.641pp`、ordinary annual minimumは`-1.667pp`で、同時gateを通過しなかった。

同cellの高配当Top5年次差分は2018–2022で`+4.55/+3.16/+1.43/+2.08/+0.82pp`と一貫していたが、2023 `-1.32pp`、2024 `+0.68pp`、2025 `+3.94pp`、2026 `-5.13pp`へ変動した。pool posteriorだけでは最新regimeの符号反転を防げない。

年filterを使わず変化へ追随するため、過去の完了race utilityだけをrolling stateにするcausal online expert gateも検証した。current payoutは入力せず、同日結果も次日以降にだけ反映し、window `100/250/500/1000`、minimum history、winner/exact harm thresholdを探索した。2021–2023の全体・通常保護と高配当改善を同時に満たすpolicyは0だった。

Artifacts: `rank5_bayesian_cell_shrinkage.py`、`rank5_online_expert_gate.py`、`rank5-bayesian-cell-shrinkage.json`、`rank5-online-expert-gate.json`。

### Independent probability calibration and richer 269 features

Balanced class weightsで歪んだraw probabilityを修正するため、2018–2021でwinner/fifth/Top5 headsを学習し、untouched 2022でisotonic calibrationを行った。さらにmarket-free 235 featuresから、騎手のseason/会場/距離、調教師class×surface×season、血統・馬場interaction、market disagreementを含むproduction 269 featuresへ拡張した。

2022 Brier scoreはwinner `0.163→0.058`、exact-fifth `0.216→0.067`、Top5 inclusion `0.179→0.168`へ改善し、異なるheadの確率scaleは大幅に整合した。calibrated utility、Top5 weights、class/surface/distance/surface×distance/field cellsを2023/2024へ固定適用したが、全体・通常race保護と高配当1ppを通るrouteは0だった。

したがってraw probability calibrationとfeature不足は実在した問題だが、それだけではrank-5 decisionのcross-fold utilityを正にできなかった。

Artifact: `rank5_calibrated_probability_gate.py`、`high-payout-specialist-v10/rank5-calibrated-probability-v269/`。

### Coherent ordinal distribution and categorical identities

独立headsの確率整合性をさらに改善するため、着順`1/2/3/4/5/6+`を単一MultiClass distributionとして学習した。2018–2021学習、2022 isotonic calibrationでmulticlass Brierは`0.668→0.495`へ改善したが、joint probability utilityでも保護routeは0だった。

集計率だけでは捉えられない騎手・調教師interactionを検証するため、PostgreSQLのrace entryから2018–2026の607,568 runner identitiesを因果的に抽出し、horse registration、jockey name、trainer nameをCatBoost categoricalとして269 rich featuresへ追加した。raw multiclass Brierは`0.668→0.586`へ改善したが、calibrated Brierは`0.501`で、2023/2024保護routeは0だった。

個体identityはraw discriminationを改善したものの、未知runner/組合せへのshrink後にrank-5 utilityの符号安定性を作れなかった。

Artifacts: `rank5_ordinal_distribution_gate.py`、`rank5_categorical_identity_gate.py`、`jra-runner-identities-2018-2026.parquet`、`high-payout-specialist-v10/rank5-ordinal-distribution-v269/`、`high-payout-specialist-v10/rank5-categorical-identity-v269/`。

### OOD abstention and analogous historical races

2026のdistribution shiftを年filterなしで避けるため、2018–2022 pair differencesをstandardizeし、PCA 32 dimensionsとIsolation Forestでfeature-densityを学習した。2023 density quantileとutility threshold、interaction cellsをcalibrateし、in-distribution pairだけをrouteする方式を検証したが、2023 calibration保護と高配当1ppを満たすpolicyは0だった。

対象raceに近い過去状態を直接検索するため、同じPCA空間で20/50/100/200 nearest historical pairのsigned utilityをdistance-weighted集約したanalog kNN gateも検証した。2018–2022だけをneighbor database、2023をcalibrationとしたが、通過policyは0だった。

したがって2026の負値を単純なfeature OODとして隔離することも、近傍過去raceからswap方向を復元することもできなかった。

Artifacts: `rank5_ood_abstention_gate.py`、`rank5_analog_knn_gate.py`、`high-payout-specialist-v10/rank5-ood-abstention-v269/`、`high-payout-specialist-v10/rank5-analog-knn-v269/`。

## Joint Group-DRO model and separate alternate candidate output

2014–2017のleakage-safe OOFを追加した。2013は学習履歴が2013開始のためfoldを作るとtrainが空になり、pre-2013 feature historyなしでは作成不可。2014–2017で11,995 changed pairs（beneficial 434、harmful 488）を追加し、従来2018–2022の14,064 changed pairsに対してboundary sampleを約85%増やした。

baseline z、payout-weighted specialist z、269 horse/jockey/trainer/pedigree/market featuresを同時入力するjoint neural rankerを実装した。race-set contextからdefer probabilityをend-to-end学習し、ListNet、Top5 utility、ordinary baseline no-regression hinge、year×payout Group-DROを同時最適化した。2014–2022の31,082 racesで20 epochs学習し、Group-DROは一貫してhigh-payout groupsへ大きいweightを割り当てた。

単一順位へ適用すると高配当Top5は2023/2024/2025/2026で`+2.26/+3.74/+3.06/+0.71pp`だったが、ordinary exact metricsが`-4.01/-3.66/-4.74/-3.30pp`まで悪化し、主順位promotion gateは失敗した。

一方、主順位Top1–5を一切変更せず、baseline 6位以下からjoint score最大の1頭を`additional Top5/upset candidate`として別出力すると、高配当勝馬の追加recallは2023/2024/2025/2026で`+18.59/+16.65/+15.40/+16.43pp`だった。主順位を変えないためexact1–5と通常順位への悪影響は構造的に0。この用途では2024 signalが他年にも一般化した。

NumPy serving forwardでも上記recallは同一だった。ただしMLX/NumPyの行列積精度差により12,027 races中raw candidate 13件がflipしたため、artifactはproduction parity未通過。candidate margin `>=0.1`へ限定すると両backend共通emitでcandidate flip 0となり、高配当勝馬追加recallは`+13.83/+12.90/+11.37/+13.10pp`、coverageは約72–74%。低marginはabstainするshadow-experimental contractとし、本番artifact登録は行わない。

production runtimeでは既存dynamic-market shadow recordへ`additional_top5_candidates`を追加した。これは`shadow_top5 - baseline_top5`をshadow順で保持し、`predicted_rank`を変更しない。DDL/upsert/migrationとtestsを更新し、finish-position suiteは2,019 passed、1 skipped、coverage 98.00%。deployは行っていない。

Artifacts: `joint_group_dro_ranker.py`、`joint_group_dro_evaluate.py`、`joint-group-dro-ranker/`、`oof-2014-2017-run/coverage-report.json`。

## Pre-race signal status

- timestamp odds: `sync-realtime-data-hot`が`fetchedAt`付きでR2保存し、Containerもexpected timestamp一致を検証済み。
- 馬体重/増減: current snapshotと既存trend featuresへ反映済み。
- 騎手変更: realtime parserで検出し、current jockey stats/identityを使用可能。
- 当日馬場: causal hourly/prior3とtrack biasが存在。未観測future weatherは使用しない。
- パドック/直前気配: `premium_paddock_bulletins`にhorse number、favorite/value group、comment/evaluation、`fetched_at`が保存される。`premium-paddock-signals.ts`でstrictly pre-start、latest snapshot、incomplete→nullのcontractを実装。履歴が短いためshadow-onlyとし、free text sentimentは使用しない。

詳細contract: `docs/probes/jra-prerace-signal-contract-2026-08-25.md`。

## Next accuracy-focused step

Do not add more weather/data fields merely because they exist. The next candidate should directly predict **counterfactual rank gain from the odds-free expert**, with special emphasis on whether it moves a potential longshot winner into ranks 2–5. It must be selected before opening a new forward period and then satisfy:

1. no Top1–Top5 regression overall or in ordinary races;
2. positive Top2–Top5 improvement in high-payout races;
3. positive race-date cluster lower bound;
4. confirmation on a future period not used in the current 2025/2026 analysis.

Artifacts: `high_payout_router_experiment.py`, `evaluation-high-payout-router-v9.json`, `jra-payouts-2023-2026.parquet`, and `jra-current-stage1-scores-2026-through-0712.parquet` under the JRA experiment directory.
