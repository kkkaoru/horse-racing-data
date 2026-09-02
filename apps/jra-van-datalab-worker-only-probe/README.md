# JV-Link Cloudflare Worker-native acquisition

Cloudflare Workers isolate単体で、Wine、Windows PE、COM、Containerを使わずに蓄積系JV-Dataを取得する実装です。
公式JV-Link 5.0.0の正常通信をoracleとしてwire contractを確認し、同じ論理端末の認証stateをWrangler secretsへ
移行しています。初回利用規約同意と端末登録は、引き続き公式UIで利用者本人が行います。

> 現行専用protocolはHTTP/80です。secret、response、recordをログへ出さず、公開APIには別のBearer認証を必須に
> してください。新規公式contractを設計できる場合はHTTPSと短命tokenを推奨します。

## Private Wasm compatibility core

互換処理の非公開実装はprivate repository `kkkaoru/horse-racing-data-private-core`で管理します。
このpublic repositoryにはRust sourceとWasm binaryを保存しません。`private-core.lock.json`には固定release、
version、artifact digestだけを記録します。

protected deployment workflowは署名済みreleaseを一時directoryへ取得してdigestを検証し、Wranglerの
**build-time Wasm module**としてbundleします。Cloudflare WorkersではR2 bytesのruntime compilationが
許可されないため、R2はcredential-free provenance artifactとmanifestの保存に使用します。

Workerへ渡すprivate core設定は単一のopaque secret `CORE_CONFIG_V1`です。public hostは内部形式を解釈せず、
値をログ、レスポンス、URL、Gitへ出しません。private artifactにもcredentialを埋め込みません。

```bash
# pinned private releaseを取得・検証（WasmはGit管理外）
bun run --filter jra-van-datalab-worker-only-probe core:prepare
```

## 実装済みcontract

公式JV-Link APIに対応する蓄積、速報、コース情報、動画一覧のWorker endpointを提供します。
公式公開仕様は`TYPELIB-ORACLE.md`と`src/compatibility.ts`で管理し、非公開の互換実装・認証schema・
通信解析資料はprivate repositoryで管理します。

## API

```text
GET  /health
GET  /compatibility
POST /acquire
POST /acquire/stream
POST /realtime
POST /course
POST /movies
```

`/acquire`と`/acquire/stream`は`JRA_VAN_WORKER_API_TOKEN`によるBearer認証が必須です。
`/acquire`は後方互換用のJSON responseです。`/acquire/stream`はfile-listの全`FN/FS`を順番に取得し、
全CP932 recordをlossless base64のNDJSONとしてstreamします。`open`、`file`、`record`、`close` eventにより、
JVOpen/JVGetsのfile境界とrecord順を維持します。1ファイルは20 MiBまでですが、全ファイルを同時bufferしません。

```bash
set -a
source apps/jra-van-datalab-worker-only-probe/.dev.vars
set +a
umask 077
curl --fail --silent --show-error \
  -H "Authorization: Bearer $JRA_VAN_WORKER_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"dataSpec":"RACE","from":"20260829000000","to":"20260830235959"}' \
  --output /tmp/jv-worker-native-response \
  https://jra-van-datalab-worker-only-probe.kaoru.workers.dev/acquire/stream
chmod 600 /tmp/jv-worker-native-response
```

必要なWorker secrets:

- `CORE_CONFIG_V1`: private provisioning toolが生成するopaque core設定
- `JRA_VAN_WORKER_API_TOKEN`: このWorker自身を保護するランダムtoken

値をURL、ログ、レスポンス、Gitへ含めないでください。`.dev.vars`はGit ignoreされ、mode `600`で管理します。

## 互換性の保証範囲

`src/compatibility.ts`はJV-Link 5.0.0 type libraryの27 methods、9 properties、7 eventsをDISPID込みで省略なく列挙します。
公式4.9.0.1文書のmethod一覧にない`JVSetPayFlag`も、5.0.0 runtime type libraryから検出して対象に含めています。
`src/compatibility.test.ts`が公式一覧との差分をCI failureにします。`/compatibility`でも現在の実装状態を確認できます。

蓄積系data planeでは、複数dataspec、全file-list entry、全file、全CRLF record、file順、record順を実装・unit test済みです。
`JVRTOpen`、`JVCourseFile*`、`JVMVOpen/JVMVRead`も公式SDK captureとWorker E2Eの全byte一致まで
実装済みです。一方、勝負服画像、動画公開check、event通知はoracle-backed protocolのpositive vectorが
不足しているため、現在`protocol-research-required`です。
`JVSetUIProperties`、`ParentHWnd`、browserを起動する`JVMVPlay*`は
Workersにdesktop UIがないため、文字通りのCOM互換ではなくWorker向けcontractへの置換が必要です。
`fullCompatibility`はこれらが解消されるまで必ず`false`であり、未実装状態を完全互換と表示しません。

## Private-core deployment gate

通常の`deploy`はWranglerを直接起動せず、必ず次の検証を完了させます。

```text
pinned private artifact取得・digest検証 → format:check → lint → tsc
→ TypeScript unit coverage → deployable compatibility ledger
→ credential-free Wasm+manifest R2 upload → wrangler deploy → production E2E
```

```bash
# ローカル検証（Cloudflareへ変更を送らない）
bun run --filter jra-van-datalab-worker-only-probe verify:local

# 検証成功時だけdeployし、最後にremote E2Eも実行
bun run --filter jra-van-datalab-worker-only-probe deploy:verified
```

`test:compatibility:local`はWorkerが公開する18 JV-Link methodsに`implemented`または
`worker-equivalent`以外が混入した場合、deploy前に失敗します。未解明methodはWorker APIへ公開せず、
`/compatibility`で明示します。

## 検証結果

2026-09-02に以下を確認しました。

1. 公式COM取得結果とprivate coreの蓄積系responseが完全一致した。
2. unit testで2ファイルと異なるrecord prefixを使用し、全file/recordの順序とbyte保存を確認した。
3. Cloudflare Workers isolateへdeployし、`/acquire/stream`がHTTP 200を返した。
4. production E2Eで7,371-byteの公式COM cacheとWorker downloadを比較した。
5. 展開後40,960 bytes、512 records全体がbyte-for-byte一致した。
6. official `JVRTOpen("0B14", "20260830")`をcaptureし、Worker-native realtime 22 records、
   1,240 bytes全体とのbyte-for-byte一致を確認した。
7. official `JVCourseFile2`の6,994-byte responseと5,414-byte GIFをWorker `/course`と比較し、
   explanationと画像全体のbyte-for-byte一致を確認した。
8. 公式開発用software IDで`JVMVOpen("11", "20260830")`を実行し、Worker `/movies`の101 keysが
   official responseと全件一致することを確認した。
9. 未認証stream requestがHTTP 401になることをE2Eで確認した。

再実行:

```bash
bun run --filter jra-van-datalab-worker-only-probe test:coverage
bun run --filter jra-van-datalab-worker-only-probe test:e2e
```

非公開通信から得た調査根拠と認証互換資料はprivate core repositoryで管理します。

Container/Wineによる公式COM互換版は`apps/jra-van-datalab-cloudflare-demo`です。WineはJRA-VAN公式サポート外です。
