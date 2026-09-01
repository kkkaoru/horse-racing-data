# JRA-VAN Data Lab. Wine demo

Windows OS、Parallels Desktop、コンテナを使わず、Apple Silicon macOS 上の Wine から
JRA-VAN Data Lab. の公式 JV-Link COM SDK を呼び出すデモです。

JV-Link 自体は Windows 専用です。本デモはWine Staging上の250KBの64-bitネイティブクライアントから、
公開 COM API (`JVInit`, `JVSetServiceKey`, `JVOpen`, `JVStatus`, `JVGets`) のみを呼び出します。
Windows Python/pywin32実装もセットアップおよびフォールバック用にソースを保持しています。

> [!IMPORTANT]
> JRA-VAN の公式サポート対象は Windows です。Wine 上の動作は非公式です。将来のSDK・Wine・認証方式での
> 動作は保証されません。通信プロトコルや認証処理は再実装していません。

## 検証済み環境

- Apple Silicon macOS
- Rosetta 2
- Wine Staging 11.16 (Gcenx macOS build、SHA-256検証)
- JRA-VAN Data Lab. SDK 5.0.0 64-bit
- MinGW-w64 14でビルドしたネイティブCOMクライアント
- Windows Python 3.13.7 / pywin32 311（セットアップ時のフォールバック）

2026年9月1日にコンテナなしで実キーを使い、次を確認しました。

```text
JV-Link OK: files=1, downloads=0, records=10, last=20260829112816
bytes=980 records=10 types=['JG']
cache=JGDW2026083020260829112816.jvd bytes=7371
```

`downloads=0` は直前の初回実行で取得したJV-Dataキャッシュを再利用した結果です。

## 必要なもの

- Apple Silicon macOSとRosetta 2
- Homebrewの `unshield`: `brew install unshield`
- 最小ネイティブクライアントのビルド用 `mingw-w64`: `brew install mingw-w64`
- `curl`, `shasum`, `tar`, `bsdtar`（macOS標準）
- 環境変数 `JRA_VAN_DATALAB_KEY`
  - ハイフンなし17桁、または17桁をハイフンで区切った表示形式
- SDK内の次のファイル
  - `temp/JRA-VAN_Data_Lab_SDK_Ver5_0_0_64bit/JV-Link/JV-Link.exe`

SDK、Wine prefix、ダウンロードデータ、利用キーはGit管理しません。利用キーはスクリプトやログへ保存しませんが、
JV-Link自身の仕様により、初回登録後はGit-ignoredな `.native-cache/prefix` のWindowsレジストリへ保存されます。
このprefixは本デモ専用として扱ってください。

## コンテナなしでセットアップ

リポジトリルートから実行します。

```bash
bun run --filter jra-van-datalab-wine-demo prepare-sdk
bun run --filter jra-van-datalab-wine-demo setup:native
bun run --filter jra-van-datalab-wine-demo build:native-client
```

`setup:native` は次を自動実行します。

1. Wine Staging 11.16、Windows Python 3.13.7 embeddable ZIP、pywin32 wheelをSHA-256検証付きで取得
2. 専用Wine prefixを `.native-cache/prefix` に作成（クラッシュする32-bit Python installerは使用しない）
3. 公式InstallShieldペイロードを展開
4. JV-Link COM DLLと `JVLink64Agent` サービスを登録
5. SDK 5.0.0の公式 `server_info` / `uid_pass` 初期値を作成
6. Windows Pythonとpywin32を導入
7. Windows localeを日本語 (`ja-JP`, CP932) に設定
8. 日本語フォントを登録

`build:native-client` は `native/jvlink_demo.cpp` を静的リンクされた64-bit PEへ変換します。実行時にMinGWは
不要です。`demo:native` はネイティブPEがあれば優先し、なければWindows Python版へフォールバックします。

既定では `~/Library/Fonts/UDEVGothic35NFLG-Regular.ttf`、なければmacOSのヒラギノ角ゴシックを使います。
別の日本語フォントを使う場合はセットアップ前に指定できます。

```bash
JRA_VAN_JAPANESE_FONT=/path/to/japanese-font.ttf \
  bun run --filter jra-van-datalab-wine-demo setup:native
```

## 初回の利用規約同意

利用規約への同意は自動化しません。初回だけ日本語設定済みWineデスクトップを表示します。

```bash
set -a
source .env
set +a
JRA_VAN_NATIVE_UI=1 \
  bun run --filter jra-van-datalab-wine-demo demo:native -- \
  --data-spec RACE \
  --from-time 20260829000000-20260830235959 \
  --limit 10
```

ダイアログの日本語を読み、本人操作で同意してください。文字化けする場合は操作せず、`setup:native` を再実行します。
同意後はWineデスクトップを閉じ、通常実行へ進みます。

## 通常実行

```bash
set -a
source .env
set +a
bun run --filter jra-van-datalab-wine-demo demo:native -- \
  --data-spec RACE \
  --from-time 20260829000000-20260830235959 \
  --limit 10 \
  --timeout 600
```

結果は `data/native-records.txt` にUTF-8で保存されます。JV-Linkの元データは
`.native-cache/prefix/drive_c/JVData/` に保存されます。

正常時の例:

```text
JV-Link OK: files=1, downloads=0, records=10, last=20260829112816
Output: Z:\...\apps\jra-van-datalab-wine-demo\data\native-records.txt
```

主なエラー:

- `-101`: 利用キー登録済み。本デモでは専用prefixの正常な再実行として扱います
- `-301`: 利用キーの認証失敗
- `-305`: 利用規約への同意が必要
- `-413`: JV-Linkサーバーから想定外のHTTP応答

## 端末認証状態とprefixの保護

JV-Linkは初回認証時に、利用キーに加えて端末識別状態を専用prefixへ保存します。この状態は別prefixへ移植・
再生成せず、同じ `.native-cache/prefix` を継続利用します。`native-ready` が存在する場合、`setup:native` は
認証状態を変更せず即終了します。

> [!WARNING]
> 利用キー登録後に `.native-cache/prefix` を削除・初期化すると、同じキーは「別のパソコン」と判定され、
> JRA-VANで利用キーの再発行が必要です。通常運用ではprefixを削除しないでください。

未認証の壊れたセットアップを修復する場合だけ、キー再発行済みであることを確認してprefixを再作成します。
端末制限の回避や認証トークンの移植は本デモの対象外です。

## ローカル復旧スナップショットと最小構成

このMacにはGit管理外の `.native-recovery/`（権限 `700`）に次のAPFS Copy-on-Writeスナップショットがあります。
いずれも利用キーと端末識別状態を含む機密データなので、共有・commit・クラウド同期は禁止です。

- `native-cache-authenticated`: 完全Wine、認証済みprefix、Python/UIを保持する完全復旧点（物理約519MB）
- `native-cache-headless-83m`: ネイティブCOMクライアント版の最小復旧点（論理約83MB、物理約34MB）

現在の `.native-cache` は論理約83MB、実行後の物理使用量約38MBです。Wine Stagingの64-bitランタイム61モジュール、API Set schema、NLS一式、
FreeTypeの推移依存、250KBのネイティブCOMクライアント、JV-Link COM DLL/Agent、registryだけを保持します。
内容が同一だった`system32` 54ファイルはWineランタイムへの相対シンボリックリンクです。Gecko、Mono、32-bit
Wine、汎用bundled dylib、Windows Python/pywin32、JV-Link設定GUI、SDK展開物などは含みません。

最小構成を復元する場合（このappディレクトリで実行）:

```bash
wine_root='.native-cache/wine-11.16/Wine Staging.app/Contents/Resources/wine'
WINEPREFIX="$PWD/.native-cache/prefix" "$wine_root/bin/wineserver" -k || true
rm -rf .native-cache
ditto --preserveHFSCompression --noclone --noqtn \
  .native-recovery/native-cache-headless-83m .native-cache
```

Pythonフォールバックと日本語UIを含む完全構成へ戻す場合は、コピー元を `native-cache-authenticated` に変更します。
両復旧点ともAPFS透過圧縮済みです。`cp`ではなく上記`ditto --preserveHFSCompression`を使うと物理使用量を
維持できます。復元後は `demo:native` を直接実行して確認し、認証状態確認前にprefixを再初期化しないでください。

削減試験では2.9GBから83MBまでの各採用段階で同じ10件の実データ取得を確認しました。129MB構成も取得には
成功しましたがFreeType警告が出るため、依存を戻した130MB構成を中間復旧点にしています。156MB/186MBのNLS
削減構成、294MBの`system32`全削除構成、API Set schemaなしの191MB候補はAgent/Wine/Python起動に失敗したため
不採用です。Python/pywin32を250KBの静的リンク済みネイティブPEへ置換し、96MBから83MBへ削減しました。

## AMD64 / ARM64コンテナ版

JV-Link COM/Agentはx86-64専用で、ARM64 Linux image内ではネイティブ実行できません。また認証済みprefixを
imageや別volumeへ複製するとJRA-VANの端末制限に触れます。このためコンテナは最小のHTTPクライアントとし、
一時Bearer tokenを使って同じMac上の認証済みランタイムへ取得要求を転送します。利用キーと`ukey`はimageや
コンテナへ渡しません。ブリッジはJV-Linkの同時実行をlockで直列化し、query/tokenをaccess logへ出しません。

各ネイティブColima contextでAlpine 3.22 base imageをビルドします。

```bash
bun run --filter jra-van-datalab-wine-demo build:container:amd64
bun run --filter jra-van-datalab-wine-demo build:container:arm64
```

別ターミナルで、`.env`のキーを持つ一時ホストブリッジを起動します。

```bash
set -a
source .env
set +a
bun run --filter jra-van-datalab-wine-demo bridge:host
```

コンテナから取得します。

```bash
bun run --filter jra-van-datalab-wine-demo demo:container:amd64 -- \
  --data-spec RACE --from-time 20260829000000-20260830235959 --limit 10 --timeout 90
bun run --filter jra-van-datalab-wine-demo demo:container:arm64 -- \
  --data-spec RACE --from-time 20260829000000-20260830235959 --limit 10 --timeout 90
```

出力はそれぞれ`data/container-amd64/records.txt`と`data/container-arm64/records.txt`です。検証済みimageは
AMD64 6,445,524 bytes、ARM64 6,779,345 bytesで、metadataとコンテナ内`uname -m`の両方を確認しています。
両imageから取得した10件・980 bytesのUTF-8 `JG`レコードはbyte-for-byte一致しました。

従来のx86-64 Wine内包imageも`Dockerfile`と`build`/`demo`/`setup:macos`として残していますが、認証済み
最小ランタイムを安全に共用できるのは上記ブリッジ版です。

## テスト

単体テストはWineを起動せず、引数検証、ハイフン付きキーの正規化、COM戻り値、登録済みキー、
ダウンロード待機、CP932からUTF-8への変換、エラー時のクローズを確認します。

```bash
bun run --filter jra-van-datalab-wine-demo format:check
bun run --filter jra-van-datalab-wine-demo lint
bun run --filter jra-van-datalab-wine-demo test
shellcheck apps/jra-van-datalab-wine-demo/scripts/*.sh
```
