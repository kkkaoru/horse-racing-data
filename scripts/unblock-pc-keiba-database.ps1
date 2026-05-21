# =============================================================================
# unblock-pc-keiba-database.ps1
# =============================================================================
# PC-KEIBA Database (ClickOnce) のインストール済みファイルから
# Zone.Identifier (MOTW: Mark of the Web) を除去し、ブロック状態を解除します。
#
# 背景:
#   ClickOnce アプリは初回起動時に Web からダウンロードされ、Windows により
#   Zone.Identifier が付与されてブロックされることがある。これにより
#   `Com.Pckeiba.Database.exe` が SmartScreen 等で起動不能になるケースがあるため、
#   本スクリプトで全関連ファイルの Unblock-File を一括実行する。
#
# -----------------------------------------------------------------------------
# 各 Windows 端末での実行手順 (推奨: GitHub からの直接取得→実行)
# -----------------------------------------------------------------------------
# 本スクリプトは公開リポジトリ kkkaoru/horse-racing-data に置かれているため、
# Windows 側から GitHub raw URL で直接ダウンロードして実行できる。
#
# 1. PC-KEIBA Database をスタートメニューから一度起動し、インストール
#    (ClickOnce 配置) を完了させておく。 ※起動失敗していても OK
#
# 2. PowerShell を起動 (管理者推奨。通常権限でも動作)。
#
# 3. 以下のワンライナーをコピペして実行:
#
#    $url = 'https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/scripts/unblock-pc-keiba-database.ps1'
#    $dst = Join-Path $env:TEMP 'unblock-pc-keiba-database.ps1'
#    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $dst
#    Unblock-File -LiteralPath $dst
#    powershell -ExecutionPolicy Bypass -File $dst
#
#    PowerShell 7 (pwsh) の場合は最終行の `powershell` を `pwsh` に置換。
#
# 4. 出力に "Unblocked: <件数>" が表示されれば完了。
#    PC-KEIBA Database を再起動して動作確認。
#
# -----------------------------------------------------------------------------
# 別解: ローカルにコピーしてから実行する
# -----------------------------------------------------------------------------
# Parallels 共有フォルダ / USB / scp 等で本ファイルを Windows にコピーした後:
#   PS> powershell -ExecutionPolicy Bypass -File .\unblock-pc-keiba-database.ps1
#
# Parallels Desktop の VM に macOS 側から一括投入する例:
#   # VM 名は `prlctl list -a` で確認
#   cp scripts/unblock-pc-keiba-database.ps1 ~/Desktop/
#   prlctl exec "<VM 名>" powershell -ExecutionPolicy Bypass -File `
#     "C:\Mac\Home\Desktop\unblock-pc-keiba-database.ps1"
#
# =============================================================================

[CmdletBinding()]
param(
    # 追加で走査したいパス (既定の AppData 以外にインストールしている場合に指定)
    [string[]]$AdditionalPaths = @()
)

$ErrorActionPreference = 'Stop'

# ClickOnce の実体 + ユーザ設定ディレクトリ
$defaultPaths = @(
    (Join-Path $env:LOCALAPPDATA 'Apps\2.0'),
    (Join-Path $env:APPDATA     'PC-KEIBA Database')
)

$targetPaths = @($defaultPaths + $AdditionalPaths) |
    Where-Object { Test-Path -LiteralPath $_ }

if (-not $targetPaths) {
    Write-Warning 'PC-KEIBA Database のインストールパスが見つかりませんでした。'
    Write-Warning '一度 PC-KEIBA Database を起動 (ClickOnce 配置) してから再実行してください。'
    exit 1
}

Write-Host "走査対象:"
$targetPaths | ForEach-Object { Write-Host "  - $_" }

# Zone.Identifier 付きファイルを抽出
$blocked = Get-ChildItem -Path $targetPaths -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { Get-Item -LiteralPath $_.FullName -Stream Zone.Identifier -ErrorAction SilentlyContinue }

$count = ($blocked | Measure-Object).Count
Write-Host "ブロック中ファイル: $count 件"

if ($count -eq 0) {
    Write-Host 'ブロック対象なし。終了します。'
    exit 0
}

$blocked | ForEach-Object {
    Write-Host "  Unblock -> $($_.FullName)"
    Unblock-File -LiteralPath $_.FullName
}

# 検証
$remaining = Get-ChildItem -Path $targetPaths -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { Get-Item -LiteralPath $_.FullName -Stream Zone.Identifier -ErrorAction SilentlyContinue } |
    Measure-Object | Select-Object -ExpandProperty Count

Write-Host ""
Write-Host "Unblocked: $count"
Write-Host "Remaining blocked: $remaining"

if ($remaining -gt 0) {
    Write-Warning '一部ファイルのブロックが残っています。管理者権限で再実行してください。'
    exit 2
}
