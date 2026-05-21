# =============================================================================
# install-pc-keiba-auto-update.ps1
# =============================================================================
# pc-keiba-auto-update.py を Windows にセットアップし、Task Scheduler に登録する。
# 何度実行しても同一状態に収束する (冪等)。
#
# 動作:
#   1. Python 3.12 が無ければ winget で user スコープにインストール
#   2. pywinauto / psutil を pip でインストール (既存なら upgrade)
#   3. pc-keiba-auto-update.py を %LOCALAPPDATA%\pc-keiba-auto-update\ に配置
#   4. ScheduledTask "PC-KEIBA Auto Update" を作成 / 更新
#
# -----------------------------------------------------------------------------
# 各 Windows 端末での実行手順 (推奨: GitHub からの直接取得→実行)
# -----------------------------------------------------------------------------
# 1. PC-KEIBA Database を一度起動し ClickOnce 配置を完了させておく
#    (まだなら unblock-pc-keiba-database.ps1 を先に実行)
#
# 2. PowerShell を「管理者として実行」で起動 (winget user-scope 用)
#    ※ Task Scheduler 登録自体は通常権限でも可
#
# 3. 以下のワンライナー (1 行) をコピペして実行:
#
#    $u='https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/scripts/install-pc-keiba-auto-update.ps1'; $d=Join-Path $env:TEMP 'install-pc-keiba-auto-update.ps1'; Invoke-WebRequest -UseBasicParsing -Uri $u -OutFile $d; Unblock-File -LiteralPath $d; powershell -ExecutionPolicy Bypass -File $d
#
#    引数で実行時刻 / 頻度をカスタマイズ可能:
#    powershell -ExecutionPolicy Bypass -File $d -DailyAt 03:00 -WaitForCompletion
#
# 4. 完了後、以下で動作確認:
#    Get-ScheduledTask -TaskName 'PC-KEIBA Auto Update'
#    Start-ScheduledTask  -TaskName 'PC-KEIBA Auto Update'   # 手動キック
#
# -----------------------------------------------------------------------------
# Parallels Desktop の VM に macOS 側から投入する例
# -----------------------------------------------------------------------------
#   prlctl exec "<VM 名>" --current-user powershell -ExecutionPolicy Bypass `
#     -Command "iwr -UseBasicParsing https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/scripts/install-pc-keiba-auto-update.ps1 -OutFile $env:TEMP\install.ps1; Unblock-File $env:TEMP\install.ps1; & $env:TEMP\install.ps1"
#
# -----------------------------------------------------------------------------
# アンインストール
# -----------------------------------------------------------------------------
#   Unregister-ScheduledTask -TaskName 'PC-KEIBA Auto Update' -Confirm:$false
#   Remove-Item -Recurse "$env:LOCALAPPDATA\pc-keiba-auto-update"
#
# =============================================================================

[CmdletBinding()]
param(
    # スケジューラ起動時刻 (HH:mm)
    [string]$DailyAt = '04:00',

    # タスク名
    [string]$TaskName = 'PC-KEIBA Auto Update',

    # スクリプト取得元 (raw URL)
    [string]$ScriptUrl = 'https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/scripts/pc-keiba-auto-update.py',

    # 設置先
    [string]$InstallDir = (Join-Path $env:LOCALAPPDATA 'pc-keiba-auto-update'),

    # Python のメジャーマイナー
    [string]$PythonVersion = '3.12',

    # py.exe に渡すバージョン指定
    [string]$PyVersionSpec = '-3.12',

    # 完了まで待機するか (--wait を Python スクリプトに付加)
    [switch]$WaitForCompletion,

    # 完了後にアプリを閉じるか (--close-when-done を付加)
    [switch]$CloseWhenDone,

    # --wait の最大分数
    [int]$WaitMinutes = 180
)

$ErrorActionPreference = 'Stop'

function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "    $msg" -ForegroundColor Green }
function Write-Warn2($msg){ Write-Host "    $msg" -ForegroundColor Yellow }

# -----------------------------------------------------------------------------
# 1. Python インストール
# -----------------------------------------------------------------------------
Write-Step "Python $PythonVersion を確認"
$pyExe = $null
$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($pyLauncher) {
    $listed = & py $PyVersionSpec -V 2>$null
    if ($LASTEXITCODE -eq 0) {
        $pyExe = "py $PyVersionSpec"
        Write-Ok "既存: $listed (via py $PyVersionSpec)"
    }
}
if (-not $pyExe) {
    # 直接探索
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python$($PythonVersion.Replace('.',''))\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python$($PythonVersion.Replace('.',''))-arm64\python.exe"),
        (Join-Path ${env:ProgramFiles} "Python$($PythonVersion.Replace('.',''))\python.exe")
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { $pyExe = "`"$c`""; Write-Ok "既存: $c"; break }
    }
}
if (-not $pyExe) {
    Write-Warn2 "Python 未検出 — winget でインストール"
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw "winget が見つかりません。手動で Python $PythonVersion をインストールしてください。"
    }
    & winget install --id "Python.Python.$PythonVersion" --accept-source-agreements --accept-package-agreements --silent --scope user
    if ($LASTEXITCODE -ne 0) { throw "winget Python インストール失敗" }
    # 直後は PATH 反映前なので絶対パスで再探索
    foreach ($c in @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python$($PythonVersion.Replace('.',''))-arm64\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python$($PythonVersion.Replace('.',''))\python.exe")
    )) {
        if (Test-Path -LiteralPath $c) { $pyExe = "`"$c`""; break }
    }
    if (-not $pyExe) { throw "Python インストール後も実行ファイルが見つかりません" }
    Write-Ok "インストール完了: $pyExe"
}

# -----------------------------------------------------------------------------
# 2. pip パッケージ
# -----------------------------------------------------------------------------
Write-Step "pywinauto / psutil をインストール"
$pipCmd = "$pyExe -m pip install --upgrade --quiet pywinauto psutil"
Write-Host "    > $pipCmd"
Invoke-Expression $pipCmd
if ($LASTEXITCODE -ne 0) { throw "pip インストール失敗" }
Write-Ok "依存インストール完了"

# -----------------------------------------------------------------------------
# 3. スクリプト配置
# -----------------------------------------------------------------------------
Write-Step "スクリプトを $InstallDir に配置"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$scriptPath = Join-Path $InstallDir 'pc-keiba-auto-update.py'
Invoke-WebRequest -UseBasicParsing -Uri $ScriptUrl -OutFile $scriptPath
Unblock-File -LiteralPath $scriptPath
Write-Ok "$scriptPath"

# -----------------------------------------------------------------------------
# 4. Task Scheduler 登録 (冪等: 同名タスクがあれば置換)
# -----------------------------------------------------------------------------
Write-Step "Task Scheduler に '$TaskName' を登録"

$pyArgs = @("`"$scriptPath`"")
if ($WaitForCompletion) {
    $pyArgs += '--wait'
    $pyArgs += '--wait-minutes'
    $pyArgs += "$WaitMinutes"
}
if ($CloseWhenDone) { $pyArgs += '--close-when-done' }
$pyArgsStr = $pyArgs -join ' '

# py.exe / python.exe どちらでも対応
if ($pyExe -like 'py *') {
    # "py -3.12" を Execute / Argument に分解
    $taskExe = (Get-Command py).Source
    $taskArgs = "$PyVersionSpec $pyArgsStr"
} else {
    $taskExe = $pyExe.Trim('"')
    $taskArgs = $pyArgsStr
}

$action = New-ScheduledTaskAction -Execute $taskExe -Argument $taskArgs -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -Daily -At $DailyAt
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6) `
    -MultipleInstances IgnoreNew

# 対話セッションが必要 (UI Automation のため)
$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description 'PC-KEIBA Database 通常データ登録を自動実行' `
    -Force | Out-Null

Write-Ok "登録完了 (daily $DailyAt)"
Write-Host ""
Write-Host "確認:"
Write-Host "  Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName'    # 手動キック"
Write-Host ""
Write-Host "ログ: $env:LOCALAPPDATA\pc-keiba-auto-update\logs\"
