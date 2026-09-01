#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cache_dir="$app_dir/.native-cache"
prefix="$cache_dir/prefix"
payload="$cache_dir/JVPayload"
wine_version=11.16
wine_archive="$cache_dir/wine-staging-$wine_version-osx64.tar.xz"
wine_dir="$cache_dir/wine-$wine_version"
wine_root="$wine_dir/Wine Staging.app/Contents/Resources/wine"
wine_url="https://github.com/Gcenx/macOS_Wine_builds/releases/download/$wine_version/wine-staging-$wine_version-osx64.tar.xz"
wine_sha256=cd68f230c773a761b8a0423a08c51fbe49e89b6e52246f3866898d259a4988c6
python_archive="$cache_dir/python-3.13.7-embeddable-amd64.zip"
python_url=https://www.python.org/ftp/python/3.13.7/python-3.13.7-embeddable-amd64.zip
python_sha256=e201b2da753a88c1af29d87f9f48af4d64a0fc8522a204ae672bd2c382496701
pywin32_wheel="$cache_dir/pywin32-311-cp313-cp313-win_amd64.whl"
pywin32_url=https://files.pythonhosted.org/packages/e3/28/e0a1909523c6890208295a29e05c2adb2126364e289826c0a8bc7297bd5c/pywin32-311-cp313-cp313-win_amd64.whl
pywin32_sha256=718a38f7e5b058e76aee1c56ddd06908116d35147e133427e59a3983f703a20d

if [[ $(uname -s) != Darwin ]]; then
  printf 'Native setup requires macOS.\n' >&2
  exit 1
fi
if [[ $(uname -m) == arm64 ]] && ! arch -x86_64 /usr/bin/true; then
  printf 'Rosetta 2 is required: softwareupdate --install-rosetta\n' >&2
  exit 1
fi
for command in curl shasum tar unshield unzip; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf '%s is required. Install unshield with: brew install unshield\n' "$command" >&2
    exit 1
  fi
done

mkdir -p "$cache_dir"
if [[ ${JRA_VAN_NATIVE_REPAIR:-0} != 1 && -f $cache_dir/native-ready ]]; then
  printf 'Containerless JV-Link runtime is already ready: %s\n' "$prefix"
  printf 'The persistent prefix and its JV-Link authentication state were left unchanged.\n'
  exit 0
fi
if [[ ! -f "$app_dir/sdk/JVLinkSetup.exe" ]]; then
  "$app_dir/scripts/prepare-sdk.sh"
fi

if [[ ! -f "$wine_archive" ]]; then
  curl --fail --location --output "$wine_archive" "$wine_url"
fi
printf '%s  %s\n' "$wine_sha256" "$wine_archive" | shasum -a 256 -c -
if [[ ! -x "$wine_root/bin/wine" ]]; then
  rm -rf "$wine_dir"
  mkdir -p "$wine_dir"
  tar -xJf "$wine_archive" -C "$wine_dir"
  xattr -dr com.apple.quarantine "$wine_dir" 2>/dev/null || true
fi
export PATH="$wine_root/bin:$PATH"
export WINEPREFIX="$prefix"
export WINEARCH=win64
export WINEDEBUG=-all
export MVK_CONFIG_LOG_LEVEL=0
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

wineboot --init
if [[ ! -d "$payload/JV-Link" ]]; then
  rm -rf "$prefix/drive_c/JVExtract" "$payload"
  wine "$app_dir/sdk/JVLinkSetup.exe" '/extract_all:C:\JVExtract'
  wineserver -k >/dev/null 2>&1 || true
  cabinet=$(find "$prefix/drive_c/JVExtract" -type f -name data1.cab -print -quit)
  if [[ -z $cabinet ]]; then
    printf 'The official installer did not produce data1.cab.\n' >&2
    exit 1
  fi
  unshield x -d "$payload" "$cabinet" >/dev/null
fi

install_dir="$prefix/drive_c/Program Files/JRA-VAN/Data Lab"
mkdir -p "$install_dir" "$prefix/drive_c/JVData"
cp "$payload/JV-Link/JVDTLab.dll" "$install_dir/"
cp "$payload/JV-Link64Agent/JVLink64Agent.exe" "$install_dir/"
cp "$payload"/JV-LinkEnv/* "$install_dir/"
cp "$payload/Remove/msvcp120.dll" "$payload/Remove/msvcr120.dll" "$install_dir/"
wine regsvr32 /s 'C:\Program Files\JRA-VAN\Data Lab\JVDTLab.dll'

server_key='HKLM\Software\JRA-VAN Data Lab.\server_info'
wine reg add "$server_key" /v datahost /t REG_SZ /d datalab.cdn.jra-van.ne.jp /f >/dev/null
wine reg add "$server_key" /v datapath /t REG_SZ /d /datalab/ /f >/dev/null
wine reg add "$server_key" /v dataport /t REG_DWORD /d 80 /f >/dev/null
wine reg add "$server_key" /v realhost /t REG_SZ /d reallab.jra-van.ne.jp /f >/dev/null
wine reg add "$server_key" /v realpath /t REG_SZ /d /Browsing/GateServlet/ /f >/dev/null
wine reg add "$server_key" /v realport /t REG_DWORD /d 80 /f >/dev/null
wine reg add "$server_key" /v serverhost /t REG_SZ /d authlab.jra-van.ne.jp /f >/dev/null
wine reg add "$server_key" /v serverpath /t REG_SZ /d /Browsing/JVServlet/ /f >/dev/null
wine reg add "$server_key" /v serverport /t REG_DWORD /d 80 /f >/dev/null
wine reg add "$server_key" /v verupurl /t REG_SZ /d http://jra-van.jp/dlb/sft/jv_x64.html /f >/dev/null

uid_key='HKLM\Software\JRA-VAN Data Lab.\uid_pass'
add_string_if_missing() {
  local name=$1
  local value=$2
  if ! wine reg query "$uid_key" /v "$name" >/dev/null 2>&1; then
    wine reg add "$uid_key" /v "$name" /t REG_SZ /d "$value" /f >/dev/null
  fi
}
add_string_if_missing installdate "$(date +%Y%m%d%H%M%S)000"
add_string_if_missing installpath 'C:\Program Files\JRA-VAN\Data Lab'
add_string_if_missing messagekey 00000000000000
add_string_if_missing saveflag 1
add_string_if_missing savepath 'C:\JVData'
add_string_if_missing servicekey ''
add_string_if_missing ukey ''
add_string_if_missing payflag 2
wine reg add "$uid_key" /v agentport /t REG_DWORD /d 56531 /f >/dev/null

appid='{FC990E87-4D02-4C29-9367-B8D245F513F5}'
appid_key=$(printf '%s\\%s' 'HKLM\Software\Classes\AppID' "$appid")
wine reg add "$appid_key" /ve /t REG_SZ /d JVLink64Agent /f >/dev/null
wine reg add "$appid_key" /v LocalService /t REG_SZ /d JVLink64Agent /f >/dev/null
wine reg add 'HKLM\Software\Classes\AppID\JVLink64Agent.EXE' /v AppID /t REG_SZ /d "$appid" /f >/dev/null
agent_bin_path='"C:\Program Files\JRA-VAN\Data Lab\JVLink64Agent.exe" -Service'
if ! wine sc.exe query JVLink64Agent >/dev/null 2>&1; then
  wine sc.exe create JVLink64Agent binPath= "$agent_bin_path" start= demand >/dev/null
fi

font_source=${JRA_VAN_JAPANESE_FONT:-$HOME/Library/Fonts/UDEVGothic35NFLG-Regular.ttf}
font_family='UDEV Gothic 35NFLG'
if [[ ! -f $font_source ]]; then
  font_source='/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc'
  font_family='Hiragino Kaku Gothic ProN'
fi
if [[ ! -f $font_source ]]; then
  printf 'Set JRA_VAN_JAPANESE_FONT to a Japanese TrueType/OpenType font.\n' >&2
  exit 1
fi
font_filename=$(basename "$font_source")
cp "$font_source" "$prefix/drive_c/windows/Fonts/$font_filename"
fonts_key='HKLM\Software\Microsoft\Windows NT\CurrentVersion\Fonts'
substitutes_key='HKLM\Software\Microsoft\Windows NT\CurrentVersion\FontSubstitutes'
wine reg add "$fonts_key" /v 'JRA-VAN Japanese UI (TrueType)' /t REG_SZ /d "$font_filename" /f >/dev/null
for font_name in 'MS UI Gothic' 'MS Gothic' 'MS PGothic' Meiryo 'Meiryo UI' Tahoma; do
  wine reg add "$substitutes_key" /v "$font_name" /t REG_SZ /d "$font_family" /f >/dev/null
done

if [[ ! -f $python_archive ]]; then
  curl --fail --location --output "$python_archive" "$python_url"
fi
if [[ ! -f $pywin32_wheel ]]; then
  curl --fail --location --output "$pywin32_wheel" "$pywin32_url"
fi
printf '%s  %s\n' "$python_sha256" "$python_archive" | shasum -a 256 -c -
printf '%s  %s\n' "$pywin32_sha256" "$pywin32_wheel" | shasum -a 256 -c -
python_dir="$prefix/drive_c/Python313"
if [[ ! -f $python_dir/python.exe ]]; then
  rm -rf "$python_dir"
  mkdir -p "$python_dir/Lib/site-packages"
  unzip -q "$python_archive" -d "$python_dir"
  unzip -q "$pywin32_wheel" -d "$python_dir/Lib/site-packages"
  cat >"$python_dir/python313._pth" <<'EOF'
python313.zip
.
Lib\site-packages
import site
EOF
  cp "$python_dir"/Lib/site-packages/pywin32_system32/*.dll "$python_dir/"
  chmod +x "$python_dir/python.exe" "$python_dir/pythonw.exe"
fi
wine 'C:\Python313\python.exe' -c 'import win32com.client'

wineserver -k >/dev/null 2>&1 || true
export LANG=ja_JP.UTF-8
export LC_ALL=ja_JP.UTF-8
wineboot --update
wineserver -k >/dev/null 2>&1 || true
touch "$cache_dir/native-ready"
printf 'Containerless JV-Link runtime is ready: %s\n' "$prefix"
printf 'Run the first consent flow with: JRA_VAN_NATIVE_UI=1 bun run --filter jra-van-datalab-wine-demo demo:native\n'
