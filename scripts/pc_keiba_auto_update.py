r"""
pc-keiba-auto-update.py
=======================
PC-KEIBA Database の「データ → 通常データ登録 → 開始」を pywinauto / UI Automation で
自動化するスクリプト。Windows Task Scheduler から起動する想定。

冪等性 (idempotency):
  - %TEMP%\pc-keiba-auto-update.lock で多重起動を防止 (PID + 生存チェック付き)。
  - PC-KEIBA Database プロセスが既に起動していれば再起動せずアタッチ。
  - 通常データ登録 ダイアログが既に開いている場合はメニュー操作をスキップ。
  - StartButton が disabled (= 既に更新進行中) の場合は何もせず正常終了。

安全装置 (アプリ強制終了の防止):
  - 本スクリプトはどの実行パスでも、更新処理中のアプリを終了させない。
  - --close-when-done が指定されていても、完了検出 (StatusLabel='完了しました' かつ
    処理件数=最大件数、または StartButton が disabled→enabled) を取れていなければ
    クローズしない。
  - StartButton 不在やクリック可能な CloseButton だけでは完了とみなさない。
  - JRA Data Lab. の期限切れ/再購入要求を検出した場合は成功扱いにしない。
  - --wait タイムアウトは成功扱いにしない (非 0 exit)。
  - また close 直前に必ず is_update_in_progress() で再確認する。

ログ:
  %LOCALAPPDATA%\pc-keiba-auto-update\logs\YYYYMMDD_HHMMSS.log
  古いログは 30 日で自動削除 (件数ではなく日数ベース)。

依存: pywinauto, psutil   (install-pc-keiba-auto-update.ps1 が pip でインストールする)

使い方:
  py -3.12 pc-keiba-auto-update.py                 # 起動 + 開始押下 (即時 exit, 完了非待機)
  py -3.12 pc-keiba-auto-update.py --wait          # 完了まで待機 (最大 --wait-minutes)
  py -3.12 pc-keiba-auto-update.py --close-when-done  # 完了後アプリを閉じる
  py -3.12 pc-keiba-auto-update.py --wait --shutdown-when-done  # 完了後Windowsを停止
  py -3.12 pc-keiba-auto-update.py --dry-run       # 開始ボタンを押さず終了 (検証用)
"""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import psutil
from pywinauto import Application, Desktop
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.timings import TimeoutError as PwaTimeoutError

if TYPE_CHECKING:
    # 型チェック時のみ stubs/pywinauto/__init__.pyi から Protocol を import。
    from pywinauto import UiElement, UiWindow
else:
    # runtime の実 pywinauto には UiElement/UiWindow symbol は存在しないが、
    # `from __future__ import annotations` で type annotation は str 化される。
    # ランタイム NameError や一部 linter の "undefined name" 警告を避けるため、
    # 等価な runtime sentinel として object を割り当てる。isinstance チェックは行わない。
    UiElement = UiWindow = object


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
APP_PROCESS_NAME = "Com.Pckeiba.Database.exe"
APP_WINDOW_TITLE_RE = r"^PC-KEIBA Database$"
CLICKONCE_INSTALL_ROOT = Path(os.environ["LOCALAPPDATA"]) / "Apps" / "2.0"
APPREF_PATH = (
    Path(os.environ["APPDATA"])
    / "Microsoft"
    / "Windows"
    / "Start Menu"
    / "Programs"
    / "pc-keiba.com"
    / "PC-KEIBA Database.appref-ms"
)
DATA_MENU_TITLE_RE = r"^データ\(D\)$"
NORMAL_REG_MENU_TITLE = "通常データ登録"
START_BUTTON_AUTO_ID = "StartButton"
CANCEL_BUTTON_TITLE = "中止"

# 更新中に表示される進捗ダイアログ (top-level window として detach される)
# - 主タイトル: 「通常データ登録」
# - 含む要素: ProgressBar, CloseButton (auto_id)
PROGRESS_WINDOW_TITLE = "通常データ登録"
PROGRESS_CLOSE_BUTTON_AUTO_ID = "CloseButton"
PROGRESS_FATAL_MESSAGES: tuple[str, ...] = (
    "Data Lab.サービスの有効期限が切れています。",
    "サービスの再購入が必要です。",
)

# 完了/確認ダイアログを閉じるための既定ボタンラベル候補。
DEFAULT_DISMISS_LABELS: tuple[str, ...] = ("OK", "はい", "閉じる")

# prlctl exec / Windows Terminal leave consoles in front of the registration
# dialog. Start click and progress detection then miss the real UI.
COVERING_WINDOW_TITLE_MARKERS: tuple[str, ...] = (
    "python.exe",
    "Parallels Tools",
    "prl_tools_service",
)
COVERING_PROCESS_NAMES: frozenset[str] = frozenset(
    {"WindowsTerminal.exe", "WindowsTerminal"}
)

LOCK_FILE = Path(os.environ["TEMP"]) / "pc-keiba-auto-update.lock"
LOG_DIR = Path(os.environ["LOCALAPPDATA"]) / "pc-keiba-auto-update" / "logs"
LOG_RETENTION_DAYS = 30

CONNECT_RETRIES = 3
CONNECT_TIMEOUT_SEC = 60
CONNECT_BACKOFF_SEC = 5
SW_MINIMIZE = 6
START_CLICK_RETRIES = 3
START_CLICK_SETTLE_SEC = 2
WINDOWS_SHUTDOWN_DELAY_SEC: int = 5
WINDOWS_SHUTDOWN_TIMEOUT_SEC: int = 30


# ---------------------------------------------------------------------------
# ログ
# ---------------------------------------------------------------------------
def hide_own_console() -> None:
    """Minimize this process console so it cannot cover the registration UI."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
    except ImportError:
        return
    hwnd = ctypes.windll.kernel32.GetConsoleWindow()
    if not hwnd:
        return
    ctypes.windll.user32.ShowWindow(hwnd, SW_MINIMIZE)


def _enable_utf8_stdout() -> None:
    """Windows コンソール CP932 → UTF-8 化 (TextIOWrapper の場合のみ)。"""
    if not isinstance(sys.stdout, io.TextIOWrapper):
        return
    with contextlib.suppress(OSError, ValueError):
        sys.stdout.reconfigure(encoding="utf-8")


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now():%Y%m%d_%H%M%S}.log"
    _enable_utf8_stdout()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_path


def purge_old_logs() -> None:
    cutoff = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
    for f in LOG_DIR.glob("*.log"):
        try:
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# ロック
# ---------------------------------------------------------------------------
def _read_lock_pid() -> int | None:
    """ロックファイルから PID を読み出す。無効/不在なら None。"""
    if not LOCK_FILE.exists():
        return None
    try:
        return int(LOCK_FILE.read_text(encoding="utf-8").strip())
    except (ValueError, OSError):
        return None


def _lock_owner_age_minutes(pid: int) -> float | None:
    """ロック保持者プロセスの経過時間 (分)。プロセス不在/不明なら None。"""
    if not psutil.pid_exists(pid):
        return None
    try:
        return (time.time() - psutil.Process(pid).create_time()) / 60
    except psutil.Error:
        return None


def _write_lock_file() -> bool:
    try:
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except OSError as e:
        logging.error("ロックファイル書込失敗: %s", e)
        return False


def acquire_lock(stale_minutes: int = 180) -> bool:
    """ロック取得。既存ロックの PID が生きていて経過時間 < stale_minutes なら失敗。"""
    pid = _read_lock_pid()
    if pid is None:
        return _write_lock_file()

    age_min = _lock_owner_age_minutes(pid)
    if age_min is None:
        logging.info("既存ロックは死んだ PID のため削除")
        return _write_lock_file()

    if age_min < stale_minutes:
        logging.error("既存ロック PID=%d (経過 %.1f 分) のため終了", pid, age_min)
        return False

    logging.warning(
        "既存ロック PID=%d は stale (%.1f 分 >= %d 分) のため奪取",
        pid,
        age_min,
        stale_minutes,
    )
    return _write_lock_file()


def release_lock() -> None:
    with contextlib.suppress(OSError):
        LOCK_FILE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# アプリ起動 / 接続
# ---------------------------------------------------------------------------
def find_app_pid() -> int | None:
    for p in psutil.process_iter(["pid", "name"]):
        if p.info.get("name") == APP_PROCESS_NAME:
            return int(p.info["pid"])
    return None


def _wait_for_app_pid(launch_timeout: int) -> int | None:
    """find_app_pid を 1 秒間隔でポーリングし PID を待つ。"""
    deadline = time.time() + launch_timeout
    while time.time() < deadline:
        time.sleep(1)
        pid = find_app_pid()
        if pid is not None:
            return pid
    return None


def find_installed_app_exe() -> Path | None:
    """Return the newest executable from the current user's ClickOnce cache."""
    if not CLICKONCE_INSTALL_ROOT.exists():
        return None
    candidates = tuple(CLICKONCE_INSTALL_ROOT.rglob(APP_PROCESS_NAME))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def resolve_app_launch_path() -> Path:
    """Prefer the installed executable and retain appref-ms as a fallback."""
    installed_exe = find_installed_app_exe()
    if installed_exe is not None:
        return installed_exe
    if APPREF_PATH.exists():
        return APPREF_PATH
    raise FileNotFoundError(
        f"PC-KEIBA Database launch target not found: {CLICKONCE_INSTALL_ROOT} / {APPREF_PATH}"
    )


def ensure_app_running(launch_timeout: int = 90) -> int:
    pid = find_app_pid()
    if pid is not None:
        logging.info("PC-KEIBA Database は既に PID=%d で起動中", pid)
        return pid
    launch_path = resolve_app_launch_path()
    logging.info("起動: %s", launch_path)
    os.startfile(str(launch_path))
    pid = _wait_for_app_pid(launch_timeout)
    if pid is None:
        raise RuntimeError(f"プロセスが {launch_timeout} 秒以内に起動しませんでした")
    logging.info("プロセス確立 PID=%d", pid)
    return pid


def connect_main(pid: int, timeout: int = 120) -> tuple[Application, UiWindow]:
    """メインウィンドウへ接続し visible まで待つ。"""
    app = Application(backend="uia").connect(process=pid, timeout=timeout)
    main: UiWindow = app.window(title_re=APP_WINDOW_TITLE_RE)
    main.wait("visible exists ready", timeout=timeout)
    return app, main


# ---------------------------------------------------------------------------
# UI 要素探索ヘルパ
# ---------------------------------------------------------------------------
def _safe_automation_id(elem: UiElement) -> str | None:
    """element_info.automation_id を安全に取得 (失敗時 None)。"""
    try:
        return elem.element_info.automation_id
    except Exception:
        return None


def find_start_button(main_window: UiWindow) -> UiElement | None:
    """StartButton (auto_id) を main ウィンドウ配下から探す。
    MDI 子ウィンドウとして表示されるため descendants で取得。"""
    for btn in main_window.descendants(control_type="Button"):
        if _safe_automation_id(btn) == START_BUTTON_AUTO_ID:
            return btn
    return None


def _is_offscreen(elem: UiElement) -> bool:
    try:
        return bool(elem.element_info.element.CurrentIsOffscreen)
    except Exception:
        return False


def _pick_visible_menu_item(main_window: UiWindow, title: str) -> UiElement | None:
    """同名 MenuItem が複数ある場合に visible (offscreen=False) を優先で返す。"""
    candidates = main_window.descendants(title=title, control_type="MenuItem")
    visible = [m for m in candidates if not _is_offscreen(m)]
    if visible:
        return visible[0]
    return candidates[0] if candidates else None


def _window_title(window: UiWindow) -> str:
    try:
        return window.window_text() or ""
    except Exception:
        return ""


def _covering_process_name(window: UiWindow) -> str:
    try:
        pid = int(window.element_info.process_id)
    except Exception:
        return ""
    try:
        return psutil.Process(pid).name()
    except Exception:
        return ""


def _is_covering_window(window: UiWindow) -> bool:
    title = _window_title(window)
    lowered = title.lower()
    if any(marker.lower() in lowered for marker in COVERING_WINDOW_TITLE_MARKERS):
        return True
    return _covering_process_name(window) in COVERING_PROCESS_NAMES


def minimize_covering_windows() -> int:
    """Minimize Terminal / python consoles that hide the registration dialog."""
    minimized = 0
    try:
        windows = Desktop(backend="uia").windows()
    except Exception as error:
        logging.warning("covering window list failed: %s", error)
        return 0
    for window in windows:
        try:
            if not _is_covering_window(window):
                continue
            window.minimize()
            minimized += 1
        except Exception:
            continue
    if minimized:
        logging.info("minimized %d covering window(s)", minimized)
    return minimized


def reveal_registration_ui(main_window: UiWindow) -> None:
    """Bring 通常データ登録 in front of prlctl / Terminal consoles."""
    minimize_covering_windows()
    try:
        main_window.set_focus()
    except Exception as error:
        logging.warning("main set_focus failed: %s", error)
    pid = _window_pid(main_window)
    if pid is None:
        return
    progress = find_progress_window(pid)
    if progress is None:
        return
    try:
        progress.set_focus()
    except Exception as error:
        logging.warning("progress set_focus failed: %s", error)


def _select_normal_data_registration(main_window: UiWindow) -> None:
    """データメニュー → 通常データ登録 を順にクリックする (副作用のみ)。"""
    main_window.set_focus()
    time.sleep(0.3)
    main_window.child_window(
        title_re=DATA_MENU_TITLE_RE, control_type="MenuItem"
    ).click_input()
    time.sleep(0.8)
    item = _pick_visible_menu_item(main_window, NORMAL_REG_MENU_TITLE)
    if item is None:
        raise RuntimeError(f"MenuItem '{NORMAL_REG_MENU_TITLE}' が見つかりません")
    item.click_input()
    time.sleep(1.5)


def open_dialog_if_needed(main_window: UiWindow) -> None:
    """通常データ登録 ダイアログが見えなければメニューから開く。"""
    if find_start_button(main_window) is not None:
        logging.info("通常データ登録 ダイアログは既に開いている")
        return
    logging.info("メニュー: データ → 通常データ登録 を選択")
    _select_normal_data_registration(main_window)
    if find_start_button(main_window) is None:
        raise RuntimeError("ダイアログ展開後も StartButton が見つかりません")


def click_start(main_window: UiWindow, dry_run: bool = False) -> bool:
    """StartButton を押す。disabled なら False を返す (= 既に進行中扱い)。"""
    btn = find_start_button(main_window)
    if btn is None:
        raise RuntimeError("StartButton が見つかりません")
    enabled = btn.is_enabled()
    logging.info("StartButton enabled=%s", enabled)
    if not enabled:
        logging.warning("StartButton disabled - 既に更新進行中とみなしスキップ")
        return False
    if dry_run:
        logging.info("[dry-run] 開始ボタンは押下しません")
        return True
    btn.click_input()
    logging.info("開始ボタン押下完了")
    reveal_registration_ui(main_window)
    return True


def click_start_until_started(
    main_window: UiWindow,
    *,
    dry_run: bool = False,
) -> bool:
    """Click 開始 and retry if the dialog stays idle."""
    clicked = click_start(main_window, dry_run=dry_run)
    if not clicked or dry_run:
        return clicked
    remaining = START_CLICK_RETRIES
    while remaining > 0:
        time.sleep(START_CLICK_SETTLE_SEC)
        if is_update_in_progress(main_window):
            logging.info("更新開始を確認")
            return True
        reveal_registration_ui(main_window)
        button = find_start_button(main_window)
        if button is None:
            remaining -= 1
            continue
        if not button.is_enabled():
            logging.info("StartButton disabled — 更新開始")
            return True
        logging.warning("StartButton still enabled after click; retrying")
        button.click_input()
        remaining -= 1
    logging.warning("StartButton stayed enabled after retries")
    return True


# ---------------------------------------------------------------------------
# 進捗ダイアログ検出
# ---------------------------------------------------------------------------
def _is_progress_window(window: UiWindow, expected_pid: int) -> bool:
    """進捗ウィンドウかどうかを判定 (pid / title / ProgressBar / CloseButton)。"""
    try:
        if window.element_info.process_id != expected_pid:
            return False
        if window.window_text() != PROGRESS_WINDOW_TITLE:
            return False
        if not window.descendants(control_type="ProgressBar"):
            return False
        return any(
            _safe_automation_id(b) == PROGRESS_CLOSE_BUTTON_AUTO_ID
            for b in window.descendants(control_type="Button")
        )
    except Exception:
        return False


def find_progress_window(pid: int) -> UiWindow | None:
    """更新中に表示される独立 top-level の '通常データ登録' progress ウィンドウを探す。
    実機検証: 進捗中は主タイトルが '通常データ登録' で、ProgressBar と
    automation_id='CloseButton' を持つボタンが必ず存在する。
    両方の特徴を満たす場合のみ進捗ウィンドウとみなす (誤検出回避)。"""
    for w in Desktop(backend="uia").windows():
        if _is_progress_window(w, pid):
            return w
    return None


def _progress_text_by_automation_id(window: UiWindow, automation_id: str) -> str:
    try:
        for element in window.descendants():
            if _safe_automation_id(element) == automation_id:
                return element.window_text() or ""
    except Exception:
        pass
    return ""


def is_progress_completed(window: UiWindow) -> bool:
    """Require the explicit completed status and a fully processed file count."""
    status = _progress_text_by_automation_id(window, "StatusLabel")
    current = _progress_text_by_automation_id(window, "ValueLabel1")
    maximum = _progress_text_by_automation_id(window, "MaximumLabel1")
    return status == "完了しました" and current != "" and current == maximum


def _progress_failure_message(window: UiWindow) -> str | None:
    detail = _progress_text_by_automation_id(window, "RichTextBox1")
    return next(
        (message for message in PROGRESS_FATAL_MESSAGES if message in detail), None
    )


def dismiss_completed_progress(pid: int) -> bool:
    """Invoke CloseButton only after the progress UI proves completion."""
    window = find_progress_window(pid)
    if window is None or not is_progress_completed(window):
        return False
    failure = _progress_failure_message(window)
    if failure is not None:
        raise RuntimeError(f"PC-KEIBA source update failed: {failure}")
    try:
        button = next(
            button
            for button in window.descendants(control_type="Button")
            if _safe_automation_id(button) == PROGRESS_CLOSE_BUTTON_AUTO_ID
            and button.is_enabled()
        )
        invoke = getattr(button, "invoke", None)
        if not callable(invoke):
            return False
        invoke()
        time.sleep(0.5)
    except (Exception, StopIteration):
        return False
    logging.info("完了ダイアログの閉じるを押下")
    return True


def is_update_in_progress_by_pid(pid: int) -> bool:
    """Check progress presence without mutating the PC-KEIBA UI."""
    try:
        return find_progress_window(pid) is not None
    except Exception as e:
        logging.warning("進行中判定 (PID) 失敗 (安全側=進行中扱い): %s", e)
        return True


def is_update_in_progress(main_window: UiWindow) -> bool:
    """更新処理中か判定。優先順:
      1. 独立進捗ウィンドウが存在 → 進行中
      2. StartButton 不在 / disabled → 進行中
      3. StartButton enabled → アイドル
    判定時に例外が出た場合は安全側 (=進行中) に倒す。"""
    try:
        pid = main_window.element_info.process_id
        if find_progress_window(pid) is not None:
            return True
        btn = find_start_button(main_window)
        if btn is None:
            return True
        return not btn.is_enabled()
    except Exception as e:
        logging.warning("進行中判定で例外 (安全側=進行中扱い): %s", e)
        return True


def safe_close_app(main_window: UiWindow) -> bool:
    """更新中でないことを確認してからアプリを閉じる。進行中なら閉じずに False。"""
    if is_update_in_progress(main_window):
        logging.warning("更新処理中のためアプリを閉じません (safe_close_app)")
        return False
    try:
        logging.info("アプリを閉じます")
        main_window.close()
        return True
    except Exception as e:
        logging.warning("close 失敗 (無視): %s", e)
        return False


def shutdown_windows() -> None:
    """Request a graceful Windows shutdown after a verified update."""
    if sys.platform != "win32":
        raise RuntimeError("--shutdown-when-done is supported only on Windows")
    logging.info("Requesting graceful Windows shutdown after completed update")
    subprocess.run(
        [
            "shutdown.exe",
            "/s",
            "/t",
            str(WINDOWS_SHUTDOWN_DELAY_SEC),
            "/d",
            "p:0:0",
            "/c",
            "PC-KEIBA update completed",
        ],
        check=True,
        timeout=WINDOWS_SHUTDOWN_TIMEOUT_SEC,
    )


# ---------------------------------------------------------------------------
# 完了待機 (任意)
# ---------------------------------------------------------------------------
StartButtonState = Literal["absent", "enabled", "disabled"]


def _probe_start_button(main_window: UiWindow) -> StartButtonState:
    """StartButton の現在状態を 3 値で返す。"""
    btn = find_start_button(main_window)
    if btn is None:
        return "absent"
    try:
        return "enabled" if btn.is_enabled() else "disabled"
    except Exception:
        return "enabled"  # 取れない場合は enabled 扱い (started に進ませる)


def _window_pid(main_window: UiWindow) -> int | None:
    """main window の PID。取得失敗時は None。"""
    try:
        return int(main_window.element_info.process_id)
    except Exception:
        return None


def _progress_visible(pid: int | None) -> bool:
    return pid is not None and find_progress_window(pid) is not None


def _log_first(already: bool, message: str) -> bool:
    """初回だけ INFO を出し、以降は静かに True を返す。"""
    if not already:
        logging.info(message)
    return True


def wait_for_progress_window_to_finish(
    pid: int, max_minutes: int = 180, poll_sec: int = 15
) -> bool:
    """進捗ウィンドウの消滅だけを待つ (main window 未接続でも可)。
    タイムアウト時は False (成功扱いにしない)。"""
    deadline = time.time() + max_minutes * 60
    logging.info(
        "進捗ウィンドウ完了待機開始 (最大 %d 分, poll %d 秒, PID=%d)",
        max_minutes,
        poll_sec,
        pid,
    )
    while time.time() < deadline:
        if not is_update_in_progress_by_pid(pid):
            logging.info("進捗ウィンドウ消滅 → 完了")
            return True
        time.sleep(poll_sec)
    logging.warning("進捗ウィンドウ完了待機タイムアウト (%d 分)", max_minutes)
    return False


def _close_explicitly_completed_progress(
    main_window: UiWindow, pid: int | None
) -> bool:
    if pid is None:
        return False
    progress_window = find_progress_window(pid)
    if progress_window is None or not is_progress_completed(progress_window):
        return False
    if not dismiss_completed_progress(pid) or _progress_visible(pid):
        return False
    logging.info("Completed progress dialog closed -> complete")
    _dismiss_popups(main_window)
    return True


def wait_for_completion(
    main_window: UiWindow, max_minutes: int = 180, poll_sec: int = 15
) -> bool:
    """Wait until StartButton proves that the database update is idle again.

    The progress dialog exposes an enabled ``CloseButton`` while processing, so
    clicking that button is not completion evidence.  Only dismiss it after the
    main dialog's StartButton is enabled.  A vanished progress window or an
    absent StartButton alone must never permit the host to stop the VM.
    """
    deadline = time.time() + max_minutes * 60
    logging.info("完了待機開始 (最大 %d 分, poll %d 秒)", max_minutes, poll_sec)
    started_via_button = False
    saw_progress = False
    pid = _window_pid(main_window)
    while time.time() < deadline:
        minimize_covering_windows()
        with contextlib.suppress(Exception):
            main_window.set_focus()
        _dismiss_popups(main_window)
        if _close_explicitly_completed_progress(main_window, pid):
            return True
        progress_visible = _progress_visible(pid)
        state = _probe_start_button(main_window)
        if state == "enabled" and (started_via_button or saw_progress):
            if progress_visible and pid is not None:
                dismiss_completed_progress(pid)
                progress_visible = _progress_visible(pid)
            if not progress_visible:
                logging.info(
                    "StartButton enabled and progress dialog absent -> complete"
                )
                _dismiss_popups(main_window)
                return True
        if progress_visible:
            saw_progress = _log_first(saw_progress, "進捗ウィンドウを検出 — 更新進行中")
            time.sleep(poll_sec)
            continue
        if state == "disabled":
            started_via_button = _log_first(
                started_via_button, "更新進行中を確認 (StartButton disabled)"
            )
            time.sleep(poll_sec)
            continue
        time.sleep(poll_sec)
    logging.warning("完了待機タイムアウト (%d 分)", max_minutes)
    return False


# ---------------------------------------------------------------------------
# ポップアップ自動 dismiss
# ---------------------------------------------------------------------------
def _main_window_identity(main_window: UiWindow) -> tuple[int, int] | None:
    """(pid, handle) を返す。取得失敗時は None。"""
    try:
        return main_window.element_info.process_id, main_window.element_info.handle
    except Exception:
        return None


def _iter_sibling_windows(pid: int, exclude_handle: int) -> Iterator[UiWindow]:
    """対象 PID の top-level window を yield する (exclude_handle と例外個別 skip)。"""
    for w in Desktop(backend="uia").windows():
        try:
            if w.element_info.process_id != pid:
                continue
            if w.element_info.handle == exclude_handle:
                continue
        except Exception:
            continue
        yield w


def _button_matches_label(button: UiElement, label: str) -> bool:
    try:
        return (button.window_text() or "") == label and button.is_enabled()
    except Exception:
        return False


def _find_labeled_button(window: UiWindow, label: str) -> UiElement | None:
    """Prefer a direct child, then search descendants (完了画面の閉じる is nested)."""
    try:
        button = window.child_window(title=label, control_type="Button")
        if button.exists() and button.is_enabled():
            return button
    except Exception:
        pass
    try:
        for button in window.descendants(control_type="Button"):
            if _button_matches_label(button, label):
                return button
    except Exception:
        return None
    return None


def _try_click_label(window: UiWindow, label: str) -> bool:
    """指定ラベルのボタンが存在 + enabled なら押下し True。
    存在しなければ / 例外時は False。"""
    button = _find_labeled_button(window, label)
    if button is None:
        return False
    title = window.window_text() or ""
    logging.info("ポップアップ '%s' を [%s] で閉じる", title, label)
    button.click_input()
    time.sleep(0.5)
    return True


def _try_dismiss_popup(window: UiWindow) -> bool:
    """既定ラベル候補を順番に試し、最初に成功したものを使って閉じる。"""
    return any(_try_click_label(window, label) for label in DEFAULT_DISMISS_LABELS)


def _dismiss_popups(main_window: UiWindow) -> None:
    """OK / はい などのデフォルトボタンを持つ確認/完了ダイアログがあれば閉じる。
    進捗ウィンドウ (CloseButton) は閉じない。"""
    identity = _main_window_identity(main_window)
    if identity is None:
        return
    pid, handle = identity
    for popup in _iter_sibling_windows(pid, handle):
        if _is_progress_window(popup, pid):
            continue
        _try_dismiss_popup(popup)


# ---------------------------------------------------------------------------
# main 用ヘルパ
# ---------------------------------------------------------------------------
class _UpdateInProgress(Exception):
    """接続前後で進行中状態を検出した際の内部シグナル。"""


class _WaitTimedOut(Exception):
    """`--wait` が完了を検出できずタイムアウトした。"""


def _connect_main_with_retry(pid: int) -> UiWindow:
    """connect_main を最大 CONNECT_RETRIES 回試行。途中で進行中を検出したら
    `_UpdateInProgress` を上位に伝える。"""
    last_error: Exception | None = None
    for _ in range(CONNECT_RETRIES):
        try:
            _, main_window = connect_main(pid, timeout=CONNECT_TIMEOUT_SEC)
            return main_window
        except (ElementNotFoundError, PwaTimeoutError) as e:
            if is_update_in_progress_by_pid(pid):
                logging.info("接続中に進行中状態を検出 - スキップ")
                raise _UpdateInProgress from None
            last_error = e
            logging.warning("メインウィンドウ接続再試行: %s", e)
            time.sleep(CONNECT_BACKOFF_SEC)
    raise RuntimeError("メインウィンドウに接続できません") from last_error


def _finalize_wait(main_window: UiWindow, args: argparse.Namespace) -> None:
    """`--wait` 後処理。完了検出時のみ `--close-when-done` を尊重する。
    タイムアウトは成功扱いにしない。"""
    done = wait_for_completion(main_window, max_minutes=args.wait_minutes)
    if not done:
        logging.warning("完了未検出のためアプリを閉じません (--close-when-done 無視)")
        raise _WaitTimedOut("完了待機がタイムアウトしました")
    if args.close_when_done:
        safe_close_app(main_window)
    if args.shutdown_when_done:
        shutdown_windows()


def _handle_already_in_progress(pid: int, args: argparse.Namespace) -> None:
    """既存の進捗ウィンドウを検出したとき。`--wait` なら消滅まで待つ。"""
    logging.info("進捗ウィンドウを検出 - 更新進行中 (PID=%d)", pid)
    if not args.wait:
        logging.info("更新進行中とみなしスキップ")
        return
    if not wait_for_progress_window_to_finish(pid, max_minutes=args.wait_minutes):
        raise _WaitTimedOut("完了待機がタイムアウトしました")
    if args.shutdown_when_done:
        shutdown_windows()


def _run_workflow(args: argparse.Namespace) -> None:
    """主処理。短絡で in-progress を検出したら早期 return。
    `--wait` 時は既存進捗の消滅まで待つ。"""
    pid = ensure_app_running()
    if is_update_in_progress_by_pid(pid):
        _handle_already_in_progress(pid, args)
        return
    try:
        main_window = _connect_main_with_retry(pid)
    except _UpdateInProgress:
        _handle_already_in_progress(pid, args)
        return
    open_dialog_if_needed(main_window)
    clicked = click_start_until_started(main_window, dry_run=args.dry_run)
    if args.wait and clicked and not args.dry_run:
        _finalize_wait(main_window, args)


# ---------------------------------------------------------------------------
# main / CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="PC-KEIBA Database 自動データ更新")
    ap.add_argument("--wait", action="store_true", help="完了まで待機")
    ap.add_argument("--wait-minutes", type=int, default=180, help="完了待機の最大分数")
    ap.add_argument(
        "--close-when-done",
        action="store_true",
        help="完了後にアプリを閉じる (--wait と併用)",
    )
    ap.add_argument(
        "--shutdown-when-done",
        action="store_true",
        help="完了後にWindowsを正常シャットダウン (--wait と併用)",
    )
    ap.add_argument("--dry-run", action="store_true", help="開始ボタンを押さずに終了")
    ap.add_argument(
        "--lock-stale-min",
        type=int,
        default=180,
        help="既存ロックを無効と見なす分数 (既定 180)",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    log_path = setup_logging()
    purge_old_logs()
    hide_own_console()
    logging.info("=== 開始 ログ=%s args=%s ===", log_path, vars(args))

    if not acquire_lock(args.lock_stale_min):
        return 2

    try:
        _run_workflow(args)
        logging.info("=== 正常終了 ===")
        return 0
    except _WaitTimedOut as e:
        logging.error("%s", e)
        return 3
    except Exception:
        logging.exception("エラー発生")
        return 1
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(main())
