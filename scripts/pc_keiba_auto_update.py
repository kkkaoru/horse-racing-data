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
  - --close-when-done が指定されていても、完了検出 (StartButton が enabled に戻る)
    を取れていなければクローズしない。
  - また close 直前に必ず is_update_in_progress() で再確認する。

ログ:
  %LOCALAPPDATA%\pc-keiba-auto-update\logs\YYYYMMDD_HHMMSS.log
  古いログは 30 日で自動削除 (件数ではなく日数ベース)。

依存: pywinauto, psutil   (install-pc-keiba-auto-update.ps1 が pip でインストールする)

使い方:
  py -3.12 pc-keiba-auto-update.py                 # 起動 + 開始押下 (即時 exit, 完了非待機)
  py -3.12 pc-keiba-auto-update.py --wait          # 完了まで待機 (最大 --wait-minutes)
  py -3.12 pc-keiba-auto-update.py --close-when-done  # 完了後アプリを閉じる
  py -3.12 pc-keiba-auto-update.py --dry-run       # 開始ボタンを押さず終了 (検証用)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import psutil
from pywinauto import Application, Desktop
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.timings import TimeoutError as PwaTimeoutError


# ---------------------------------------------------------------------------
# pywinauto には公式型 stub が無いため、本ファイルで実際に呼び出す API のみを
# Protocol として宣言し、Any の代わりに構造的型として使う。
# ---------------------------------------------------------------------------
class _ElementInfo(Protocol):  # pragma: no cover
    @property
    def automation_id(self) -> str: ...
    @property
    def process_id(self) -> int: ...
    @property
    def handle(self) -> int: ...
    @property
    def control_type(self) -> str: ...
    @property
    def element(self) -> Any: ...  # UIA raw IUIAutomationElement (COM)


class _UiElement(Protocol):  # pragma: no cover
    @property
    def element_info(self) -> _ElementInfo: ...
    def click_input(self) -> Any: ...
    def is_enabled(self) -> bool: ...
    def exists(self) -> bool: ...
    def window_text(self) -> str: ...
    def descendants(
        self, *, control_type: str = ..., title: str = ...
    ) -> list["_UiElement"]: ...
    def child_window(
        self,
        *,
        title: str = ...,
        title_re: str = ...,
        control_type: str = ...,
    ) -> "_UiElement": ...


class _UiWindow(_UiElement, Protocol):  # pragma: no cover
    def set_focus(self) -> Any: ...
    def close(self) -> Any: ...
    def wait(self, state: str, timeout: float = ...) -> Any: ...

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
APP_PROCESS_NAME = "Com.Pckeiba.Database.exe"
APP_WINDOW_TITLE_RE = r"^PC-KEIBA Database$"
APPREF_PATH = (
    Path(os.environ["APPDATA"])
    / "Microsoft" / "Windows" / "Start Menu" / "Programs"
    / "pc-keiba.com" / "PC-KEIBA Database.appref-ms"
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

LOCK_FILE = Path(os.environ["TEMP"]) / "pc-keiba-auto-update.lock"
LOG_DIR = Path(os.environ["LOCALAPPDATA"]) / "pc-keiba-auto-update" / "logs"
LOG_RETENTION_DAYS = 30


# ---------------------------------------------------------------------------
# ログ
# ---------------------------------------------------------------------------
def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now():%Y%m%d_%H%M%S}.log"
    # Windows コンソールは既定で CP932。日本語ログを stdout に出すため UTF-8 化。
    # sys.stdout は TextIOWrapper の場合のみ reconfigure() を持つ (Python 3.7+)。
    import io as _io

    if isinstance(sys.stdout, _io.TextIOWrapper):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except (OSError, ValueError):
            pass
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
def acquire_lock(stale_minutes: int = 180) -> bool:
    """ロック取得。既存ロックの PID が生きていて経過時間 < stale_minutes なら失敗。"""
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text(encoding="utf-8").strip())
        except (ValueError, OSError):
            pid = None
        if pid and psutil.pid_exists(pid):
            try:
                age_min = (time.time() - psutil.Process(pid).create_time()) / 60
                if age_min < stale_minutes:
                    logging.error(
                        "既存ロック PID=%d (経過 %.1f 分) のため終了", pid, age_min
                    )
                    return False
                logging.warning(
                    "既存ロック PID=%d は stale (%.1f 分 >= %d 分) のため奪取",
                    pid, age_min, stale_minutes,
                )
            except psutil.Error:
                pass
        else:
            logging.info("既存ロックは死んだ PID のため削除")
    try:
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    except OSError as e:
        logging.error("ロックファイル書込失敗: %s", e)
        return False
    return True


def release_lock() -> None:
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# アプリ起動 / 接続
# ---------------------------------------------------------------------------
def find_app_pid() -> int | None:
    for p in psutil.process_iter(["pid", "name"]):
        if p.info.get("name") == APP_PROCESS_NAME:
            return int(p.info["pid"])
    return None


def ensure_app_running(launch_timeout: int = 90) -> int:
    pid = find_app_pid()
    if pid is not None:
        logging.info("PC-KEIBA Database は既に PID=%d で起動中", pid)
        return pid
    if not APPREF_PATH.exists():
        raise FileNotFoundError(f"appref-ms 未検出: {APPREF_PATH}")
    logging.info("起動: %s", APPREF_PATH)
    os.startfile(str(APPREF_PATH))
    deadline = time.time() + launch_timeout
    while time.time() < deadline:
        time.sleep(1)
        pid = find_app_pid()
        if pid is not None:
            logging.info("プロセス確立 PID=%d", pid)
            # メインウィンドウ表示まで少し待つ (ClickOnce 初回起動は重い)
            return pid
    raise RuntimeError(f"プロセスが {launch_timeout} 秒以内に起動しませんでした")


def connect_main(pid: int, timeout: int = 120) -> tuple[Any, _UiWindow]:
    """メインウィンドウへ接続し visible まで待つ。
    第 1 戻り値は pywinauto Application (本ファイル外では使わない opaque)。"""
    app = Application(backend="uia").connect(process=pid, timeout=timeout)
    main: _UiWindow = app.window(title_re=APP_WINDOW_TITLE_RE)
    main.wait("visible exists ready", timeout=timeout)
    return app, main


# ---------------------------------------------------------------------------
# 通常データ登録 ダイアログ操作
# ---------------------------------------------------------------------------
def find_start_button(main_window: _UiWindow) -> _UiElement | None:
    """StartButton (auto_id) を main ウィンドウ配下から探す。
    MDI 子ウィンドウとして表示されるため descendants で取得。"""
    for btn in main_window.descendants(control_type="Button"):
        try:
            if btn.element_info.automation_id == START_BUTTON_AUTO_ID:
                return btn
        except Exception:
            continue
    return None


def open_dialog_if_needed(main_window: _UiWindow) -> None:
    """通常データ登録 ダイアログが見えなければメニューから開く。"""
    if find_start_button(main_window) is not None:
        logging.info("通常データ登録 ダイアログは既に開いている")
        return
    logging.info("メニュー: データ → 通常データ登録 を選択")
    main_window.set_focus()
    time.sleep(0.3)
    data_menu = main_window.child_window(
        title_re=DATA_MENU_TITLE_RE, control_type="MenuItem"
    )
    data_menu.click_input()
    time.sleep(0.8)
    # 表示中 (offscreen=False) の MenuItem を選ぶ (同名 MenuItem が 2 つ存在する)
    candidates = main_window.descendants(
        title=NORMAL_REG_MENU_TITLE, control_type="MenuItem"
    )
    visible = [m for m in candidates if not _is_offscreen(m)]
    target = visible[0] if visible else (candidates[0] if candidates else None)
    if target is None:
        raise RuntimeError(f"MenuItem '{NORMAL_REG_MENU_TITLE}' が見つかりません")
    target.click_input()
    time.sleep(1.5)
    if find_start_button(main_window) is None:
        raise RuntimeError("ダイアログ展開後も StartButton が見つかりません")


def _is_offscreen(elem: _UiElement) -> bool:
    try:
        return bool(elem.element_info.element.CurrentIsOffscreen)
    except Exception:
        return False


def click_start(main_window: _UiWindow, dry_run: bool = False) -> bool:
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
    return True


def find_progress_window(pid: int) -> _UiWindow | None:
    """更新中に表示される独立 top-level の '通常データ登録' progress ウィンドウを探す。
    実機検証: 進捗中は主タイトルが '通常データ登録' で、ProgressBar と
    automation_id='CloseButton' を持つボタンが必ず存在する。
    両方の特徴を満たす場合のみ進捗ウィンドウとみなす (誤検出回避)。"""
    for w in Desktop(backend="uia").windows():
        try:
            if w.element_info.process_id != pid:
                continue
            if w.window_text() != PROGRESS_WINDOW_TITLE:
                continue
            has_progress = bool(w.descendants(control_type="ProgressBar"))
            has_close = any(
                b.element_info.automation_id == PROGRESS_CLOSE_BUTTON_AUTO_ID
                for b in w.descendants(control_type="Button")
            )
            if has_progress and has_close:
                return w
        except Exception:
            continue
    return None


def is_update_in_progress_by_pid(pid: int) -> bool:
    """プロセス PID だけで進行中判定。main window 未接続時にも使える。"""
    try:
        return find_progress_window(pid) is not None
    except Exception as e:
        logging.warning("進行中判定 (PID) 失敗 (安全側=進行中扱い): %s", e)
        return True


def is_update_in_progress(main_window: _UiWindow) -> bool:
    """更新処理中か判定。優先順:
      1. 独立進捗ウィンドウが存在 → 進行中
      2. StartButton が見つかり enabled → アイドル
      3. それ以外 (StartButton 不在 / disabled / 例外) → 安全側=進行中扱い
    """
    try:
        pid = main_window.element_info.process_id
        if find_progress_window(pid) is not None:
            return True
    except Exception as e:
        logging.warning("進捗ウィンドウ検索失敗 (安全側=進行中扱い): %s", e)
        return True
    try:
        btn = find_start_button(main_window)
    except Exception as e:
        logging.warning("進行中判定失敗 (安全側=進行中扱い): %s", e)
        return True
    if btn is None:
        logging.warning("StartButton 不在 → 安全側=進行中扱い")
        return True
    try:
        return not btn.is_enabled()
    except Exception as e:
        logging.warning("StartButton.is_enabled() 失敗 (安全側=進行中扱い): %s", e)
        return True


def safe_close_app(main_window: _UiWindow) -> bool:
    """更新中でないことを確認してからアプリを閉じる。
    進行中なら閉じずに False を返す。"""
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


# ---------------------------------------------------------------------------
# 完了待機 (任意)
# ---------------------------------------------------------------------------
def wait_for_completion(main_window: _UiWindow, max_minutes: int = 180, poll_sec: int = 15) -> bool:
    """StartButton が再び enabled になる、または完了/エラーポップアップが
    表示されたら終了とみなす。"""
    deadline = time.time() + max_minutes * 60
    logging.info("完了待機開始 (最大 %d 分, poll %d 秒)", max_minutes, poll_sec)
    # 一度 disabled になるのを確認 (起動確認)
    started = False
    while time.time() < deadline:
        time.sleep(poll_sec)
        _dismiss_popups(main_window)
        btn = find_start_button(main_window)
        if btn is None:
            logging.info("StartButton 消滅 → 完了とみなす")
            return True
        try:
            enabled = btn.is_enabled()
        except Exception:
            enabled = True
        if not started and not enabled:
            started = True
            logging.info("更新進行中を確認")
            continue
        if started and enabled:
            logging.info("StartButton が再 enabled → 完了")
            return True
    logging.warning("完了待機タイムアウト (%d 分)", max_minutes)
    return False


def _dismiss_popups(main_window: _UiWindow) -> None:
    """OK / はい などのデフォルトボタンを持つ確認/完了ダイアログがあれば閉じる。"""
    try:
        pid = main_window.element_info.process_id
    except Exception:
        return
    for w in Desktop(backend="uia").windows():
        try:
            if w.element_info.process_id != pid:
                continue
            if w.element_info.handle == main_window.element_info.handle:
                continue
            title = w.window_text() or ""
            for label in ("OK", "はい", "閉じる"):
                try:
                    btn = w.child_window(title=label, control_type="Button")
                    if btn.exists() and btn.is_enabled():
                        logging.info("ポップアップ '%s' を [%s] で閉じる", title, label)
                        btn.click_input()
                        time.sleep(0.5)
                        break
                except Exception:
                    continue
        except Exception:
            continue


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="PC-KEIBA Database 自動データ更新")
    ap.add_argument("--wait", action="store_true", help="完了まで待機")
    ap.add_argument("--wait-minutes", type=int, default=180, help="完了待機の最大分数")
    ap.add_argument("--close-when-done", action="store_true",
                    help="完了後にアプリを閉じる (--wait と併用)")
    ap.add_argument("--dry-run", action="store_true", help="開始ボタンを押さずに終了")
    ap.add_argument("--lock-stale-min", type=int, default=180,
                    help="既存ロックを無効と見なす分数 (既定 180)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    log_path = setup_logging()
    purge_old_logs()
    logging.info("=== 開始 ログ=%s args=%s ===", log_path, vars(args))

    if not acquire_lock(args.lock_stale_min):
        return 2

    try:
        pid = ensure_app_running()

        # 接続前に「更新進行中」を進捗ウィンドウで判定する。
        # 更新中は MdiParentForm が "ready" 状態にならず connect_main が timeout
        # するため、connect 試行の前にこちらで短絡する。
        if is_update_in_progress_by_pid(pid):
            logging.info(
                "進捗ウィンドウを検出 - 更新進行中とみなしスキップ (PID=%d)", pid
            )
            return 0

        # 初回起動時はメインウィンドウ表示まで時間がかかる
        main_window: _UiWindow | None = None
        for _ in range(3):
            try:
                _app, main_window = connect_main(pid, timeout=60)
                break
            except (ElementNotFoundError, PwaTimeoutError) as e:
                # connect 中に進行中に切り替わった可能性も考慮し再度短絡判定
                if is_update_in_progress_by_pid(pid):
                    logging.info("接続中に進行中状態を検出 - スキップ")
                    return 0
                logging.warning("メインウィンドウ接続再試行: %s", e)
                time.sleep(5)
        if main_window is None:
            raise RuntimeError("メインウィンドウに接続できません")

        open_dialog_if_needed(main_window)
        clicked = click_start(main_window, dry_run=args.dry_run)

        if args.wait and clicked and not args.dry_run:
            done = wait_for_completion(main_window, max_minutes=args.wait_minutes)
            if args.close_when_done:
                if not done:
                    logging.warning(
                        "完了未検出のためアプリを閉じません (--close-when-done 無視)"
                    )
                else:
                    safe_close_app(main_window)

        logging.info("=== 正常終了 ===")
        return 0
    except Exception:
        logging.exception("エラー発生")
        return 1
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(main())
