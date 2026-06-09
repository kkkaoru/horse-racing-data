"""pc_keiba_auto_update.py のユニットテスト。

pywinauto 等の Windows ランタイム依存は conftest.py で sys.modules に stub を注入し、
個別テストで MagicMock を使ってふるまいを差し替える。
"""

from __future__ import annotations

import itertools
import logging
import sys
import time
from pathlib import Path
from typing import override
from unittest.mock import MagicMock, patch

import pytest

import pc_keiba_auto_update as mod


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def isolate_tmp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """各テストでロック / ログを tmp_path に隔離する (pytest autouse)。"""
    lock = tmp_path / "lock"
    log_dir = tmp_path / "logs"
    monkeypatch.setattr(mod, "LOCK_FILE", lock)
    monkeypatch.setattr(mod, "LOG_DIR", log_dir)
    monkeypatch.setattr(mod, "APPREF_PATH", tmp_path / "appref.appref-ms")
    return tmp_path


@pytest.fixture(autouse=True)
def reset_logging() -> None:
    """各テスト前に root logger ハンドラをクリアし basicConfig を再適用可能にする。"""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)


def _mk_element(
    *,
    automation_id: str = "",
    control_type: str = "Button",
    name: str = "",
    is_enabled: bool = True,
    is_offscreen: bool = False,
    process_id: int = 1234,
    handle: int = 1,
    exists: bool = True,
    descendants: list[MagicMock] | None = None,
) -> MagicMock:
    elem = MagicMock()
    elem.element_info.automation_id = automation_id
    elem.element_info.control_type = control_type
    elem.element_info.process_id = process_id
    elem.element_info.handle = handle
    elem.element_info.name = name
    elem.element_info.element.CurrentIsOffscreen = is_offscreen
    elem.is_enabled.return_value = is_enabled
    elem.exists.return_value = exists
    elem.window_text.return_value = name

    def _descendants(**kw: object) -> list[MagicMock]:
        items = descendants or []
        ct = kw.get("control_type")
        title = kw.get("title")
        out = items
        if ct:
            out = [e for e in out if e.element_info.control_type == ct]
        if title:
            out = [e for e in out if e.element_info.name == title]
        return out

    elem.descendants.side_effect = _descendants
    return elem


# ---------------------------------------------------------------------------
# setup_logging / purge_old_logs
# ---------------------------------------------------------------------------
def test_setup_logging_creates_log_dir_and_file(tmp_path: Path) -> None:
    log_path = mod.setup_logging()
    assert log_path.parent == mod.LOG_DIR
    assert mod.LOG_DIR.exists()
    logging.info("hello")
    # ファイルに何か書かれている (FileHandler が動いた)
    assert log_path.exists()


def test_setup_logging_handles_non_textiowrapper_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_stdout = MagicMock()
    # TextIOWrapper ではないので reconfigure は呼ばれない
    monkeypatch.setattr(sys, "stdout", fake_stdout)
    mod.setup_logging()
    fake_stdout.reconfigure.assert_not_called()


def test_setup_logging_swallow_reconfigure_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import io

    class _BrokenWrapper(io.TextIOWrapper):
        @override
        def reconfigure(self, **_: object) -> None:
            raise OSError("nope")

    broken = _BrokenWrapper(io.BytesIO(), encoding="ascii")
    monkeypatch.setattr(sys, "stdout", broken)
    # 例外が外に出ないことを確認
    mod.setup_logging()


def test_purge_old_logs_deletes_aged(tmp_path: Path) -> None:
    mod.LOG_DIR.mkdir(parents=True, exist_ok=True)
    old = mod.LOG_DIR / "old.log"
    new = mod.LOG_DIR / "new.log"
    old.write_text("x")
    new.write_text("y")
    # 60 日前
    import os

    sixty_days = time.time() - 60 * 86400
    os.utime(old, (sixty_days, sixty_days))
    mod.purge_old_logs()
    assert not old.exists()
    assert new.exists()


def test_purge_old_logs_no_dir_does_not_crash() -> None:
    # LOG_DIR が無くても例外を漏らさない
    mod.purge_old_logs()


# ---------------------------------------------------------------------------
# acquire_lock / release_lock
# ---------------------------------------------------------------------------
def test_acquire_lock_fresh() -> None:
    assert mod.acquire_lock() is True
    assert mod.LOCK_FILE.exists()


def test_acquire_lock_release() -> None:
    mod.acquire_lock()
    mod.release_lock()
    assert not mod.LOCK_FILE.exists()


def test_release_lock_idempotent() -> None:
    # 存在しなくても OK
    mod.release_lock()


def test_acquire_lock_existing_live_pid_within_stale_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod.LOCK_FILE.write_text("999")
    monkeypatch.setattr(mod.psutil, "pid_exists", lambda _pid: True)
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = time.time() - 60  # 1 分前
    monkeypatch.setattr(mod.psutil, "Process", lambda _pid: fake_proc)
    assert mod.acquire_lock(stale_minutes=180) is False


def test_acquire_lock_existing_live_pid_stale_takes_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod.LOCK_FILE.write_text("999")
    monkeypatch.setattr(mod.psutil, "pid_exists", lambda _pid: True)
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = time.time() - 60 * 60 * 4  # 4 時間前
    monkeypatch.setattr(mod.psutil, "Process", lambda _pid: fake_proc)
    assert mod.acquire_lock(stale_minutes=180) is True


def test_acquire_lock_existing_dead_pid_takes_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod.LOCK_FILE.write_text("999")
    monkeypatch.setattr(mod.psutil, "pid_exists", lambda _pid: False)
    assert mod.acquire_lock() is True


def test_acquire_lock_invalid_pid_content_takes_over() -> None:
    mod.LOCK_FILE.write_text("not-a-number")
    assert mod.acquire_lock() is True


def test_acquire_lock_psutil_process_raises_treated_as_takeover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod.LOCK_FILE.write_text("999")
    monkeypatch.setattr(mod.psutil, "pid_exists", lambda _pid: True)

    def _raise(_pid: int) -> object:
        raise mod.psutil.NoSuchProcess(999)

    monkeypatch.setattr(mod.psutil, "Process", _raise)
    assert mod.acquire_lock() is True


def test_acquire_lock_write_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bad = tmp_path / "nodir" / "lock"
    monkeypatch.setattr(mod, "LOCK_FILE", bad)
    # 親ディレクトリ無し → OSError
    assert mod.acquire_lock() is False


# ---------------------------------------------------------------------------
# find_app_pid / ensure_app_running
# ---------------------------------------------------------------------------
def _make_proc(pid: int, name: str) -> MagicMock:
    p = MagicMock()
    p.info = {"pid": pid, "name": name}
    return p


def test_find_app_pid_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        mod.psutil,
        "process_iter",
        lambda *_a, **_k: iter([_make_proc(42, mod.APP_PROCESS_NAME)]),
    )
    assert mod.find_app_pid() == 42


def test_find_app_pid_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        mod.psutil,
        "process_iter",
        lambda *_a, **_k: iter([_make_proc(1, "other.exe")]),
    )
    assert mod.find_app_pid() is None


def test_ensure_app_running_already(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "find_app_pid", lambda: 42)
    assert mod.ensure_app_running() == 42


def test_ensure_app_running_launches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mod.APPREF_PATH.write_text("dummy")
    monkeypatch.setattr(mod, "find_app_pid", MagicMock(side_effect=[None, None, 77]))
    monkeypatch.setattr(mod.os, "startfile", MagicMock(), raising=False)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.ensure_app_running() == 77


def test_ensure_app_running_appref_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "find_app_pid", lambda: None)
    # APPREF_PATH は autouse fixture で作成していない (Path だけ)
    with pytest.raises(FileNotFoundError):
        mod.ensure_app_running()


def test_ensure_app_running_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    mod.APPREF_PATH.write_text("dummy")
    monkeypatch.setattr(mod, "find_app_pid", lambda: None)
    monkeypatch.setattr(mod.os, "startfile", MagicMock(), raising=False)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError):
        mod.ensure_app_running(launch_timeout=1)


# ---------------------------------------------------------------------------
# connect_main
# ---------------------------------------------------------------------------
def test_connect_main(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_main = MagicMock()
    fake_app = MagicMock()
    fake_app.window.return_value = fake_main
    monkeypatch.setattr(
        mod, "Application", MagicMock(return_value=MagicMock(connect=MagicMock(return_value=fake_app)))
    )
    _app, main = mod.connect_main(123, timeout=1)
    fake_main.wait.assert_called_once()
    assert main is fake_main


# ---------------------------------------------------------------------------
# find_progress_window / is_update_in_progress_by_pid
# ---------------------------------------------------------------------------
def _mk_window_with(
    *, pid: int, title: str, buttons: list[MagicMock], has_progress: bool
) -> MagicMock:
    w = MagicMock()
    w.element_info.process_id = pid
    w.window_text.return_value = title

    def _desc(**kw: object) -> list[MagicMock]:
        ct = kw.get("control_type")
        if ct == "ProgressBar":
            return [MagicMock()] if has_progress else []
        if ct == "Button":
            return buttons
        return []

    w.descendants.side_effect = _desc
    return w


def test_find_progress_window_match(monkeypatch: pytest.MonkeyPatch) -> None:
    close_btn = _mk_element(automation_id=mod.PROGRESS_CLOSE_BUTTON_AUTO_ID)
    win = _mk_window_with(pid=1, title=mod.PROGRESS_WINDOW_TITLE, buttons=[close_btn], has_progress=True)
    other = _mk_window_with(pid=2, title="other", buttons=[], has_progress=False)
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [other, win]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    assert mod.find_progress_window(1) is win


def test_find_progress_window_no_progressbar(monkeypatch: pytest.MonkeyPatch) -> None:
    close_btn = _mk_element(automation_id=mod.PROGRESS_CLOSE_BUTTON_AUTO_ID)
    win = _mk_window_with(pid=1, title=mod.PROGRESS_WINDOW_TITLE, buttons=[close_btn], has_progress=False)
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [win]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    assert mod.find_progress_window(1) is None


def test_find_progress_window_skips_other_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    close_btn = _mk_element(automation_id=mod.PROGRESS_CLOSE_BUTTON_AUTO_ID)
    win = _mk_window_with(pid=999, title=mod.PROGRESS_WINDOW_TITLE, buttons=[close_btn], has_progress=True)
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [win]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    assert mod.find_progress_window(1) is None


def test_find_progress_window_swallows_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    bad = MagicMock()
    type(bad.element_info).process_id = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [bad]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    assert mod.find_progress_window(1) is None


def test_is_update_in_progress_by_pid_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: MagicMock())
    assert mod.is_update_in_progress_by_pid(1) is True


def test_is_update_in_progress_by_pid_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    assert mod.is_update_in_progress_by_pid(1) is False


def test_is_update_in_progress_by_pid_exception_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(_pid: int) -> object:
        raise RuntimeError("x")

    monkeypatch.setattr(mod, "find_progress_window", _raise)
    assert mod.is_update_in_progress_by_pid(1) is True


# ---------------------------------------------------------------------------
# find_start_button / open_dialog_if_needed / _is_offscreen / click_start
# ---------------------------------------------------------------------------
def test_find_start_button_found() -> None:
    target = _mk_element(automation_id=mod.START_BUTTON_AUTO_ID)
    other = _mk_element(automation_id="X")
    main = _mk_element(descendants=[other, target])
    assert mod.find_start_button(main) is target


def test_find_start_button_not_found() -> None:
    main = _mk_element(descendants=[_mk_element(automation_id="X")])
    assert mod.find_start_button(main) is None


def test_find_start_button_skip_exception() -> None:
    bad = MagicMock()
    bad.element_info = MagicMock()
    type(bad.element_info).automation_id = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    bad.element_info.control_type = "Button"
    target = _mk_element(automation_id=mod.START_BUTTON_AUTO_ID)
    main = MagicMock()
    main.descendants.return_value = [bad, target]
    assert mod.find_start_button(main) is target


def test_is_offscreen_true() -> None:
    e = _mk_element(is_offscreen=True)
    assert mod._is_offscreen(e) is True


def test_is_offscreen_false() -> None:
    e = _mk_element(is_offscreen=False)
    assert mod._is_offscreen(e) is False


def test_is_offscreen_exception_returns_false() -> None:
    e = MagicMock()
    type(e).element_info = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert mod._is_offscreen(e) is False


def test_open_dialog_if_needed_already_open(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    monkeypatch.setattr(mod, "find_start_button", lambda _w: MagicMock())
    mod.open_dialog_if_needed(main)
    main.set_focus.assert_not_called()


def test_open_dialog_if_needed_opens_via_menu(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    data_menu = MagicMock()
    main.child_window.return_value = data_menu
    visible_reg = _mk_element(name=mod.NORMAL_REG_MENU_TITLE, is_offscreen=False)
    main.descendants.return_value = [visible_reg]
    # 最初は None (未展開) → 2 度目で発見させる
    btns = iter([None, MagicMock()])
    monkeypatch.setattr(mod, "find_start_button", lambda _w: next(btns))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    mod.open_dialog_if_needed(main)
    data_menu.click_input.assert_called_once()
    visible_reg.click_input.assert_called_once()


def test_open_dialog_if_needed_fallback_to_offscreen(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.child_window.return_value = MagicMock()
    offscreen = _mk_element(name=mod.NORMAL_REG_MENU_TITLE, is_offscreen=True)
    main.descendants.return_value = [offscreen]
    btns = iter([None, MagicMock()])
    monkeypatch.setattr(mod, "find_start_button", lambda _w: next(btns))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    mod.open_dialog_if_needed(main)
    offscreen.click_input.assert_called_once()


def test_open_dialog_if_needed_no_menu_item(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.child_window.return_value = MagicMock()
    main.descendants.return_value = []
    monkeypatch.setattr(mod, "find_start_button", lambda _w: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError):
        mod.open_dialog_if_needed(main)


def test_open_dialog_if_needed_start_button_missing_after_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.child_window.return_value = MagicMock()
    visible = _mk_element(name=mod.NORMAL_REG_MENU_TITLE)
    main.descendants.return_value = [visible]
    monkeypatch.setattr(mod, "find_start_button", lambda _w: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError):
        mod.open_dialog_if_needed(main)


def test_click_start_enabled() -> None:
    btn = _mk_element(is_enabled=True)
    main = MagicMock()
    with patch.object(mod, "find_start_button", return_value=btn):
        assert mod.click_start(main) is True
    btn.click_input.assert_called_once()


def test_click_start_dry_run() -> None:
    btn = _mk_element(is_enabled=True)
    main = MagicMock()
    with patch.object(mod, "find_start_button", return_value=btn):
        assert mod.click_start(main, dry_run=True) is True
    btn.click_input.assert_not_called()


def test_click_start_disabled() -> None:
    btn = _mk_element(is_enabled=False)
    main = MagicMock()
    with patch.object(mod, "find_start_button", return_value=btn):
        assert mod.click_start(main) is False
    btn.click_input.assert_not_called()


def test_click_start_no_button() -> None:
    main = MagicMock()
    with patch.object(mod, "find_start_button", return_value=None):
        with pytest.raises(RuntimeError):
            mod.click_start(main)


# ---------------------------------------------------------------------------
# is_update_in_progress / safe_close_app
# ---------------------------------------------------------------------------
def test_is_update_in_progress_progress_window(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: MagicMock())
    assert mod.is_update_in_progress(main) is True


def test_is_update_in_progress_progress_window_search_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    type(main.element_info).process_id = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    assert mod.is_update_in_progress(main) is True


def test_is_update_in_progress_button_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    btn = _mk_element(is_enabled=False)
    monkeypatch.setattr(mod, "find_start_button", lambda _w: btn)
    assert mod.is_update_in_progress(main) is True


def test_is_update_in_progress_button_enabled_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    btn = _mk_element(is_enabled=True)
    monkeypatch.setattr(mod, "find_start_button", lambda _w: btn)
    assert mod.is_update_in_progress(main) is False


def test_is_update_in_progress_button_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    monkeypatch.setattr(mod, "find_start_button", lambda _w: None)
    assert mod.is_update_in_progress(main) is True


def test_is_update_in_progress_find_start_button_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)

    def _raise(_w: MagicMock) -> object:
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "find_start_button", _raise)
    assert mod.is_update_in_progress(main) is True


def test_is_update_in_progress_is_enabled_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    monkeypatch.setattr(mod, "find_progress_window", lambda _pid: None)
    btn = MagicMock()
    btn.is_enabled.side_effect = RuntimeError("boom")
    monkeypatch.setattr(mod, "find_start_button", lambda _w: btn)
    assert mod.is_update_in_progress(main) is True


def test_safe_close_app_idle_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    monkeypatch.setattr(mod, "is_update_in_progress", lambda _w: False)
    assert mod.safe_close_app(main) is True
    main.close.assert_called_once()


def test_safe_close_app_busy_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    monkeypatch.setattr(mod, "is_update_in_progress", lambda _w: True)
    assert mod.safe_close_app(main) is False
    main.close.assert_not_called()


def test_safe_close_app_close_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.close.side_effect = RuntimeError("boom")
    monkeypatch.setattr(mod, "is_update_in_progress", lambda _w: False)
    assert mod.safe_close_app(main) is False


# ---------------------------------------------------------------------------
# wait_for_completion / _dismiss_popups
# ---------------------------------------------------------------------------
def test_wait_for_completion_success_via_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    btn_states = [
        _mk_element(is_enabled=False),  # 進行中
        _mk_element(is_enabled=False),
        _mk_element(is_enabled=True),  # 完了
    ]
    monkeypatch.setattr(mod, "find_start_button", MagicMock(side_effect=btn_states))
    monkeypatch.setattr(mod, "_dismiss_popups", lambda _w: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.wait_for_completion(main, max_minutes=5, poll_sec=1) is True


def test_wait_for_completion_button_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    monkeypatch.setattr(mod, "find_start_button", lambda _w: None)
    monkeypatch.setattr(mod, "_dismiss_popups", lambda _w: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.wait_for_completion(main, max_minutes=5, poll_sec=1) is True


def test_wait_for_completion_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    btn = _mk_element(is_enabled=False)
    monkeypatch.setattr(mod, "find_start_button", lambda _w: btn)
    monkeypatch.setattr(mod, "_dismiss_popups", lambda _w: None)
    # time.time が即座に deadline を越えるよう細工
    fake_times = itertools.chain([0.0, 1.0, 1.0], itertools.repeat(100000.0))
    monkeypatch.setattr(mod.time, "time", lambda: next(fake_times))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.wait_for_completion(main, max_minutes=1, poll_sec=1) is False


def test_wait_for_completion_is_enabled_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1 回目: is_enabled が例外 → enabled=True と解釈、started=False のため継続
    2 回目: started=False & enabled=True → started=False のまま (initially not started case)
            ※コードロジック: not started and not enabled → started=True 設定
            ここでは enabled=True なので started に変化なし → continue ループ
    3 回目: btn=None で 完了とみなす
    """
    main = MagicMock()
    btn_raise = MagicMock()
    btn_raise.is_enabled.side_effect = RuntimeError("boom")
    seq: list[MagicMock | None] = [btn_raise, _mk_element(is_enabled=True), None]
    monkeypatch.setattr(mod, "find_start_button", MagicMock(side_effect=seq))
    monkeypatch.setattr(mod, "_dismiss_popups", lambda _w: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.wait_for_completion(main, max_minutes=5, poll_sec=1) is True


def test_dismiss_popups_clicks_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999

    popup = MagicMock()
    popup.element_info.process_id = 1
    popup.element_info.handle = 111
    popup.window_text.return_value = "完了"
    ok_btn = _mk_element(is_enabled=True, exists=True)
    popup.child_window.return_value = ok_btn

    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [main, popup]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    mod._dismiss_popups(main)
    ok_btn.click_input.assert_called_once()


def test_dismiss_popups_skip_other_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999
    other = MagicMock()
    other.element_info.process_id = 2
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [other]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    mod._dismiss_popups(main)  # 何も起きず終了


def test_dismiss_popups_skip_same_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999
    same = MagicMock()
    same.element_info.process_id = 1
    same.element_info.handle = 999
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [same]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    mod._dismiss_popups(main)


def test_dismiss_popups_process_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    main = MagicMock()
    type(main.element_info).process_id = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    mod._dismiss_popups(main)


def test_dismiss_popups_button_label_loop_no_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999
    popup = MagicMock()
    popup.element_info.process_id = 1
    popup.element_info.handle = 111
    popup.window_text.return_value = ""
    nofound = _mk_element(exists=False)
    popup.child_window.return_value = nofound
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [popup]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    mod._dismiss_popups(main)


# ---------------------------------------------------------------------------
# parse_args / main
# ---------------------------------------------------------------------------
def test_parse_args_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    a = mod.parse_args()
    assert a.wait is False
    assert a.dry_run is False
    assert a.close_when_done is False
    assert a.lock_stale_min == 180


def test_parse_args_all_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--wait", "--wait-minutes", "5", "--close-when-done", "--dry-run"],
    )
    a = mod.parse_args()
    assert a.wait is True
    assert a.wait_minutes == 5
    assert a.close_when_done is True
    assert a.dry_run is True


def test_main_lock_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: False)
    assert mod.main() == 2


def test_main_in_progress_short_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: True)
    assert mod.main() == 0


def test_main_full_path_no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)
    monkeypatch.setattr(
        mod, "connect_main", lambda *_a, **_k: (MagicMock(), MagicMock())
    )
    monkeypatch.setattr(mod, "open_dialog_if_needed", lambda _w: None)
    monkeypatch.setattr(mod, "click_start", lambda _w, dry_run=False: True)
    assert mod.main() == 0


def test_main_connect_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    # in_progress 判定は 1 回目 False (短絡しない) → 接続失敗時の再短絡も False
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)
    seq = [
        mod.ElementNotFoundError("x"),
        (MagicMock(), MagicMock()),
    ]

    def _cm(*_a: object, **_k: object) -> object:
        v = seq.pop(0)
        if isinstance(v, Exception):
            raise v
        return v

    monkeypatch.setattr(mod, "connect_main", _cm)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "open_dialog_if_needed", lambda _w: None)
    monkeypatch.setattr(mod, "click_start", lambda _w, dry_run=False: True)
    assert mod.main() == 0


def test_main_connect_fails_then_in_progress_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    progress_states = iter([False, True])  # 初回 False (進む) → 接続失敗中に True
    monkeypatch.setattr(
        mod, "is_update_in_progress_by_pid", lambda _pid: next(progress_states)
    )

    def _cm(*_a: object, **_k: object) -> object:
        raise mod.ElementNotFoundError("x")

    monkeypatch.setattr(mod, "connect_main", _cm)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.main() == 0


def test_main_connect_exhausts_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)

    def _cm(*_a: object, **_k: object) -> object:
        raise mod.ElementNotFoundError("x")

    monkeypatch.setattr(mod, "connect_main", _cm)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert mod.main() == 1  # RuntimeError 経由


def test_main_wait_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "--wait", "--close-when-done"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)
    monkeypatch.setattr(
        mod, "connect_main", lambda *_a, **_k: (MagicMock(), MagicMock())
    )
    monkeypatch.setattr(mod, "open_dialog_if_needed", lambda _w: None)
    monkeypatch.setattr(mod, "click_start", lambda _w, dry_run=False: True)
    monkeypatch.setattr(mod, "wait_for_completion", lambda _w, max_minutes: True)
    safe_close = MagicMock(return_value=True)
    monkeypatch.setattr(mod, "safe_close_app", safe_close)
    assert mod.main() == 0
    safe_close.assert_called_once()


def test_main_wait_timeout_skips_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "--wait", "--close-when-done"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)
    monkeypatch.setattr(
        mod, "connect_main", lambda *_a, **_k: (MagicMock(), MagicMock())
    )
    monkeypatch.setattr(mod, "open_dialog_if_needed", lambda _w: None)
    monkeypatch.setattr(mod, "click_start", lambda _w, dry_run=False: True)
    monkeypatch.setattr(mod, "wait_for_completion", lambda _w, max_minutes: False)
    safe_close = MagicMock()
    monkeypatch.setattr(mod, "safe_close_app", safe_close)
    assert mod.main() == 0
    safe_close.assert_not_called()


def test_main_wait_without_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """--wait あり / --close-when-done なし → safe_close_app は呼ばれない。"""
    monkeypatch.setattr(sys, "argv", ["prog", "--wait"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)
    monkeypatch.setattr(mod, "ensure_app_running", lambda: 42)
    monkeypatch.setattr(mod, "is_update_in_progress_by_pid", lambda _pid: False)
    monkeypatch.setattr(
        mod, "connect_main", lambda *_a, **_k: (MagicMock(), MagicMock())
    )
    monkeypatch.setattr(mod, "open_dialog_if_needed", lambda _w: None)
    monkeypatch.setattr(mod, "click_start", lambda _w, dry_run=False: True)
    monkeypatch.setattr(mod, "wait_for_completion", lambda _w, max_minutes: True)
    safe_close = MagicMock()
    monkeypatch.setattr(mod, "safe_close_app", safe_close)
    assert mod.main() == 0
    safe_close.assert_not_called()


def test_main_unexpected_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(mod, "acquire_lock", lambda _stale: True)
    monkeypatch.setattr(mod, "release_lock", lambda: None)

    def _boom() -> object:
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "ensure_app_running", _boom)
    assert mod.main() == 1


# ---------------------------------------------------------------------------
# 追加: 例外ハンドラ / 早期 continue 等の枝
# ---------------------------------------------------------------------------
def test_purge_old_logs_unlink_oserror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mod.LOG_DIR.mkdir(parents=True, exist_ok=True)
    old = mod.LOG_DIR / "old.log"
    old.write_text("x")
    import os as _os

    sixty_days = time.time() - 60 * 86400
    _os.utime(old, (sixty_days, sixty_days))

    original_unlink = Path.unlink

    def _raise_unlink(self: Path, missing_ok: bool = False) -> None:
        if self == old:
            raise OSError("denied")
        original_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", _raise_unlink)
    # 例外を漏らさず completion
    mod.purge_old_logs()


def test_release_lock_oserror_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    mod.LOCK_FILE.write_text("x")

    def _raise(_self: Path, *_a: object, **_k: object) -> None:
        raise OSError("denied")

    monkeypatch.setattr(Path, "unlink", _raise)
    mod.release_lock()  # 例外を漏らさない


def test_find_progress_window_title_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    # 同じ pid だが title が違う → continue
    win = _mk_window_with(pid=1, title="他のWindow", buttons=[], has_progress=False)
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [win]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    assert mod.find_progress_window(1) is None


def test_dismiss_popups_child_window_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """内側 try (label loop) で child_window 取得が例外 → 次の label へ"""
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999

    popup = MagicMock()
    popup.element_info.process_id = 1
    popup.element_info.handle = 111
    popup.window_text.return_value = ""

    # 1 回目 (OK) は例外、2 回目 (はい) は exists=True → click される
    ok_failing = MagicMock()
    ok_failing.exists.side_effect = RuntimeError("boom")
    hai_btn = _mk_element(is_enabled=True, exists=True)

    popup.child_window.side_effect = [ok_failing, hai_btn, MagicMock()]

    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [popup]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    mod._dismiss_popups(main)
    hai_btn.click_input.assert_called_once()


def test_dismiss_popups_outer_loop_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """外側 try で process_id 比較が例外 → 次の Window へ"""
    main = MagicMock()
    main.element_info.process_id = 1
    main.element_info.handle = 999

    bad = MagicMock()
    type(bad.element_info).process_id = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    fake_desktop = MagicMock()
    fake_desktop.windows.return_value = [bad]
    monkeypatch.setattr(mod, "Desktop", MagicMock(return_value=fake_desktop))
    mod._dismiss_popups(main)
