"""pywinauto は Windows ランタイム専用で macOS / Linux に installable でないため、
テスト実行前に sys.modules へ最小限の stub を注入してから本体スクリプトを import 可能にする。
"""

from __future__ import annotations

import os
import sys
import tempfile
import types

# 本体スクリプトは APPDATA/TEMP/LOCALAPPDATA を module import 時に参照するため、
# Windows 以外のホストでも import できるよう dummy 値を流し込む。
_tmp_root = tempfile.gettempdir()
os.environ.setdefault("APPDATA", _tmp_root)
os.environ.setdefault("TEMP", _tmp_root)
os.environ.setdefault("LOCALAPPDATA", _tmp_root)


def _install_pywinauto_stub() -> None:
    if "pywinauto" in sys.modules:
        return

    pywinauto = types.ModuleType("pywinauto")
    findwindows = types.ModuleType("pywinauto.findwindows")
    timings = types.ModuleType("pywinauto.timings")

    class _StubApplication:
        def __init__(self, backend: str | None = None) -> None: ...
        def connect(self, **_: object) -> object: ...
        def window(self, **_: object) -> object: ...

    class _StubDesktop:
        def __init__(self, backend: str | None = None) -> None: ...
        def windows(self) -> list[object]:
            return []

    class _StubElementNotFoundError(Exception):
        pass

    class _StubPwaTimeoutError(Exception):
        pass

    setattr(pywinauto, "Application", _StubApplication)
    setattr(pywinauto, "Desktop", _StubDesktop)
    setattr(findwindows, "ElementNotFoundError", _StubElementNotFoundError)
    setattr(timings, "TimeoutError", _StubPwaTimeoutError)

    sys.modules["pywinauto"] = pywinauto
    sys.modules["pywinauto.findwindows"] = findwindows
    sys.modules["pywinauto.timings"] = timings


_install_pywinauto_stub()
