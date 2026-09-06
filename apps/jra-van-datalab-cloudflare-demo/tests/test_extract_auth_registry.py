from pathlib import Path

from pytest import CaptureFixture

from scripts.extract_auth_registry import extract_auth_registry, main


def test_extract_auth_registry_keeps_only_uid_subtree(tmp_path: Path) -> None:
    source = tmp_path / "system.reg"
    destination = tmp_path / "auth.reg"
    source.write_text(
        "WINE REGISTRY Version 2\n"
        "[Software\\\\Other] 1\n"
        '"secret"="excluded"\n'
        "[Software\\\\JRA-VAN Data Lab.\\\\server_info] 2\n"
        '"serverhost"="included"\n'
        "[Software\\\\JRA-VAN Data Lab.\\\\uid_pass] 2\n"
        '"ukey"="included"\n'
        '"messagekey"=hex(1):41,00,\\\n'
        "  42,00,00,00\n"
        "[Software\\\\JRA-VAN Data Lab.\\\\uid_pass\\\\WebBrowser] 3\n"
        '"browser"="included"\n',
        encoding="utf-8",
    )

    extract_auth_registry(source, destination)

    assert destination.read_text(encoding="utf-8") == (
        "REGEDIT4\n\n"
        "[HKEY_LOCAL_MACHINE\\Software\\JRA-VAN Data Lab.\\server_info]\n"
        '"serverhost"="included"\n'
        "[HKEY_LOCAL_MACHINE\\Software\\JRA-VAN Data Lab.\\uid_pass]\n"
        '"ukey"="included"\n'
        '"messagekey"="AB"\n'
        "[HKEY_LOCAL_MACHINE\\Software\\JRA-VAN Data Lab.\\uid_pass\\WebBrowser]\n"
        '"browser"="included"\n'
        "[-HKEY_LOCAL_MACHINE\\System\\ControlSet001\\Services\\JVLink64Agent]\n\n"
        "[HKEY_LOCAL_MACHINE\\System\\ControlSet001\\Services\\JVLink64Agent]\n"
        '"ErrorControl"=dword:00000001\n'
        '"ImagePath"="\\"C:\\\\Program Files\\\\JRA-VAN\\\\Data Lab\\\\'
        'JVLink64Agent.exe\\" -Service"\n'
        '"ObjectName"="LocalSystem"\n'
        '"PreshutdownTimeout"=dword:0002bf20\n'
        '"Start"=dword:00000003\n'
        '"Type"=dword:00000010\n'
    )


def test_main_rejects_missing_paths(capsys: CaptureFixture[str]) -> None:
    assert main([]) == 2
    assert "Usage:" in capsys.readouterr().err
