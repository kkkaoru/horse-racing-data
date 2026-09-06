"""Extract only JV-Link terminal authentication keys from a Wine system registry."""

import sys
from pathlib import Path


def _portable_hex_string(lines: list[str], index: int) -> tuple[str, int]:
    """Convert Wine's internal UTF-16 hex string to portable REGEDIT4 text."""
    name, payload = lines[index].split("=hex(1):", 1)
    while payload.endswith("\\"):
        index += 1
        payload = payload[:-1] + lines[index].strip()
    encoded = bytes(int(value, 16) for value in payload.split(",") if value)
    value = encoded.decode("utf-16-le").removesuffix("\0")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{name}="{escaped}"', index


def extract_auth_registry(source: Path, destination: Path) -> None:
    """Convert JV-Link machine settings to a portable 64-bit REGEDIT4 file."""
    prefixes = (
        r"Software\\JRA-VAN Data Lab.\\server_info",
        r"Software\\JRA-VAN Data Lab.\\uid_pass",
    )
    selected = False
    output = ["REGEDIT4", ""]
    lines = source.read_text(encoding="utf-8").splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("["):
            section = line[1 : line.index("]")]
            selected = any(
                section == prefix or section.startswith(f"{prefix}\\\\") for prefix in prefixes
            )
            if selected:
                windows_section = section.replace("\\\\", "\\")
                output.append(f"[HKEY_LOCAL_MACHINE\\{windows_section}]")
        elif selected and "=hex(1):" in line:
            portable, index = _portable_hex_string(lines, index)
            output.append(portable)
        elif selected and not line.startswith("#"):
            output.append(line)
        index += 1
    output.extend(
        [
            r"[-HKEY_LOCAL_MACHINE\System\ControlSet001\Services\JVLink64Agent]",
            "",
            r"[HKEY_LOCAL_MACHINE\System\ControlSet001\Services\JVLink64Agent]",
            '"ErrorControl"=dword:00000001',
            (
                '"ImagePath"="\\"C:\\\\Program Files\\\\JRA-VAN\\\\Data Lab\\\\'
                'JVLink64Agent.exe\\" -Service"'
            ),
            '"ObjectName"="LocalSystem"',
            '"PreshutdownTimeout"=dword:0002bf20',
            '"Start"=dword:00000003',
            '"Type"=dword:00000010',
        ]
    )
    destination.write_text("\n".join(output) + "\n", encoding="utf-8")


def main(arguments: list[str]) -> int:
    """Run the bounded two-path command line interface."""
    if len(arguments) != 2:
        print("Usage: extract_auth_registry.py SOURCE DESTINATION", file=sys.stderr)
        return 2
    extract_auth_registry(Path(arguments[0]), Path(arguments[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
