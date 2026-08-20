from pathlib import Path

class ArrowSchema:
    names: list[str]

class ParquetFile:
    schema_arrow: ArrowSchema
    def __init__(self, source: str | Path) -> None: ...

def write_table(table: object, where: str | Path) -> None: ...
