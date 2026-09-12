"""Fail-closed identity supersession from explicit NVD source revisions.

Never select a runner because its finish or odds is nonzero. Raw source files
remain immutable; callers apply the returned identity ledger to a separate copy.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import chain

SourceKey = tuple[str, str, str, str, str]


@dataclass(frozen=True)
class Revision:
    key: SourceKey
    registry: str
    name: str
    kind: str
    created: date
    facts: tuple[str, str, str]


@dataclass(frozen=True)
class Supersession:
    key: SourceKey
    superseded_registry: str
    current_registry: str
    name: str
    old_created: str
    current_created: str
    reason: str


def _text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing source revision field: {name}")
    return value.strip()


def _revision(row: Mapping[str, object]) -> Revision:
    registry = _text(row, "ketto_toroku_bango")
    if re.fullmatch(r"[0-9]{10}", registry) is None:
        raise ValueError("Invalid source registry")
    created = _text(row, "data_sakusei_nengappi")
    if re.fullmatch(r"[0-9]{8}", created) is None:
        raise ValueError("Invalid source revision date")
    return Revision(
        (
            _text(row, "kaisai_nen"),
            _text(row, "kaisai_tsukihi"),
            _text(row, "keibajo_code"),
            _text(row, "race_bango"),
            _text(row, "umaban"),
        ),
        registry,
        _text(row, "bamei"),
        _text(row, "data_kubun"),
        date.fromisoformat(created),
        (_text(row, "kakutei_chakujun"), _text(row, "ijo_kubun_code"), _text(row, "tansho_odds")),
    )


def _resolve_group(rows: Sequence[Revision]) -> list[Supersession]:
    if len(rows) == 1:
        return []
    finals = [row for row in rows if row.kind == "7" and row.registry != "0000000000"]
    if len(finals) != 1:
        raise ValueError("Ambiguous final source identity")
    current = finals[0]
    results: list[Supersession] = []
    for row in rows:
        if row is current:
            continue
        if (
            row.registry == current.registry
            or row.name != current.name
            or row.created > current.created
        ):
            raise ValueError("Conflicting identity revision")
        if row.kind == "2":
            reason = "preliminary_identity_superseded_by_final"
        elif row.kind == "7" and row.registry == "0000000000" and row.facts == current.facts:
            reason = "placeholder_registry_corrected"
        else:
            raise ValueError("Unsupported or conflicting source supersession")
        results.append(
            Supersession(
                row.key,
                row.registry,
                current.registry,
                current.name,
                row.created.isoformat(),
                current.created.isoformat(),
                reason,
            )
        )
    return results


def resolve_source_revisions(rows: Sequence[Mapping[str, object]]) -> list[Supersession]:
    """Resolve only documented 2→7 transitions or matching zero-ID corrections.

    Multiple valid final identities, name disagreements, backward revision dates,
    and conflicting placeholder facts all remain hard failures. This does not
    establish availability at an earlier prediction timestamp.
    """
    groups: dict[SourceKey, list[Revision]] = {}
    for row in rows:
        revision = _revision(row)
        groups.setdefault(revision.key, []).append(revision)
    return list(chain.from_iterable(_resolve_group(group) for group in groups.values()))
