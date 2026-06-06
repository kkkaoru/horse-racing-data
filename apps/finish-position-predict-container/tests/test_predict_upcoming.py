"""Tests for the small pure helpers in ``predict_upcoming.py``.

``predict_upcoming.py`` itself is the I/O orchestration entrypoint (Neon TCP,
R2, DuckDB subprocess, native CatBoost / XGBoost load) and is excluded from
the ``--cov=predict_lib`` coverage gate per ``pyproject.toml``. The
per-category class-code extractor is structurally pure though — Phase F adds
NAR routing on top of the existing JRA contract, so a unit test pins the
per-category column-name dispatch.

Tests here run alongside the predict_lib suite but do NOT count towards the
predict_lib coverage threshold; they are exclusively a correctness check for
the small extractor.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Import the private helper directly so the test stays I/O-free; the rest of
# predict_upcoming requires Neon credentials + the DuckDB subprocess to run.
from predict_upcoming import extract_race_class_code


def testextract_race_class_code_jra_returns_kyoso_joken_code() -> None:
    entries = [
        {"kyoso_joken_code": "005", "umaban": 1},
        {"kyoso_joken_code": "005", "umaban": 2},
    ]
    assert extract_race_class_code("jra", entries) == "005"


def testextract_race_class_code_jra_returns_none_when_missing() -> None:
    # JRA entry without the kyoso_joken_code field -> None.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_returns_none_for_empty_string() -> None:
    # PG returns the empty string for some legacy races; we collapse it to
    # None so the per-class router falls back to iter14.
    entries = [{"kyoso_joken_code": "  ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_strips_whitespace() -> None:
    entries = [{"kyoso_joken_code": " 703 ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"


def testextract_race_class_code_nar_returns_nar_subclass() -> None:
    # Phase F: NAR reads ``nar_subclass`` rather than ``kyoso_joken_code``.
    entries = [
        {"nar_subclass": "NEW", "umaban": 1},
        {"nar_subclass": "NEW", "umaban": 2},
    ]
    assert extract_race_class_code("nar", entries) == "NEW"


def testextract_race_class_code_nar_returns_each_named_subclass() -> None:
    # All six NAR sub-classes pass through verbatim — the extractor reads from
    # the first entry only since all entries of a race share the same class.
    for subclass in ("NEW", "MUKATSU", "C", "B", "A", "OP"):
        entries = [{"nar_subclass": subclass, "umaban": 1}]
        assert extract_race_class_code("nar", entries) == subclass


def testextract_race_class_code_nar_returns_other_when_no_meisho_match() -> None:
    # The DuckDB build emits ``"other"`` literal when nothing matches the
    # nar_subclass_case_sql regex — pass it through verbatim so the resolver
    # routes to the NAR ``other`` ensemble.
    entries = [{"nar_subclass": "other", "umaban": 1}]
    assert extract_race_class_code("nar", entries) == "other"


def testextract_race_class_code_nar_returns_none_when_field_absent() -> None:
    # A NAR entry without the nar_subclass field collapses to None — the
    # router then falls back to the NAR iter 12 baseline.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_nar_returns_none_when_field_null() -> None:
    entries = [{"nar_subclass": None, "umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_banei_returns_none_by_dispatch() -> None:
    # Ban-ei is NOT in the per-category dispatch map — the extractor returns
    # None regardless of whether the columns exist so the resolver
    # short-circuits to the Ban-ei category-global model.
    entries = [
        {"kyoso_joken_code": "BAN", "nar_subclass": "other", "umaban": 1},
    ]
    assert extract_race_class_code("ban-ei", entries) is None


def testextract_race_class_code_returns_none_for_empty_entries() -> None:
    # No entries -> nothing to read; safe None.
    assert extract_race_class_code("jra", []) is None
    assert extract_race_class_code("nar", []) is None


def testextract_race_class_code_coerces_non_string_value() -> None:
    # Defensive: numeric class code (DuckDB sometimes emits int for the JRA
    # numeric codes) is str-coerced before strip + return so the resolver
    # sees a clean string key.
    entries = [{"kyoso_joken_code": 703, "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"
