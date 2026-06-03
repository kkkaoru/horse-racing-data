"""Tests for the cron-execution audit builder."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.audit import (
    AuditRecord,
    audit_params,
    build_audit_insert_sql,
    build_audit_record,
    build_audit_table_ddl,
)


def test_build_audit_record_success() -> None:
    record = build_audit_record("2026-06-03", "success", 412, 90210, None)
    assert record == AuditRecord("2026-06-03", "success", 412, 90210, None)


def test_build_audit_record_error_carries_message() -> None:
    record = build_audit_record("2026-06-03", "error", 0, 1200, "neon timeout")
    assert record.status == "error"
    assert record.error == "neon timeout"


def test_build_audit_record_rejects_negative_races() -> None:
    with pytest.raises(ValueError, match="races_predicted"):
        build_audit_record("2026-06-03", "success", -1, 10, None)


def test_build_audit_record_rejects_negative_duration() -> None:
    with pytest.raises(ValueError, match="duration_ms"):
        build_audit_record("2026-06-03", "success", 1, -5, None)


def test_build_audit_table_ddl_creates_table() -> None:
    ddl = build_audit_table_ddl()
    assert "create table if not exists finish_position_cron_executions" in ddl


def test_build_audit_insert_sql_placeholders() -> None:
    sql = build_audit_insert_sql()
    assert "$1" in sql
    assert "$5" in sql
    assert "$6" not in sql


def test_audit_params_order() -> None:
    record = build_audit_record("2026-06-03", "partial", 7, 800, "one race failed")
    assert audit_params(record) == ["2026-06-03", "partial", 7, 800, "one race failed"]
