from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

import cleanup_tmp as subject

SECONDS_PER_DAY = 86400.0


def _make_dir(parent: Path, name: str, *, size: int = 0, age_days: float = 0.0, now: float = 0.0) -> Path:
    target = parent / name
    target.mkdir()
    if size > 0:
        (target / "data.bin").write_bytes(b"x" * size)
    mtime = now - age_days * SECONDS_PER_DAY
    os.utime(target, (mtime, mtime))
    return target


def test_is_disposable_matches_prefix_pattern():
    assert subject.is_disposable("feat-jra-v8-merged") is True


def test_is_disposable_matches_exact_pattern():
    assert subject.is_disposable("bucket-eval") is True


def test_is_disposable_false_for_unmatched_name():
    assert subject.is_disposable("random-output") is False


def test_is_disposable_false_for_protected_keep_name():
    assert subject.is_disposable("feat-jra-v9-weather") is False


def test_is_disposable_false_for_protected_models():
    assert subject.is_disposable("models") is False


def test_prefix_group_strips_version_token():
    assert subject.prefix_group("feat-jra-v9-weather") == "feat-jra"


def test_prefix_group_strips_at_first_numeric_token():
    assert subject.prefix_group("eval-2024-final") == "eval"


def test_prefix_group_returns_full_name_when_no_version_token():
    assert subject.prefix_group("bucket-eval") == "bucket-eval"


def test_prefix_group_returns_name_when_first_token_has_digit():
    assert subject.prefix_group("3way-cache") == "3way-cache"


def test_dir_size_bytes_sums_file_sizes(tmp_path: Path):
    target = tmp_path / "store"
    target.mkdir()
    (target / "a.bin").write_bytes(b"abc")
    (target / "nested").mkdir()
    (target / "nested" / "b.bin").write_bytes(b"de")
    assert subject.dir_size_bytes(target) == 5


def test_dir_size_bytes_skips_symlinked_files(tmp_path: Path):
    target = tmp_path / "store"
    target.mkdir()
    real = tmp_path / "real.bin"
    real.write_bytes(b"abcd")
    (target / "link.bin").symlink_to(real)
    assert subject.dir_size_bytes(target) == 0


def test_scan_disposable_dirs_returns_empty_when_tmp_missing(tmp_path: Path):
    assert subject.scan_disposable_dirs(tmp_path / "absent") == []


def test_scan_disposable_dirs_skips_files(tmp_path: Path):
    (tmp_path / "feat-jra-v1").write_text("not a dir")
    assert subject.scan_disposable_dirs(tmp_path) == []


def test_scan_disposable_dirs_skips_symlinked_dirs(tmp_path: Path):
    real = tmp_path / "elsewhere"
    real.mkdir()
    (tmp_path / "feat-jra-v1-link").symlink_to(real, target_is_directory=True)
    assert subject.scan_disposable_dirs(tmp_path) == []


def test_scan_disposable_dirs_skips_non_disposable_dir(tmp_path: Path):
    (tmp_path / "keep-me").mkdir()
    assert subject.scan_disposable_dirs(tmp_path) == []


def test_scan_disposable_dirs_collects_entry_metadata(tmp_path: Path):
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=2.0, now=1_000_000.0)
    entries = subject.scan_disposable_dirs(tmp_path)
    assert len(entries) == 1
    assert entries[0].path.name == "feat-jra-v1"
    assert entries[0].size_bytes == 4
    assert entries[0].group_key == "feat-jra"


def test_select_for_deletion_keeps_newest_per_group():
    entries = [
        subject.DirEntry(Path("tmp/feat-jra-v1"), 10, mtime=100.0, group_key="feat-jra"),
        subject.DirEntry(Path("tmp/feat-jra-v2"), 20, mtime=200.0, group_key="feat-jra"),
    ]
    plan = subject.select_for_deletion(entries, max_age_days=0.0, keep_latest=1, now=1000.0)
    assert [e.path.name for e in plan.delete] == ["feat-jra-v1"]
    assert [e.path.name for e in plan.kept] == ["feat-jra-v2"]


def test_select_for_deletion_keeps_dirs_younger_than_cutoff():
    entries = [
        subject.DirEntry(Path("tmp/feat-jra-v1"), 10, mtime=900.0, group_key="feat-jra"),
        subject.DirEntry(Path("tmp/feat-jra-v2"), 20, mtime=950.0, group_key="feat-jra"),
    ]
    # cutoff = 1 day = 86400s; both within window of now=1000 -> nothing deleted
    plan = subject.select_for_deletion(entries, max_age_days=1.0, keep_latest=0, now=1000.0)
    assert plan.delete == ()
    assert len(plan.kept) == 2


def test_select_for_deletion_deletes_old_unprotected_dir():
    entries = [
        subject.DirEntry(Path("tmp/feat-jra-v1"), 10, mtime=0.0, group_key="feat-jra"),
    ]
    plan = subject.select_for_deletion(
        entries, max_age_days=1.0, keep_latest=0, now=10.0 * SECONDS_PER_DAY,
    )
    assert [e.path.name for e in plan.delete] == ["feat-jra-v1"]
    assert plan.freed_bytes == 10


def test_cleanup_tmp_dry_run_does_not_delete(tmp_path: Path):
    now = 10_000_000.0
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=30.0, now=now)
    result = subject.cleanup_tmp(
        tmp_path, execute=False, max_age_days=1.0, keep_latest=0, now=now,
    )
    assert (tmp_path / "feat-jra-v1").exists()
    assert result.executed is False
    assert result.total_after_bytes == result.total_before_bytes
    assert result.plan.freed_bytes == 4


def test_cleanup_tmp_execute_deletes_and_reports_freed(tmp_path: Path):
    now = 10_000_000.0
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=30.0, now=now)
    _make_dir(tmp_path, "feat-jra-v9-weather", size=8, age_days=30.0, now=now)
    result = subject.cleanup_tmp(
        tmp_path, execute=True, max_age_days=1.0, keep_latest=0, now=now,
    )
    assert not (tmp_path / "feat-jra-v1").exists()
    assert (tmp_path / "feat-jra-v9-weather").exists()
    assert result.executed is True
    assert result.plan.freed_bytes == 4
    assert result.total_after_bytes == result.total_before_bytes - 4


def test_cleanup_tmp_uses_wall_clock_when_now_is_none(tmp_path: Path):
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=0.0, now=time.time())
    result = subject.cleanup_tmp(tmp_path, execute=False, max_age_days=7.0, keep_latest=0)
    # Just-created dir is younger than 7 days -> kept.
    assert result.plan.delete == ()


def test_format_gib_renders_two_decimals():
    assert subject.format_gib(int(1024**3 * 2.5)) == "2.50 GiB"


def test_render_report_dry_run_lists_would_delete(tmp_path: Path):
    now = 10_000_000.0
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=30.0, now=now)
    result = subject.cleanup_tmp(
        tmp_path, execute=False, max_age_days=1.0, keep_latest=0, now=now,
    )
    report = subject.render_report(result)
    assert "Would delete: " in report
    assert "Freed:" in report


def test_render_report_execute_uses_deleted_verb(tmp_path: Path):
    now = 10_000_000.0
    _make_dir(tmp_path, "feat-jra-v1", size=4, age_days=30.0, now=now)
    result = subject.cleanup_tmp(
        tmp_path, execute=True, max_age_days=1.0, keep_latest=0, now=now,
    )
    report = subject.render_report(result)
    assert "Deleted: " in report


def test_render_report_handles_nothing_to_delete(tmp_path: Path):
    result = subject.cleanup_tmp(
        tmp_path, execute=False, max_age_days=1.0, keep_latest=0, now=0.0,
    )
    report = subject.render_report(result)
    assert "(nothing to delete)" in report


def test_parse_args_defaults_to_dry_run():
    args = subject.parse_args([])
    assert args.dry_run is False
    assert args.execute is False
    assert args.tmp_dir == Path("tmp")
    assert args.max_age_days == subject.DEFAULT_MAX_AGE_DAYS
    assert args.keep_latest == subject.DEFAULT_KEEP_LATEST


def test_parse_args_accepts_execute_and_overrides():
    args = subject.parse_args(
        ["--execute", "--max-age-days", "3", "--keep-latest", "2", "--tmp-dir", "/x/tmp"],
    )
    assert args.execute is True
    assert args.max_age_days == 3.0
    assert args.keep_latest == 2
    assert args.tmp_dir == Path("/x/tmp")


def test_parse_args_rejects_dry_run_and_execute_together():
    with pytest.raises(SystemExit):
        subject.parse_args(["--dry-run", "--execute"])


def test_main_dry_run_prints_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    target = tmp_path / "feat-jra-v1"
    target.mkdir()
    (target / "data.bin").write_bytes(b"abcd")
    old = time.time() - 30 * SECONDS_PER_DAY
    os.utime(target, (old, old))
    subject.main(["--tmp-dir", str(tmp_path), "--max-age-days", "1", "--keep-latest", "0"])
    out = capsys.readouterr().out
    assert "Would delete: " in out
    assert target.exists()


def test_main_execute_deletes(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    target = tmp_path / "feat-jra-v1"
    target.mkdir()
    (target / "data.bin").write_bytes(b"abcd")
    old = time.time() - 30 * SECONDS_PER_DAY
    os.utime(target, (old, old))
    subject.main(
        ["--execute", "--tmp-dir", str(tmp_path), "--max-age-days", "1", "--keep-latest", "0"],
    )
    out = capsys.readouterr().out
    assert "Deleted: " in out
    assert not target.exists()
