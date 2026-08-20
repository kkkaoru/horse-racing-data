"""Time built-in day-base + RACE_CHAIN without touching tonight's feat-jra-*.

Calls pipeline_runner.build_day_base and build_pipeline_from_day_base after
rebinding WORK_DIR to /tmp/fp-builtin-split. No Neon write, no R2 put/get.

Host-only: rebind COURSE_LOOKUP_PATH to the repo lookup parquet. Production
code is not modified.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
import traceback
from pathlib import Path

REPO = Path("/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data")
SAFE_WORK = Path("/tmp/fp-builtin-split")
PRESERVE_ROOT = Path("/tmp/predict-upcoming")
SRC = REPO / "apps/finish-position-predict-container/src"
LOG = REPO / "docs/probes/finish-position-recovery-20260816/measure-builtin-split-0401.log"
RESULT = REPO / "docs/probes/finish-position-recovery-20260816/measure-builtin-split-0401.md"
HOST_COURSE_LOOKUP = (
    REPO / "apps/pc-keiba-viewer/finish-position/lookups/course-numerical-features.parquet"
)
RESUME_FROM = Path("/tmp/fp-builtin-split/daybase-jra-20260816/layer-7")

os.environ.setdefault("PIPELINE_DIR", str(REPO / "apps/pc-keiba-viewer/src/scripts"))
os.environ["PIPELINE_FORCE_MEMORY_GB"] = "8"
os.environ["PIPELINE_FORCE_THREADS"] = "4"
os.environ["PREDICT_DEBUG_LOGS"] = "1"
os.environ.setdefault(
    "R2_CATALOG_URI",
    "https://catalog.cloudflarestorage.com/78109ec18c7c85b194b19fb32e3bb149/pc-keiba-r2-catalog",
)
os.environ.setdefault(
    "R2_CATALOG_WAREHOUSE",
    "78109ec18c7c85b194b19fb32e3bb149_pc-keiba-r2-catalog",
)
if not os.environ.get("R2_CATALOG_TOKEN") and os.environ.get("CLOUDFLARE_DEBUG_TOKEN"):
    os.environ["R2_CATALOG_TOKEN"] = os.environ["CLOUDFLARE_DEBUG_TOKEN"]

sys.path.insert(0, str(SRC))

from predict_lib import pipeline_args  # noqa: E402
import pipeline_runner  # noqa: E402
from predict_lib.pipeline_args import day_chain_for, race_chain_for  # noqa: E402


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def feat_jra_fingerprint() -> list[tuple[str, int, int]]:
    rows: list[tuple[str, int, int]] = []
    for path in sorted(PRESERVE_ROOT.glob("feat-jra-*")):
        files = list(path.rglob("*.parquet"))
        size = sum(file.stat().st_size for file in files)
        mtime_ns = max((file.stat().st_mtime_ns for file in files), default=0)
        rows.append((path.name, size, mtime_ns))
    return rows


def assert_preserved(before: list[tuple[str, int, int]], label: str) -> None:
    after = feat_jra_fingerprint()
    if after != before:
        raise RuntimeError(f"feat-jra-* changed during {label}: {before} -> {after}")


def patch_host_lookup() -> None:
    if not HOST_COURSE_LOOKUP.is_file():
        raise FileNotFoundError(HOST_COURSE_LOOKUP)
    pipeline_args.COURSE_LOOKUP_PATH = HOST_COURSE_LOOKUP
    log(f"host_lookup {HOST_COURSE_LOOKUP}")


def write_result(
    status: str,
    day_elapsed: float,
    race_elapsed: float,
    race_ok: bool,
    day_base_dir: Path | None,
) -> None:
    RESULT.write_text(
        "\n".join(
            [
                "# Built-in DAY / RACE split timing (2026-08-16)",
                "",
                f"- status: `{status}`",
                f"- day_base_seconds: `{day_elapsed:.1f}`",
                f"- race_chain_seconds: `{race_elapsed:.1f}`",
                f"- race_chain_ok: `{race_ok}`",
                f"- day_base_dir: `{day_base_dir}`",
                f"- work_dir: `{SAFE_WORK}`",
                f"- host_course_lookup: `{HOST_COURSE_LOOKUP}`",
                "- preserved `/tmp/predict-upcoming/feat-jra-*`: yes (asserted)",
                "- Neon write: no (`record_layer_timing_row` stubbed; catalog URL)",
                "- R2: no (`r2_config=None`)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    log(f"wrote {RESULT} status={status}")


def finish_remaining_day_layers(current: Path, before: list[tuple[str, int, int]]) -> Path:
    """Run DAY_CHAIN scripts 9–12 from layer-7, then rename to final."""
    day_dir = SAFE_WORK / "daybase-jra-20260816"
    chain = list(day_chain_for("jra"))
    remaining = chain[8:]  # course-numerical, kohan3f, similar-race, sire-venue
    log(f"resume remaining_day n={len(remaining)} from={current}")
    for offset, script in enumerate(remaining, start=9):
        nxt = day_dir / f"layer-{offset - 1}"
        shutil.rmtree(nxt, ignore_errors=True)
        t0 = time.perf_counter()
        log(f"start remaining-day {offset}/12 {script}")
        pipeline_runner.run_with_stderr_capture(
            pipeline_args.build_layer_argv(
                script,
                "jra",
                pipeline_runner.LAYER_DIR,
                current,
                nxt,
                "r2-catalog://pc-keiba",
                target_date="20260816",
                target_race=None,
            )
        )
        elapsed = time.perf_counter() - t0
        log(f"done remaining-day {offset}/12 {script} elapsed_s={elapsed:.3f}")
        current = nxt
        assert_preserved(before, f"remaining-day-{offset}")
    final_dir = day_dir / "final"
    shutil.rmtree(final_dir, ignore_errors=True)
    current.rename(final_dir)
    log(f"daybase-final {final_dir}")
    return final_dir


def main() -> int:
    SAFE_WORK.mkdir(parents=True, exist_ok=True)
    before = feat_jra_fingerprint()
    log(f"preserve_start n={len(before)} names={[name for name, _, _ in before]}")
    if len(before) != 19:
        log(f"WARN expected 19 feat-jra dirs, got {len(before)}")

    pipeline_runner.WORK_DIR = SAFE_WORK
    pipeline_runner.record_layer_timing_row = (  # type: ignore[method-assign]
        lambda *args, **kwargs: None
    )
    patch_host_lookup()

    day_base_dir: Path | None = None
    day_elapsed = 0.0
    race_elapsed = 0.0
    race_ok = False
    status = "failed"

    try:
        if RESUME_FROM.exists() and any(RESUME_FROM.rglob("*.parquet")):
            log(f"resume from existing {RESUME_FROM}")
            t0 = time.perf_counter()
            day_base_dir = finish_remaining_day_layers(RESUME_FROM, before)
            remaining_elapsed = time.perf_counter() - t0
            day_elapsed = 1117.8 + remaining_elapsed
            log(
                f"done remaining-day elapsed_s={remaining_elapsed:.1f} "
                f"approx_total_day_s={day_elapsed:.1f}"
            )
        else:
            log("start build_day_base jra 20260816")
            t0 = time.perf_counter()
            day_base_dir = pipeline_runner.build_day_base(
                "jra",
                "20260816",
                0,
                "r2-catalog://pc-keiba",
                realtime_odds_path=None,
                venue_weather_dir=None,
                r2_config=None,
            )
            day_elapsed = time.perf_counter() - t0
            log(f"done build_day_base elapsed_s={day_elapsed:.1f} dir={day_base_dir}")
        assert_preserved(before, "day-base")
        if day_base_dir is None:
            log("FAIL day-base returned None")
            status = "day_base_empty"
            write_result(status, day_elapsed, race_elapsed, race_ok, day_base_dir)
            return 1
        log("start build_pipeline_from_day_base target=04:01")
        final_dir = SAFE_WORK / "feat-jra-v7-final"
        t1 = time.perf_counter()
        race_ok = pipeline_runner.build_pipeline_from_day_base(
            "jra",
            "20260816",
            0,
            "r2-catalog://pc-keiba",
            day_base_dir,
            final_dir,
            "04:01",
            realtime_odds_path=None,
            venue_weather_dir=None,
        )
        race_elapsed = time.perf_counter() - t1
        log(
            f"done build_pipeline_from_day_base ok={race_ok} "
            f"elapsed_s={race_elapsed:.1f} final={final_dir} "
            f"race_scripts={list(race_chain_for('jra'))}"
        )
        assert_preserved(before, "build_pipeline_from_day_base")
        status = "ok" if race_ok else "racechain_false"
        write_result(status, day_elapsed, race_elapsed, race_ok, day_base_dir)
        return 0 if status == "ok" else 1
    except Exception:
        log("EXC\n" + traceback.format_exc())
        try:
            assert_preserved(before, "exception-path")
        except Exception as preserve_exc:
            log(f"PRESERVE_BROKEN {preserve_exc}")
        write_result(status, day_elapsed, race_elapsed, race_ok, day_base_dir)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
