"""Tests for mlflow_tracking.trace_emit.

Entirely hermetic: the `client` fixture (from conftest.py) is a REAL
`MlflowClient` backed by an isolated sqlite tracking store, and every
`TracingClient` used here is built from that SAME store via
`trace_emit.build_tracing_client` -- exercising the real MLflow trace/span/
assessment write path against a throwaway store is correct and intended
(never a mock of `TracingClient` itself, since the whole point of this
module is proving the REAL API behaves as documented).
"""

from __future__ import annotations

import ast
import inspect
import types
from datetime import UTC, datetime

import mlflow
import pytest
from mlflow import MlflowClient
from mlflow.entities import Assessment, Trace
from mlflow.entities.span import SpanType
from mlflow.entities.trace_state import TraceState
from mlflow.exceptions import MlflowException
from mlflow.store.entities.paged_list import PagedList

from mlflow_tracking import serve_eval, trace_emit

GEN_AT: datetime = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)


def _module_name_references(module: types.ModuleType) -> set[str]:
    """Return every bare `Name` identifier referenced anywhere in `module`'s
    source AST -- used to prove a bare module reference (e.g. `mlflow`) is
    never used as code, immune to false positives from docstring prose."""
    tree = ast.parse(inspect.getsource(module))
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _feedback_value(assessment: Assessment) -> object:
    """Narrow `Assessment.feedback` (typed `FeedbackValue | None`) to a
    plain `.value` read -- every assessment `trace_emit` itself logs is
    ALWAYS a `Feedback` (never a bare `Expectation`/`IssueReference`), so
    `.feedback` is always populated in practice; this helper just gives the
    type checker a single, explicit place to see that asserted (a real
    `assert`, never a `# type: ignore` suppression)."""
    feedback = assessment.feedback
    assert feedback is not None
    return feedback.value


def _fp_eval_row(
    *,
    venue: str = "05",
    race_bango: str = "01",
    model_version: str = "iter14",
    top1_hit: int = 1,
    place2_hit: int = 1,
    place3_hit: int = 1,
    place4_hit: int | None = 1,
    place5_hit: int | None = 1,
    place6_hit: int | None = 1,
    top3_box_hit: int = 1,
) -> serve_eval.FpRaceEvalRow:
    return serve_eval.FpRaceEvalRow(
        category="jra",
        race_date="20260614",
        venue=venue,
        race_bango=race_bango,
        model_version=model_version,
        distance_band="sprint",
        field_size_band="medium",
        season_band="summer",
        class_code="A",
        surface="turf",
        predicted_top1_ketto="H1",
        actual_top1_ketto="H1",
        matched_horses=6,
        top1_hit=top1_hit,
        place2_hit=place2_hit,
        place3_hit=place3_hit,
        place4_hit=place4_hit,
        place5_hit=place5_hit,
        place6_hit=place6_hit,
        fukusho_2p_hit=1,
        top3_box_hit=top3_box_hit,
    )


def _rs_eval_row(
    *,
    venue: str = "05",
    race_bango: str = "01",
    ketto_toroku_bango: str = "H1",
    model_version: str = "rs-v3",
    predicted_class: int = 2,
    actual_class: int = 2,
    hit: int = 1,
) -> serve_eval.RsHorseEvalRow:
    return serve_eval.RsHorseEvalRow(
        category="jra",
        race_date="20260614",
        venue=venue,
        race_bango=race_bango,
        ketto_toroku_bango=ketto_toroku_bango,
        model_version=model_version,
        distance_band="sprint",
        field_size_band="medium",
        season_band="summer",
        class_code="A",
        surface="turf",
        predicted_class=predicted_class,
        predicted_label=serve_eval.RS_CLASS_LABELS[predicted_class],
        actual_class=actual_class,
        actual_label=serve_eval.RS_CLASS_LABELS[actual_class],
        hit=hit,
    )


def _fp_row(
    venue: str = "05", race_bango: str = "01", generated_at: datetime = GEN_AT
) -> dict[str, object]:
    return {
        "keibajo_code": venue,
        "race_bango": race_bango,
        "prediction_generated_at": generated_at,
    }


def _rs_row(
    venue: str = "05",
    race_bango: str = "01",
    ketto: str = "H1",
    generated_at: datetime = GEN_AT,
) -> dict[str, object]:
    return {
        "keibajo_code": venue,
        "race_bango": race_bango,
        "ketto_toroku_bango": ketto,
        "prediction_generated_at": generated_at,
    }


@pytest.fixture
def experiment_id(client: MlflowClient) -> str:
    return client.create_experiment("finish-position/production-usage")


@pytest.fixture
def rs_experiment_id(client: MlflowClient) -> str:
    return client.create_experiment("running-style/production-usage")


@pytest.fixture
def tracing_client(client: MlflowClient) -> trace_emit.TracingClient:
    return trace_emit.build_tracing_client(client)


# ── build_tracing_client: resolves from the explicit tracking_uri ──────────


def test_build_tracing_client_uses_explicit_tracking_uri(client: MlflowClient) -> None:
    tc = trace_emit.build_tracing_client(client)
    assert tc.tracking_uri == client.tracking_uri


# ── client_request_id: length-safe regardless of business_key length ───────
#
# Exercised through the PUBLIC emit_fp_race_trace/emit_rs_horse_trace API
# (never the private `_client_request_id` helper directly, per this
# package's own private-member convention) -- these are exactly the two
# properties that matter operationally: (1) an arbitrarily long
# model_version (e.g. a cell-routed variant name) never produces a
# `client_request_id` over the real VARCHAR(50) column limit (see this
# module's own "landmine" docstring section), and (2) the SAME business key
# always maps to the SAME client_request_id, proven indirectly by the
# idempotency behavior itself (a second call for an unchanged, very-long
# model_version is still recognized as a duplicate).


def test_long_model_version_still_yields_a_length_safe_client_request_id(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row(model_version="x" * 500)
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    assert trace.info.client_request_id is not None
    assert len(trace.info.client_request_id) <= 50

    # Determinism proof: re-emitting for the SAME (very long) business key
    # is recognized as an idempotent duplicate, not a new trace.
    second = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert second is None


# ── emit_fp_race_trace: content + idempotency ───────────────────────────────


def test_emit_fp_race_trace_creates_trace_with_root_and_child_spans(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None

    trace = tracing_client.get_trace(trace_id)
    assert trace.info.state == TraceState.OK
    assert trace.info.client_request_id is not None
    assert trace.info.client_request_id.startswith("fp:")
    assert trace.info.tags[trace_emit.TRACE_BUSINESS_KEY_TAG] == "20260614:jra:05:01:iter14"
    assert trace.info.tags["model_version"] == "iter14"
    assert trace.info.tags["category"] == "jra"
    assert trace.info.request_time == int(GEN_AT.timestamp() * 1000)

    span_by_name = {span.name: span for span in trace.data.spans}
    assert set(span_by_name) == {
        trace_emit.FP_ROOT_SPAN_NAME,
        trace_emit.SCORE_MODEL_SPAN_NAME,
        trace_emit.UPSERT_NEON_SPAN_NAME,
    }
    root_span = span_by_name[trace_emit.FP_ROOT_SPAN_NAME]
    assert root_span.span_type == SpanType.TASK
    assert root_span.attributes["model_version"] == "iter14"
    assert root_span.attributes["race_key"] == "20260614:jra:05:01"
    assert root_span.attributes[trace_emit.TIMING_ATTR_KEY] == trace_emit.TIMING_APPROXIMATE
    score_span = span_by_name[trace_emit.SCORE_MODEL_SPAN_NAME]
    assert score_span.span_type == SpanType.TOOL
    assert score_span.attributes["model_version"] == "iter14"
    upsert_span = span_by_name[trace_emit.UPSERT_NEON_SPAN_NAME]
    assert upsert_span.span_type == SpanType.TOOL


def test_emit_fp_race_trace_logs_all_6_rank_assessments_plus_top3_box(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row(
        top1_hit=1, place2_hit=1, place3_hit=0, place4_hit=0, place5_hit=1, place6_hit=0
    )
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    assessments_by_name = {a.name: a for a in trace.info.assessments}
    assert set(assessments_by_name) == {
        "place1_hit",
        "place2_hit",
        "place3_hit",
        "place4_hit",
        "place5_hit",
        "place6_hit",
        trace_emit.TOP3_BOX_ASSESSMENT_NAME,
    }
    assert _feedback_value(assessments_by_name["place1_hit"]) is True
    assert _feedback_value(assessments_by_name["place3_hit"]) is False
    assert _feedback_value(assessments_by_name["place5_hit"]) is True
    assert _feedback_value(assessments_by_name[trace_emit.TOP3_BOX_ASSESSMENT_NAME]) == 1.0
    for assessment in trace.info.assessments:
        assert assessment.source.source_type == "CODE"
        assert assessment.source.source_id == trace_emit.ASSESSMENT_SOURCE_ID


def test_emit_fp_race_trace_small_field_excludes_place456_assessments(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """A too-small field (place4_hit/place5_hit/place6_hit == None, see
    serve_eval._compute_race_hits) must get NO Feedback logged for those
    ranks at all -- never a fabricated `False`."""
    eval_row = _fp_eval_row(place4_hit=None, place5_hit=None, place6_hit=None)
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    assessment_names = {a.name for a in trace.info.assessments}
    assert "place4_hit" not in assessment_names
    assert "place5_hit" not in assessment_names
    assert "place6_hit" not in assessment_names
    assert "place1_hit" in assessment_names
    assert trace_emit.TOP3_BOX_ASSESSMENT_NAME in assessment_names


def test_emit_fp_race_trace_is_idempotent_second_call_returns_none(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row()
    first = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    second = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert first is not None
    assert second is None

    all_traces = tracing_client.search_traces(
        experiment_ids=[experiment_id], include_spans=False, max_results=100
    )
    assert len(all_traces) == 1


# ── emit_rs_horse_trace: content + idempotency ──────────────────────────────


def test_emit_rs_horse_trace_creates_trace_with_predicted_class_hit(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_row = _rs_eval_row(predicted_class=2, actual_class=2, hit=1)
    trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    assert trace.info.state == TraceState.OK
    assert trace.info.tags["ketto_toroku_bango"] == "H1"
    assert trace.info.tags[trace_emit.TRACE_BUSINESS_KEY_TAG] == "20260614:jra:05:01:H1:rs-v3"

    span_by_name = {span.name: span for span in trace.data.spans}
    assert span_by_name[trace_emit.RS_ROOT_SPAN_NAME].span_type == SpanType.TASK
    assert span_by_name[trace_emit.RS_ROOT_SPAN_NAME].attributes["ketto_toroku_bango"] == "H1"

    assessments = {a.name: a for a in trace.info.assessments}
    assert set(assessments) == {trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME}
    assert _feedback_value(assessments[trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME]) is True


def test_emit_rs_horse_trace_predicted_class_miss_is_false(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_row = _rs_eval_row(predicted_class=1, actual_class=2, hit=0)
    trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    assessment = trace.info.assessments[0]
    assert _feedback_value(assessment) is False


def test_emit_rs_horse_trace_is_idempotent_second_call_returns_none(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_row = _rs_eval_row()
    first = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    second = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert first is not None
    assert second is None
    all_traces = tracing_client.search_traces(
        experiment_ids=[rs_experiment_id], include_spans=False, max_results=100
    )
    assert len(all_traces) == 1


# ── emit_fp_race_traces / emit_rs_horse_traces: batch + error isolation ─────


def test_emit_fp_race_traces_batch_creates_one_trace_per_race(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_rows = [
        _fp_eval_row(venue="05", race_bango="01"),
        _fp_eval_row(venue="05", race_bango="02"),
    ]
    rows = [_fp_row("05", "01"), _fp_row("05", "02")]
    summary = trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    assert summary.traces_created == 2
    assert summary.traces_already_existed == 0
    assert summary.errors == []


def test_fp_generated_at_lookup_dedupes_multiple_rows_sharing_one_race(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """A real FP prediction group has MULTIPLE rows per race (one per
    horse, see `_fp_prediction_table`) all sharing the same
    `prediction_generated_at` -- the lookup must not crash or double-count
    on the second (and further) row sharing an already-seen race key."""
    eval_rows = [_fp_eval_row(venue="05", race_bango="01")]
    rows = [
        _fp_row("05", "01"),
        _fp_row("05", "01"),
        _fp_row("05", "01"),
    ]
    summary = trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    assert summary.traces_created == 1
    assert summary.errors == []


def test_emit_fp_race_traces_second_call_counts_already_existed(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_rows = [_fp_eval_row()]
    rows = [_fp_row()]
    trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    second = trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    assert second.traces_created == 0
    assert second.traces_already_existed == 1
    assert second.errors == []


def test_emit_fp_race_traces_missing_generated_at_lookup_is_isolated(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """A race present in eval_rows with no matching raw row (defensive --
    should not happen in practice, see this function's own docstring) is
    recorded as an error and skipped, never raised."""
    eval_rows = [_fp_eval_row(venue="05", race_bango="01")]
    rows: list[dict[str, object]] = []  # no matching raw row at all
    summary = trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    assert summary.traces_created == 0
    assert summary.traces_already_existed == 0
    assert len(summary.errors) == 1
    assert "no prediction_generated_at found" in summary.errors[0]


def test_emit_fp_race_traces_isolates_one_bad_race_from_the_rest(
    tracing_client: trace_emit.TracingClient, experiment_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient MlflowException while emitting ONE race's trace must not
    abort the rest of the batch."""
    eval_rows = [
        _fp_eval_row(venue="05", race_bango="01"),
        _fp_eval_row(venue="05", race_bango="02"),
    ]
    rows = [_fp_row("05", "01"), _fp_row("05", "02")]

    real_emit = trace_emit.emit_fp_race_trace

    def _boom_on_race_01(
        tracing_client: trace_emit.TracingClient,
        experiment_id: str,
        eval_row: serve_eval.FpRaceEvalRow,
        *,
        generated_at: datetime,
    ) -> str | None:
        if eval_row.race_bango == "01":
            raise MlflowException("boom trace emit")
        return real_emit(tracing_client, experiment_id, eval_row, generated_at=generated_at)

    monkeypatch.setattr(trace_emit, "emit_fp_race_trace", _boom_on_race_01)
    summary = trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
    assert summary.traces_created == 1
    assert len(summary.errors) == 1
    assert "boom trace emit" in summary.errors[0]


def test_emit_rs_horse_traces_batch_creates_one_trace_per_horse(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_rows = [
        _rs_eval_row(ketto_toroku_bango="H1"),
        _rs_eval_row(ketto_toroku_bango="H2"),
    ]
    rows = [_rs_row(ketto="H1"), _rs_row(ketto="H2")]
    summary = trace_emit.emit_rs_horse_traces(tracing_client, rs_experiment_id, eval_rows, rows)
    assert summary.traces_created == 2
    assert summary.traces_already_existed == 0
    assert summary.errors == []


def test_emit_rs_horse_traces_second_call_counts_already_existed(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_rows = [_rs_eval_row()]
    rows = [_rs_row()]
    trace_emit.emit_rs_horse_traces(tracing_client, rs_experiment_id, eval_rows, rows)
    second = trace_emit.emit_rs_horse_traces(tracing_client, rs_experiment_id, eval_rows, rows)
    assert second.traces_created == 0
    assert second.traces_already_existed == 1


def test_emit_rs_horse_traces_missing_generated_at_lookup_is_isolated(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_rows = [_rs_eval_row(ketto_toroku_bango="H1")]
    rows: list[dict[str, object]] = []
    summary = trace_emit.emit_rs_horse_traces(tracing_client, rs_experiment_id, eval_rows, rows)
    assert summary.traces_created == 0
    assert len(summary.errors) == 1
    assert "no prediction_generated_at found" in summary.errors[0]


def test_emit_rs_horse_traces_isolates_one_bad_horse_from_the_rest(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    eval_rows = [
        _rs_eval_row(ketto_toroku_bango="H1"),
        _rs_eval_row(ketto_toroku_bango="H2"),
    ]
    rows = [_rs_row(ketto="H1"), _rs_row(ketto="H2")]

    real_emit = trace_emit.emit_rs_horse_trace

    def _boom_on_h1(
        tracing_client: trace_emit.TracingClient,
        experiment_id: str,
        eval_row: serve_eval.RsHorseEvalRow,
        *,
        generated_at: datetime,
    ) -> str | None:
        if eval_row.ketto_toroku_bango == "H1":
            raise MlflowException("boom rs trace emit")
        return real_emit(tracing_client, experiment_id, eval_row, generated_at=generated_at)

    monkeypatch.setattr(trace_emit, "emit_rs_horse_trace", _boom_on_h1)
    summary = trace_emit.emit_rs_horse_traces(tracing_client, rs_experiment_id, eval_rows, rows)
    assert summary.traces_created == 1
    assert len(summary.errors) == 1
    assert "boom rs trace emit" in summary.errors[0]


# ── status=OK always; ERROR is unreachable (documented, not an oversight) ──


def test_every_emitted_trace_is_state_ok(
    tracing_client: trace_emit.TracingClient, experiment_id: str, rs_experiment_id: str
) -> None:
    fp_trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, _fp_eval_row(), generated_at=GEN_AT
    )
    rs_trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, _rs_eval_row(), generated_at=GEN_AT
    )
    assert fp_trace_id is not None
    assert rs_trace_id is not None
    assert tracing_client.get_trace(fp_trace_id).info.state == TraceState.OK
    assert tracing_client.get_trace(rs_trace_id).info.state == TraceState.OK


def _function_attribute_accesses(func: types.FunctionType) -> set[tuple[str, str]]:
    """Same AST-level attribute-access extraction the pre-2026-07-11 form of
    this test module applied to the WHOLE module, scoped instead to ONE
    function's own source -- see `test_fp_rs_emit_path_never_reaches_error_
    status` for why this scoping matters after `job_trace`'s addition."""
    tree = ast.parse(inspect.getsource(func))
    return {
        (node.value.id, node.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
    }


def test_fp_rs_emit_path_never_reaches_error_status() -> None:
    """Structural confirmation that ERROR is unreachable for the FP/RS
    per-race/per-horse emit path's PUBLIC entry points specifically
    (`emit_fp_race_trace`/`emit_rs_horse_trace`), not merely untested: walk
    each function's own real AST (never a naive string/docstring search,
    which would false-positive on this module's own docstring prose
    describing WHY ERROR is unreachable there) and confirm no `TraceState.
    ERROR`/`SpanStatusCode.ERROR` attribute access exists in either of them.
    Their shared private helper `_emit_trace` is, by inspection, ALSO
    ERROR-free (it always builds `TraceInfo(state=TraceState.OK, ...)` and
    never passes a `status=` override to `_make_span`) -- not re-verified
    here at the AST level to avoid a `reportPrivateUsage`/`getattr`-workaround
    dance for a private module member, which this package's lint rules both
    discourage.

    Scoped to these two functions only (not the whole module, unlike this
    test's pre-2026-07-11 form) because `job_trace`'s `JobTrace` class
    (added 2026-07-11) legitimately DOES reach `TraceState.ERROR`/
    `SpanStatusCode.ERROR` when the job it wraps raises -- see
    `trace_emit.py`'s own "JOB-EXECUTION TRACES" docstring section for why
    that is a deliberate, narrow exception, not a whole-module invariant.
    """
    for func in (trace_emit.emit_fp_race_trace, trace_emit.emit_rs_horse_trace):
        attribute_pairs = _function_attribute_accesses(func)
        assert ("TraceState", "ERROR") not in attribute_pairs, func
        assert ("SpanStatusCode", "ERROR") not in attribute_pairs, func


# ── ★ Regression: never touches ambient/global tracking-URI state ─────────
#
# This is the single most important regression to lock in, per the
# historical incident this module's own docstring documents (a leaked
# global tracking URI once silently overwrote two production champion
# aliases with fake test data -- see tests/conftest.py's own
# clear_ambient_backend_uri docstring).


def test_trace_emit_never_calls_mlflow_set_tracking_uri(
    tracing_client: trace_emit.TracingClient,
    experiment_id: str,
    rs_experiment_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("trace_emit must never call mlflow.set_tracking_uri()")

    monkeypatch.setattr(mlflow, "set_tracking_uri", _boom)

    trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, _fp_eval_row(), generated_at=GEN_AT
    )
    trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, _rs_eval_row(), generated_at=GEN_AT
    )
    trace_emit.emit_fp_race_traces(
        tracing_client, experiment_id, [_fp_eval_row(race_bango="03")], [_fp_row(race_bango="03")]
    )
    trace_emit.emit_rs_horse_traces(
        tracing_client,
        rs_experiment_id,
        [_rs_eval_row(ketto_toroku_bango="H9")],
        [_rs_row(ketto="H9")],
    )


def test_trace_emit_does_not_mutate_ambient_global_tracking_uri(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """A completely independent (real) proof, not just a monkeypatched
    assertion: the AMBIENT global tracking uri (`mlflow.get_tracking_uri()`)
    is read BEFORE and AFTER emitting a trace against an isolated sqlite
    store whose uri is guaranteed different from the ambient default, and
    must be byte-for-byte unchanged."""
    before = mlflow.get_tracking_uri()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, _fp_eval_row(), generated_at=GEN_AT
    )
    after = mlflow.get_tracking_uri()
    assert trace_id is not None
    assert before == after
    assert tracing_client.tracking_uri != before


def test_trace_emit_module_never_imports_bare_mlflow_fluent_module() -> None:
    """Structural regression guard: this module must only ever import
    SPECIFIC symbols from `mlflow.entities`/`mlflow.tracing.*`/
    `mlflow.exceptions`/`mlflow.tracking.client` -- never a bare
    `import mlflow` that would make a fluent `mlflow.start_trace()`/
    `mlflow.log_feedback()`/`mlflow.set_tracking_uri()` call possible from
    this file at all. `from x.y import z` binds only `z`, never `x` -- so if
    this module never did a bare `import mlflow`, the module-level name
    `mlflow` is simply absent from its namespace."""
    assert not hasattr(trace_emit, "mlflow")
    # AST-level confirmation (not a docstring-fooled string search): no
    # `Name(id="mlflow")` reference exists anywhere in the real code.
    names_referenced = _module_name_references(trace_emit)
    assert "mlflow" not in names_referenced


# ── fp_race_key / rs_race_key helpers ───────────────────────────────────────


def test_fp_race_key_shape() -> None:
    assert trace_emit.fp_race_key(_fp_eval_row()) == "20260614:jra:05:01"


def test_rs_race_key_shape() -> None:
    assert trace_emit.rs_race_key(_rs_eval_row()) == "20260614:jra:05:01"


# ── job_trace: one trace per CLI/job invocation ─────────────────────────────


@pytest.fixture
def job_experiment_id(client: MlflowClient) -> str:
    return client.create_experiment("finish-position/champion-eval")


def test_job_trace_happy_path_logs_root_span_and_steps_with_real_timing(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    with trace_emit.job_trace(
        tracing_client, job_experiment_id, "eval-champion-cells", attributes={"category": "jra"}
    ) as t:
        trace_id = t.trace_id
        with t.step("resolve-champion"):
            pass
        with t.step("eval-join", category="jra"):
            pass

    trace = tracing_client.get_trace(trace_id)
    assert trace.info.state == TraceState.OK
    assert trace.info.tags[trace_emit.JOB_TRACE_KIND_TAG] == trace_emit.JOB_TRACE_KIND_VALUE
    assert trace.info.execution_duration is not None
    assert trace.info.execution_duration >= 0

    span_by_name = {span.name: span for span in trace.data.spans}
    assert set(span_by_name) == {"eval-champion-cells", "resolve-champion", "eval-join"}
    root_span = span_by_name["eval-champion-cells"]
    assert root_span.span_type == SpanType.TASK
    assert root_span.attributes["category"] == "jra"
    assert root_span.attributes[trace_emit.TIMING_ATTR_KEY] == trace_emit.TIMING_REAL
    resolve_span = span_by_name["resolve-champion"]
    assert resolve_span.span_type == SpanType.TOOL
    eval_join_span = span_by_name["eval-join"]
    assert eval_join_span.attributes["category"] == "jra"
    # REAL timing: every span's end must be >= its own start (never negative
    # duration), and the root span must fully CONTAIN each child span's
    # window (it started before, and ended after, every step it wraps).
    # `end_time_ns` is typed `int | None` on `Span` in general (an
    # in-flight/unfinished span has none) -- every span `job_trace` itself
    # ever logs is always already-finished, so `is not None` narrows this
    # via a real `assert` rather than a type-ignore-style suppression.
    for span in trace.data.spans:
        assert span.end_time_ns is not None
        assert span.end_time_ns >= span.start_time_ns
    assert root_span.start_time_ns <= resolve_span.start_time_ns
    assert eval_join_span.end_time_ns is not None
    assert root_span.end_time_ns is not None
    assert root_span.end_time_ns >= eval_join_span.end_time_ns


def test_job_trace_no_steps_still_logs_a_valid_root_span_only_trace(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    with trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells") as t:
        trace_id = t.trace_id

    trace = tracing_client.get_trace(trace_id)
    assert trace.info.state == TraceState.OK
    assert {span.name for span in trace.data.spans} == {"eval-cells"}


def test_job_trace_sets_error_status_and_reraises_on_exception(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    captured_trace_id: str | None = None

    with (
        pytest.raises(ValueError, match="boom"),
        trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells") as t,
    ):
        captured_trace_id = t.trace_id
        with t.step("collect-rows"):
            raise ValueError("boom")

    assert captured_trace_id is not None
    trace = tracing_client.get_trace(captured_trace_id)
    assert trace.info.state == TraceState.ERROR
    span_by_name = {span.name: span for span in trace.data.spans}
    assert span_by_name["eval-cells"].status.status_code == "ERROR"
    assert span_by_name["collect-rows"].status.status_code == "ERROR"


def test_job_trace_step_failure_does_not_mark_earlier_steps_error(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    trace_id_holder: list[str] = []
    with (
        pytest.raises(RuntimeError),
        trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells") as t,
    ):
        trace_id_holder.append(t.trace_id)
        with t.step("resolve-champion"):
            pass
        with t.step("collect-rows"):
            raise RuntimeError("boom")

    trace = tracing_client.get_trace(trace_id_holder[0])
    span_by_name = {span.name: span for span in trace.data.spans}
    assert span_by_name["resolve-champion"].status.status_code == "OK"
    assert span_by_name["collect-rows"].status.status_code == "ERROR"


def test_job_trace_feedback_numeric_and_bool(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    with trace_emit.job_trace(tracing_client, job_experiment_id, "eval-champion-cells") as t:
        trace_id = t.trace_id
        t.feedback("has_champion_coverage", True)
        t.feedback("cells_evaluated", 12.0)

    trace = tracing_client.get_trace(trace_id)
    assessments_by_name = {a.name: a for a in trace.info.assessments}
    assert set(assessments_by_name) == {"has_champion_coverage", "cells_evaluated"}
    assert _feedback_value(assessments_by_name["has_champion_coverage"]) is True
    assert _feedback_value(assessments_by_name["cells_evaluated"]) == 12.0
    for assessment in trace.info.assessments:
        assert assessment.source.source_id == trace_emit.JOB_TRACE_SOURCE_ID


def test_job_trace_feedback_default_rationale_mentions_job_name(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    with trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells") as t:
        trace_id = t.trace_id
        t.feedback("runs_created", 3.0)

    trace = tracing_client.get_trace(trace_id)
    assessment = trace.info.assessments[0]
    assert assessment.rationale is not None
    assert "eval-cells" in assessment.rationale


def test_job_trace_discard_drops_trace_on_normal_exit(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    with trace_emit.job_trace(tracing_client, job_experiment_id, "sync-production") as t:
        trace_id = t.trace_id
        t.discard()

    all_traces = tracing_client.search_traces(
        experiment_ids=[job_experiment_id], include_spans=False, max_results=100
    )
    assert all_traces == []
    with pytest.raises(MlflowException):
        tracing_client.get_trace(trace_id)


def test_job_trace_discard_has_no_effect_when_block_raises(
    tracing_client: trace_emit.TracingClient, job_experiment_id: str
) -> None:
    """A real failure must never be silently dropped, even if `discard()`
    was already called earlier in the same `with` block."""
    trace_id_holder: list[str] = []
    with (
        pytest.raises(ValueError, match="boom"),
        trace_emit.job_trace(tracing_client, job_experiment_id, "sync-production") as t,
    ):
        trace_id_holder.append(t.trace_id)
        t.discard()
        raise ValueError("boom")

    trace = tracing_client.get_trace(trace_id_holder[0])
    assert trace.info.state == TraceState.ERROR


def test_job_trace_persist_failure_is_swallowed_when_job_itself_succeeded(
    tracing_client: trace_emit.TracingClient,
    job_experiment_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient tracking-store failure while trying to RECORD that a job
    ran must never itself crash a job that otherwise completed
    successfully -- see trace_emit.py's own docstring rationale."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("transient store failure")

    monkeypatch.setattr(tracing_client, "start_trace", _boom)

    with (
        pytest.warns(UserWarning, match="failed to persist job-execution trace"),
        trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells"),
    ):
        pass  # the wrapped "job" itself succeeds


def test_job_trace_persist_failure_does_not_mask_the_real_exception(
    tracing_client: trace_emit.TracingClient,
    job_experiment_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When BOTH the wrapped job raises AND trace persistence itself fails,
    the job's own (original) exception must still be what propagates."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("transient store failure")

    monkeypatch.setattr(tracing_client, "start_trace", _boom)

    with (
        pytest.warns(UserWarning, match="failed to persist job-execution trace"),
        pytest.raises(ValueError, match="real job failure"),
        trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells"),
    ):
        raise ValueError("real job failure")


def test_job_trace_never_calls_mlflow_set_tracking_uri(
    tracing_client: trace_emit.TracingClient,
    job_experiment_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("job_trace must never call mlflow.set_tracking_uri()")

    monkeypatch.setattr(mlflow, "set_tracking_uri", _boom)

    with trace_emit.job_trace(tracing_client, job_experiment_id, "eval-cells") as t:
        with t.step("resolve-champion"):
            pass
        t.feedback("runs_created", 1.0)


# ── ASSESSMENT-COMPLETENESS HEALING ─────────────────────────────────────────
#
# Reproduces the real 2026-07-11 production incident directly: a trace whose
# spans were logged successfully but whose assessments were left partial (FP)
# or entirely empty (RS) by a transient store error between `log_spans` and
# one of the `log_assessment` calls that follow it. `tracing_client.
# delete_assessment` is used to simulate that exact after-the-fact state (a
# real assessment id deleted from an otherwise-complete trace), rather than
# reaching into this module's own private machinery.


def _assessment_names(trace: Trace) -> list[str]:
    return [a.name for a in trace.info.assessments]


def test_fp_expected_assessment_names_full_field() -> None:
    eval_row = _fp_eval_row(place4_hit=0, place5_hit=1, place6_hit=0)
    assert trace_emit.fp_expected_assessment_names(eval_row) == {
        "place1_hit",
        "place2_hit",
        "place3_hit",
        "place4_hit",
        "place5_hit",
        "place6_hit",
        trace_emit.TOP3_BOX_ASSESSMENT_NAME,
    }


def test_fp_expected_assessment_names_small_field_excludes_456() -> None:
    eval_row = _fp_eval_row(place4_hit=None, place5_hit=None, place6_hit=None)
    assert trace_emit.fp_expected_assessment_names(eval_row) == {
        "place1_hit",
        "place2_hit",
        "place3_hit",
        trace_emit.TOP3_BOX_ASSESSMENT_NAME,
    }


def test_rs_expected_assessment_names_is_the_fixed_singleton() -> None:
    assert trace_emit.rs_expected_assessment_names() == {
        trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME
    }


def test_fp_missing_assessment_names_empty_when_all_present() -> None:
    eval_row = _fp_eval_row()
    expected = trace_emit.fp_expected_assessment_names(eval_row)
    assert trace_emit.fp_missing_assessment_names(eval_row, expected) == frozenset()


def test_fp_missing_assessment_names_returns_the_gap() -> None:
    eval_row = _fp_eval_row()
    missing = trace_emit.fp_missing_assessment_names(eval_row, {"place1_hit", "place2_hit"})
    assert missing == {
        "place3_hit",
        "place4_hit",
        "place5_hit",
        "place6_hit",
        trace_emit.TOP3_BOX_ASSESSMENT_NAME,
    }


def test_fp_missing_assessment_names_small_field_never_flags_excluded_ranks() -> None:
    """A small-field race whose trace already has exactly its (smaller)
    expected set must show NO gap -- place4/5/6 are legitimately absent from
    `existing_names` too, never flagged as "missing"."""
    eval_row = _fp_eval_row(place4_hit=None, place5_hit=None, place6_hit=None)
    existing = {"place1_hit", "place2_hit", "place3_hit", trace_emit.TOP3_BOX_ASSESSMENT_NAME}
    assert trace_emit.fp_missing_assessment_names(eval_row, existing) == frozenset()


def test_rs_missing_assessment_names() -> None:
    assert trace_emit.rs_missing_assessment_names(set()) == {
        trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME
    }
    assert (
        trace_emit.rs_missing_assessment_names(
            {trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME}
        )
        == frozenset()
    )


# ── heal_fp_race_traces ──────────────────────────────────────────────────────


def test_heal_fp_race_traces_no_op_when_already_complete(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None

    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_checked == 1
    assert summary.traces_healed == 0
    assert summary.assessments_added == 0
    assert summary.errors == []

    names = _assessment_names(tracing_client.get_trace(trace_id))
    assert len(names) == len(set(names)) == 7  # no duplicates created


def test_heal_fp_race_traces_tops_up_missing_top3_box_score(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """Reproduces the real FP incident directly: top3_box_score (an
    UNCONDITIONAL assessment) missing from an otherwise-complete trace."""
    eval_row = _fp_eval_row()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    top3_assessment = next(
        a for a in trace.info.assessments if a.name == trace_emit.TOP3_BOX_ASSESSMENT_NAME
    )
    assert top3_assessment.assessment_id is not None
    tracing_client.delete_assessment(trace_id, top3_assessment.assessment_id)
    assert trace_emit.TOP3_BOX_ASSESSMENT_NAME not in _assessment_names(
        tracing_client.get_trace(trace_id)
    )

    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_healed == 1
    assert summary.assessments_added == 1
    assert summary.errors == []

    healed_names = _assessment_names(tracing_client.get_trace(trace_id))
    assert healed_names.count(trace_emit.TOP3_BOX_ASSESSMENT_NAME) == 1
    # The still-present rank assessments were never re-logged (no duplicates).
    assert healed_names.count("place1_hit") == 1
    assert set(healed_names) == trace_emit.fp_expected_assessment_names(eval_row)


def test_heal_fp_race_traces_tops_up_a_single_missing_rank_only(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """The reverse of the top3-only case: a single missing rank assessment
    while top3_box_score is already present -- must top up ONLY that rank,
    never re-logging top3_box_score."""
    eval_row = _fp_eval_row()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    place2_assessment = next(a for a in trace.info.assessments if a.name == "place2_hit")
    assert place2_assessment.assessment_id is not None
    tracing_client.delete_assessment(trace_id, place2_assessment.assessment_id)

    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_healed == 1
    assert summary.assessments_added == 1

    healed_names = _assessment_names(tracing_client.get_trace(trace_id))
    assert healed_names.count("place2_hit") == 1
    assert healed_names.count(trace_emit.TOP3_BOX_ASSESSMENT_NAME) == 1
    assert set(healed_names) == trace_emit.fp_expected_assessment_names(eval_row)


def test_heal_fp_race_traces_from_zero_assessments(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row()
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    for assessment in list(trace.info.assessments):
        assert assessment.assessment_id is not None
        tracing_client.delete_assessment(trace_id, assessment.assessment_id)
    assert tracing_client.get_trace(trace_id).info.assessments == []

    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_healed == 1
    assert summary.assessments_added == 7

    healed_names = _assessment_names(tracing_client.get_trace(trace_id))
    assert set(healed_names) == trace_emit.fp_expected_assessment_names(eval_row)


def test_heal_fp_race_traces_small_field_never_flags_excluded_ranks_as_missing(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    eval_row = _fp_eval_row(place4_hit=None, place5_hit=None, place6_hit=None)
    trace_id = trace_emit.emit_fp_race_trace(
        tracing_client, experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None

    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_healed == 0
    assert summary.assessments_added == 0
    names = _assessment_names(tracing_client.get_trace(trace_id))
    assert "place4_hit" not in names
    assert "place5_hit" not in names
    assert "place6_hit" not in names


def test_heal_fp_race_traces_skips_race_with_no_trace_at_all(
    tracing_client: trace_emit.TracingClient, experiment_id: str
) -> None:
    """A race whose OWN trace creation never happened (a different failure,
    already surfaced elsewhere) is silently skipped, not an error."""
    eval_row = _fp_eval_row()
    summary = trace_emit.heal_fp_race_traces(tracing_client, experiment_id, [eval_row])
    assert summary.traces_checked == 1
    assert summary.traces_healed == 0
    assert summary.errors == []


def test_heal_fp_race_traces_isolates_one_bad_race_from_the_rest(
    tracing_client: trace_emit.TracingClient, experiment_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    eval_row_ok = _fp_eval_row(venue="05", race_bango="01")
    eval_row_bad = _fp_eval_row(venue="05", race_bango="02")
    for row in (eval_row_ok, eval_row_bad):
        trace_id = trace_emit.emit_fp_race_trace(
            tracing_client, experiment_id, row, generated_at=GEN_AT
        )
        assert trace_id is not None
        top3 = next(
            a
            for a in tracing_client.get_trace(trace_id).info.assessments
            if a.name == trace_emit.TOP3_BOX_ASSESSMENT_NAME
        )
        assert top3.assessment_id is not None
        tracing_client.delete_assessment(trace_id, top3.assessment_id)

    # heal_fp_race_traces visits eval_rows in list order, one search_traces
    # lookup each -- fail only the SECOND lookup (eval_row_bad's own), so the
    # first (eval_row_ok) must have already healed successfully by then.
    real_search_traces = tracing_client.search_traces
    call_count = 0

    def _flaky_search_traces(
        *,
        experiment_ids: list[str] | None = None,
        filter_string: str | None = None,
        max_results: int = 100,
        order_by: list[str] | None = None,
        page_token: str | None = None,
        run_id: str | None = None,
        include_spans: bool = True,
        model_id: str | None = None,
        locations: list[str] | None = None,
    ) -> PagedList[Trace]:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise MlflowException("boom heal search")
        return real_search_traces(
            experiment_ids=experiment_ids,
            filter_string=filter_string,
            max_results=max_results,
            order_by=order_by,
            page_token=page_token,
            run_id=run_id,
            include_spans=include_spans,
            model_id=model_id,
            locations=locations,
        )

    monkeypatch.setattr(tracing_client, "search_traces", _flaky_search_traces)
    summary = trace_emit.heal_fp_race_traces(
        tracing_client, experiment_id, [eval_row_ok, eval_row_bad]
    )
    assert summary.traces_checked == 2
    assert summary.traces_healed == 1
    assert summary.assessments_added == 1
    assert len(summary.errors) == 1
    assert "boom heal search" in summary.errors[0]


# ── heal_rs_horse_traces ─────────────────────────────────────────────────────


def test_heal_rs_horse_traces_no_op_when_already_complete(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_row = _rs_eval_row()
    trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None

    summary = trace_emit.heal_rs_horse_traces(tracing_client, rs_experiment_id, [eval_row])
    assert summary.traces_checked == 1
    assert summary.traces_healed == 0
    assert summary.assessments_added == 0

    names = _assessment_names(tracing_client.get_trace(trace_id))
    assert names == [trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME]


def test_heal_rs_horse_traces_tops_up_from_zero_assessments(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    """Reproduces the real 2026-07-11 production incident directly: an RS
    trace with ZERO assessments logged at all."""
    eval_row = _rs_eval_row()
    trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    for assessment in list(trace.info.assessments):
        assert assessment.assessment_id is not None
        tracing_client.delete_assessment(trace_id, assessment.assessment_id)
    assert tracing_client.get_trace(trace_id).info.assessments == []

    summary = trace_emit.heal_rs_horse_traces(tracing_client, rs_experiment_id, [eval_row])
    assert summary.traces_healed == 1
    assert summary.assessments_added == 1
    assert summary.errors == []

    healed_names = _assessment_names(tracing_client.get_trace(trace_id))
    assert healed_names == [trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME]


def test_heal_rs_horse_traces_skips_horse_with_no_trace_at_all(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str
) -> None:
    eval_row = _rs_eval_row()
    summary = trace_emit.heal_rs_horse_traces(tracing_client, rs_experiment_id, [eval_row])
    assert summary.traces_checked == 1
    assert summary.traces_healed == 0
    assert summary.errors == []


def test_heal_rs_horse_traces_isolates_one_bad_horse_from_the_rest(
    tracing_client: trace_emit.TracingClient, rs_experiment_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    eval_row = _rs_eval_row()
    trace_id = trace_emit.emit_rs_horse_trace(
        tracing_client, rs_experiment_id, eval_row, generated_at=GEN_AT
    )
    assert trace_id is not None
    trace = tracing_client.get_trace(trace_id)
    only_assessment = trace.info.assessments[0]
    assert only_assessment.assessment_id is not None
    tracing_client.delete_assessment(trace_id, only_assessment.assessment_id)

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom rs heal search")

    monkeypatch.setattr(tracing_client, "search_traces", _boom)
    summary = trace_emit.heal_rs_horse_traces(tracing_client, rs_experiment_id, [eval_row])
    assert summary.traces_healed == 0
    assert len(summary.errors) == 1
    assert "boom rs heal search" in summary.errors[0]
