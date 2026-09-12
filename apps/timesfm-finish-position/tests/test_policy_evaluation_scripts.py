"""Regression checks for typed offline policy reports."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load_script(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load evaluation script")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_cell_policy_query_preserves_placeholders() -> None:
    subject = load_script("optimize_prophet_cell_policy")
    query = subject.entry_query("jvd", "jra")
    assert query.count("%s") == 5
    assert "from jvd_se" in query


@pytest.mark.parametrize(
    "name",
    ["optimize_prophet_served_branch_policy", "optimize_prophet_maximum_branch_policy"],
)
def test_branch_metric_reports_keep_numeric_contract(name: str) -> None:
    subject = load_script(name)
    assert subject.metric_report((1,), (2,), 4) == {
        "top1": {
            "baseline_hits": 1,
            "adjusted_hits": 2,
            "delta_hits": 1,
            "baseline_rate": 0.25,
            "adjusted_rate": 0.5,
            "delta_pp": 25.0,
        }
    }
