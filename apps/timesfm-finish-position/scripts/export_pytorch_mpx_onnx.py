#!/usr/bin/env python3
"""Export the JSON-dumped RaceSetTransformer to ONNX and check CPU parity."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final

import numpy as np
import torch

from timesfm_finish_position.pytorch_mpx import RaceSetTransformer

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
DEFAULT_JSON: Final[Path] = (
    REPO_ROOT / "apps/finish-position-predict-container/src/predict_lib/pytorch_mpx_jra.json"
)
PARITY_TOLERANCE: Final[float] = 1e-4
TRACE_RUNNERS: Final[int] = 8
MAX_RUNNERS: Final[int] = 18
PARITY_SEED: Final[int] = 20261016


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="export_pytorch_mpx_onnx")
    parser.add_argument(
        "--source-json",
        type=Path,
        required=True,
        help="Weights JSON. Never the served sidecar: export overwrites the sidecar.",
    )
    parser.add_argument("--onnx-out", type=Path, default=None)
    parser.add_argument("--sidecar-out", type=Path, default=None)
    parser.add_argument("--tolerance", type=float, default=PARITY_TOLERANCE)
    return parser.parse_args(argv)


def _tensor_state(weights: object) -> dict[str, torch.Tensor]:
    if not isinstance(weights, dict) or not weights:
        raise ValueError("source json must include weights")
    state: dict[str, torch.Tensor] = {}
    for key, value in weights.items():
        if not isinstance(key, str):
            raise ValueError("weight keys must be strings")
        state[key] = torch.tensor(np.asarray(value, dtype=np.float32))
    return state


def parity_max_abs(model: RaceSetTransformer, onnx_path: Path, feature_count: int) -> float:
    """Max |torch - onnx| over every field size 1..MAX_RUNNERS, full and padded masks."""
    import onnxruntime as ort

    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    rng = np.random.default_rng(PARITY_SEED)
    worst = 0.0
    for runners in range(1, MAX_RUNNERS + 1):
        features = rng.standard_normal((1, runners, feature_count)).astype(np.float32)
        for active in sorted({runners, max(1, runners - 2)}):
            mask = np.zeros((1, runners), dtype=bool)
            mask[0, :active] = True
            with torch.inference_mode():
                expected = model(torch.from_numpy(features), torch.from_numpy(mask)).numpy()
            actual = np.asarray(session.run(["scores"], {"features": features, "mask": mask})[0])
            worst = max(worst, float(np.max(np.abs(expected[mask] - actual[mask]))))
    return worst


def export(source: Path, onnx_out: Path, sidecar_out: Path, tolerance: float) -> None:
    document = json.loads(source.read_text(encoding="utf-8"))
    feature_order = document["feature_order"]
    model = RaceSetTransformer(
        len(feature_order),
        embedding_dim=int(document["embedding_dim"]),
        heads=int(document["heads"]),
        layers=int(document["layers"]),
        dropout=0.0,
    )
    model.load_state_dict(_tensor_state(document["weights"]))
    model.eval()
    features = torch.randn(1, TRACE_RUNNERS, len(feature_order), dtype=torch.float32)
    mask = torch.ones(1, TRACE_RUNNERS, dtype=torch.bool)
    runners = torch.export.Dim("runners", min=1, max=MAX_RUNNERS)
    # dynamo export keeps the runner axis symbolic inside nn.MultiheadAttention;
    # the legacy tracer baked the trace length (8) into every attention reshape,
    # so every race with a different field size failed in onnxruntime.
    torch.onnx.export(
        model,
        (features, mask),
        str(onnx_out),
        input_names=["features", "mask"],
        output_names=["scores"],
        dynamo=True,
        external_data=False,
        opset_version=18,
        dynamic_shapes={"features": {1: runners}, "mask": {1: runners}},
    )
    max_abs = parity_max_abs(model, onnx_out, len(feature_order))
    if max_abs > tolerance:
        raise SystemExit(f"onnx parity failed: max_abs={max_abs} tolerance={tolerance}")
    sidecar = {
        "version": "pytorch-mpx-onnx-v1",
        "category": document["category"],
        "feature_order": feature_order,
        "mean": document["mean"],
        "scale": document["scale"],
        "onnx": onnx_out.name,
        "signals": document.get("signals", {}),
        "parity_max_abs": max_abs,
    }
    sidecar_out.write_text(
        json.dumps(sidecar, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {onnx_out} sidecar={sidecar_out} max_abs={max_abs}", flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source = args.source_json
    onnx_out = args.onnx_out or DEFAULT_JSON.with_name("pytorch_mpx_jra.onnx")
    sidecar_out = args.sidecar_out or DEFAULT_JSON
    export(source, onnx_out, sidecar_out, args.tolerance)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
