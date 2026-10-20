"""Race-set Transformer for JRA finish-position prediction.

Run via the CLI entrypoint at src/scripts/finish_position_transformer.py.
"""
import os

# MLX >= 0.31 runs float32 GPU matmuls as TF32 on Apple neural accelerators
# (~1e-3 relative error vs ~1e-6), so MLX scores drift from the float64 numpy
# serve path. MLX reads this once at the first matmul; export
# MLX_ENABLE_TF32=1 to opt back in for speed.
_ = os.environ.setdefault("MLX_ENABLE_TF32", "0")
