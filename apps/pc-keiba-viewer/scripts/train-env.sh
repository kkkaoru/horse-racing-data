#!/usr/bin/env bash
# Apple Silicon (M5 Pro 5P+10E=15 core, 48GB) 向け ML 訓練前 env。
#
# Usage:
#   source apps/pc-keiba-viewer/scripts/train-env.sh
#   bun run train:lightgbm        # or any subsequent ML command
#
# Goal: BLAS / OpenMP の oversubscription を防ぎ、LightGBM/XGBoost の num_threads と
# 衝突しないようライブラリ並列を抑える。Accelerate (numpy default) は P コア相当に揃える。

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-8}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-8}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-4}"
