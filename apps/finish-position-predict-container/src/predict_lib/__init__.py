"""Pure, unit-testable helpers for the finish-position upcoming predictor.

The heavy orchestration (DuckDB feature build, model load, PG writes) lives in
``predict_upcoming.py``; everything in this package is side-effect-free so it can
be covered to >= 95% with mocked I/O.
"""
