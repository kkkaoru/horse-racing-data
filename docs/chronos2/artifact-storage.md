# Lossless artifact storage — local APFS

All completed trial artifacts are retained. No model weights were pruned or converted to a lower precision.

A probe of one rejected JRA portable model reduced allocated bytes from **477,933,568 to 318,783,488**, while its logical size stayed **477,930,496 bytes**. SHA-256 before and after was identical: `68fb1b7d18915cd8efa7f341ebaa4b872dcba954e514c5862bf54b1079f3a1ab`.

Completed JRA/Ban-ei weights were copied with macOS `ditto --hfsCompression`, verified byte-for-byte by SHA-256, checked for concurrent source changes, and atomically replaced at the original path only when allocated storage decreased. A per-file ledger records both digests and allocation sizes. This is transparent filesystem compression, not tensor quantization or a different trained model. It does not imply smaller logical upload sizes or Linux/container storage savings.

Before the independent NAR run, the measured largest completed trial occupied 756,275,014 logical bytes. The reservation was 96 times that size plus 15 GiB: **88,708,528,704 bytes**. An extra buffer was recovered after other allocations; NAR started with **90,861,998,080 bytes free**. Continue monitoring rather than assuming available space stays constant.

Artifacts under `.cache/chronos2/cell-improvement/`:

- `compression-probe/result.json`
- `compression-work/plan.json`, `files.jsonl`, `result.json`
- `compression-work/launch-buffer.jsonl`
- `lossless-compression.log`

These operations preserve original model paths and logical contents, including frozen selected-model hashes. No compression job runs concurrently with heavy model training.
