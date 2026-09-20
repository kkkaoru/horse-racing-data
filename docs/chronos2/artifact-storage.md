# Lossless artifact storage — local APFS

## Current: clone sharing for completed trials

The 288 completed trials retain **all 576** portable models and resumable checkpoints. No model was deleted or quantized; model paths and data-fork bytes are unchanged.

APFS clone-and-patch replaced independent copies of common weights with shared filesystem extents. Files were grouped into six equal-length layouts, cloned from a plain reference, patched in64KiB blocks, and SHA-256 checked before atomic replacement. Source/reference changes cause rejection. Generated references were removed after each group: clones own their extents independently and do not depend on reference filenames. Permissions and modification times are retained.

Measured free space increased from **143,825,367,040 to312,830,701,568 bytes**: approximately **169GB (157.4GiB) reclaimed** in268 seconds. The target files previously accounted for178.3GiB of allocated blocks. Free-space measurements may include unrelated system activity.

**Use `df` to assess real free space.** `du` counts shared APFS extents repeatedly, so `.cache/chronos2` can display about204GiB despite the substantial reduction in actual disk use. A normal copy/archive to another filesystem can expand back to the full logical size. Do not rerun blanket per-file compression over these clones: breaking shared extents can consume more space even if per-file `du` gets smaller.

Implementation and evidence:

- `research/chronos2/reflink_storage.py`: guarded clone/patch/verify/replace implementation.
- `research/chronos2/test_reflink_storage.py`:16 tests,100% statement/branch coverage; configured Ruff/format/basedpyright passed.
- `research/chronos2/storage-probe-002/`:12-file non-mutating probe.
- `research/chronos2/storage-apply-002/{plan.json,ledger.jsonl,result.json}`:576-file inventory, per-file hashes and allocation accounting.
- `research/chronos2/storage-apply-002/verification.json`: independent post-cleanup verification completed for all576 files: every SHA-256 matches, every safetensors file opens and its first tensor loads on CPU. Free space after verification:312,828,878,848 bytes, approximately169GB above the initial value.
- `research/chronos2/verify-storage-optimization.sh`: full checksum and CPU safetensors read verification after reference cleanup. Uses PyTorch because NumPy cannot materialize the original BF16 checkpoint tensors.
- `research/chronos2/storage-optimize-001/`: retained quality/probe/apply/verification logs, including failed preflights. None of those failures damaged model contents.

Run only on immutable completed trials, never concurrently with training. The driver intentionally refuses an existing output directory; a future optimization needs a new reviewed inventory rather than silently rerunning over already-shared files. Production loading code is unchanged.

## Earlier: transparent per-file compression

All completed trial artifacts were retained. No model weights were pruned or converted to a lower precision.

A probe of one rejected JRA portable model reduced allocated bytes from **477,933,568 to 318,783,488**, while its logical size stayed **477,930,496 bytes**. SHA-256 before and after was identical: `68fb1b7d18915cd8efa7f341ebaa4b872dcba954e514c5862bf54b1079f3a1ab`.

Completed JRA/Ban-ei weights were copied with macOS `ditto --hfsCompression`, verified byte-for-byte by SHA-256, checked for concurrent source changes, and atomically replaced at the original path only when allocated storage decreased. A per-file ledger records both digests and allocation sizes. This is transparent filesystem compression, not tensor quantization or a different trained model. It does not imply smaller logical upload sizes or Linux/container storage savings.

Before the independent NAR run, the measured largest completed trial occupied 756,275,014 logical bytes. The reservation was 96 times that size plus 15 GiB: **88,708,528,704 bytes**. An extra buffer was recovered after other allocations; NAR started with **90,861,998,080 bytes free**. Continue monitoring rather than assuming available space stays constant.

Artifacts under `.cache/chronos2/cell-improvement/`:

- `compression-probe/result.json`
- `compression-work/plan.json`, `files.jsonl`, `result.json`
- `compression-work/launch-buffer.jsonl`
- `lossless-compression.log`

These operations preserve original model paths and logical contents, including frozen selected-model hashes. No compression job runs concurrently with heavy model training.
