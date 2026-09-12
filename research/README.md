# Research source and generated artifacts

`research/.gitignore` ignores new files by default, regardless of experiment name or output format. Scripts, tests, SQL, documentation, and the listed source/configuration formats remain visible to Git at any directory depth.

- Commit validation and experiment scripts together with their tests. Do not hide scripts merely because they are used only for research.
- Model weights, predictions, datasets, HTTP captures, coverage output, and other generated files stay local. For example, `.cbm`, `.parquet`, `.json`, `.csv`, and `.headers` are not automatically tracked.
- `runtime/`, dependency/cache directories, and `test-work-*`, `test-full-*`, and `test-model-*` directories are reserved for generated state. Keep authored code outside these directories.
- Existing tracked snapshots remain tracked; ignore rules neither delete nor untrack them.
- For a new source format or an intentionally retained fixture/snapshot, add a narrow exception to the applicable `.gitignore` and extend the ignore-rule test. Do not broadly allow all experiment outputs or use force-add as the normal workflow.
- Secrets must never be committed, including secrets embedded in otherwise allowed source/config files.

Check source/artifact classification without running a research job:

```sh
bash research/test-gitignore.sh
```
