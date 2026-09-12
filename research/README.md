# Research source and generated artifacts

New files in `research/` are visible to Git by default. `research/.gitignore` excludes known generated artifacts, not everything outside a source-extension allowlist. Scripts with new extensions or no extension can be committed normally.

- Commit validation and experiment scripts together with their tests. Do not hide scripts merely because they are used only for research.
- Known model weights, datasets, HTTP headers, and generated JRA JSON captures stay local. See the explicit patterns in `.gitignore`.
- `runtime/`, dependency/cache directories, and `test-work-*`, `test-full-*`, and `test-model-*` directories are reserved for generated state. Keep authored code outside these directories.
- Existing tracked snapshots remain tracked; ignore rules neither delete nor untrack them.
- When a new generated format appears, review it and add a specific ignore rule if appropriate. Unknown files must remain visible rather than being silently hidden.
- Secrets must never be committed, including secrets embedded in source/config files.

Check source/artifact classification without running a research job:

```sh
bash research/test-gitignore.sh
```
