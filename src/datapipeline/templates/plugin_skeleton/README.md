# {{PACKAGE_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) framework.

Quick start
- Initialize a plugin (already done if you’re reading this here):
- `jerry plugin init --name {{PACKAGE_NAME}}`
- Add a source via CLI (transport-specific placeholders are scaffolded):
  - File data: `jerry source add -p <provider> -d <dataset> -t fs -f <csv|json|json-lines>`
  - URL data: `jerry source add -p <provider> -d <dataset> -t url -f <json|json-lines|csv>`
  - Synthetic: `jerry source add -p <provider> -d <dataset> -t synthetic`
- Edit the generated `config/sources/*.yaml` to fill in the `path`, delimiter, etc.
- Reinstall after EP changes (pyproject.toml) and restart Python processes:
  - Core: `cd lib/datapipeline && python -m pip install -e .`
  - This plugin: `python -m pip install -e .`

Folder layout
- `config/`
  - `sources/*.yaml` — raw source definitions (one file per source)
  - `contracts/*.yaml` — canonical stream definitions
  - `datasets/<name>/build.yaml` — build configuration (partitioned ids today, more artifacts later)
- `src/{{PACKAGE_NAME}}/`
  - `sources/<provider>/<dataset>/dto.py` — DTO model for the source
  - `sources/<provider>/<dataset>/parser.py` — parse raw → DTO
  - Optional: `sources/<provider>/<dataset>/loader.py` for synthetic sources
  - `domains/<domain>/model.py` — domain record models
  - `mappers/*.py` — map DTOs → domain records

How loaders work
- For fs/url, sources use the generic loader entry point:
  - `loader.entrypoint: "{{COMPOSED_LOADER_EP}}"`
- `loader.args` include `transport`, `format`, and source-specific args (placeholders are provided):
    - fs: `path`, `glob`, `encoding`, plus `delimiter` for csv
    - url: `url`, `headers`, `encoding`, optional `count_by_fetch`
- Synthetic sources generate data in-process and keep a small loader stub.

Run data flows
- Build artifacts once: `jerry build --project config/datasets/default/project.yaml`
- Records: `jerry prep pour -p config/datasets/default/project.yaml -n 100`
- Features: `jerry prep build -p config/datasets/default/project.yaml -n 100`
- Vectors: `jerry prep stir -p config/datasets/default/project.yaml -n 100`

Analyze vectors
- `jerry inspect report   --project config/datasets/default/project.yaml` (console only)
- `jerry inspect coverage --project config/datasets/default/project.yaml` (writes build/coverage.json)
- `jerry inspect matrix   --project config/datasets/default/project.yaml --format html` (writes build/matrix.html)
- `jerry inspect partitions --project config/datasets/default/project.yaml` (writes build/partitions.json)
- Use post-processing `transforms` in `postprocess.yaml` to keep coverage high
  (history/horizontal fills, constants, or drop rules) before serving vectors.

Train/Val/Test splits (deterministic)
- Configure splits once in your project file (single source of truth):
  - Edit `config/datasets/default/project.yaml` and set:
    ```yaml
    globals:
      split:
        keep: train           # train|val|test
        mode: hash            # hash|time
        key: group            # group or feature:<id> (entity-stable)
        seed: 42              # deterministic hash seed
        ratios: {train: 0.8, val: 0.1, test: 0.1}
    ```
- Serve examples (change `keep` between runs to materialize each split):
  - `jerry run serve -p config/datasets/default/project.yaml -o stream > train.jsonl`
  - Edit `keep: val`, then run again → `val.jsonl`
  - Edit `keep: test`, then run again → `test.jsonl`
- The split is applied at the end (after postprocess transforms), and assignment
  is deterministic (hash-based) with a fixed seed; no overlap across runs.

Key selection guidance
- `key: group` hashes the group key (commonly the time bucket). This yields a uniform random split per group but may allow the same entity to appear in multiple splits across time.
- `key: feature:<id>` hashes a specific feature value, e.g., `feature:entity_id` or `feature:station_id`, ensuring all vectors for that entity land in the same split (recommended to avoid leakage).

Postprocess expected IDs
- Build once with `jerry build --project config/datasets/default/project.yaml` (or run `jerry inspect expected …`) to materialize `<paths.artifacts>/expected.txt`.
- Bootstrap registers the artifact; postprocess transforms read it automatically. Per-transform `expected:` overrides are no longer required or supported — the build output is the single source of truth.

Scaler statistics
- Enable the scaler task in `build.yaml` (default `enabled: true`) to compute mean/std per feature using the configured training split.
- The build writes `<paths.artifacts>/scaler.pkl`; runtime scaling requires this artifact. If it is missing, scaling transforms raise a runtime error.

Tips
- Keep parsers thin — mirror source schema and return DTOs; use the identity parser only if your loader already emits domain records.
- Prefer small, composable configs over monolithic ones: one YAML per source is easier to review and reuse.
