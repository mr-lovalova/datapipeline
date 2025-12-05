# {{DIST_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) framework.

Quick start
- Initialize a plugin (already done if you’re reading this here):
- `jerry plugin init {{DIST_NAME}}`
- Add a source via CLI (transport-specific placeholders are scaffolded):
  - File data: `jerry source add <provider> <dataset> -t fs -f <csv|json|json-lines|pickle>`
  - HTTP data: `jerry source add <provider>.<dataset> -t http -f <json|json-lines|csv>`
  - Synthetic: `jerry source add -p <provider> -d <dataset> -t synthetic`
- Edit the generated `config/sources/*.yaml` to fill in the `path`, delimiter, etc.
- Reinstall after EP changes (pyproject.toml) and restart Python processes:
  - Core: `cd lib/datapipeline && python -m pip install -e .`
  - This plugin: `python -m pip install -e .`

Folder layout
- `example/`
  - `project.yaml` — project root (paths, globals, cadence/split)
  - `dataset.yaml` — feature/target declarations (uses `${group_by}` from globals)
  - `postprocess.yaml` — postprocess transforms
  - `contracts/*.yaml` — canonical stream definitions
  - `sources/*.yaml` — raw source definitions (one file per source)
  - `tasks/*.yaml` — task specs (schema/scaler/metadata/serve)
- Every dataset `project.yaml` declares a `name`; reference it via `${project_name}`
  inside other config files (e.g., `paths.artifacts: ../artifacts/${project_name}`) to
  avoid hard-coding per-dataset directories.
- `src/{{PACKAGE_NAME}}/`
  - `sources/<provider>/<dataset>/dto.py` — DTO model for the source
  - `sources/<provider>/<dataset>/parser.py` — parse raw → DTO
  - Optional: `sources/<provider>/<dataset>/loader.py` for synthetic sources
  - `domains/<domain>/model.py` — domain record models
  - `mappers/*.py` — map DTOs → domain records

How loaders work
- For fs/http, sources use the generic loader entry point:
  - `loader.entrypoint: "{{DEFAULT_IO_LOADER_EP}}"`
- `loader.args` include `transport`, `format`, and source-specific args (placeholders are provided):
    - fs: `path`, `glob`, `encoding`, plus `delimiter` for csv
    - http: `url`, `headers`, `params`, `encoding`, optional `count_by_fetch`
- Synthetic sources generate data in-process and keep a small loader stub.

Run data flows
- Build artifacts once: `jerry build --project example/project.yaml`
- Preview records (stage 1): `jerry serve --project example/project.yaml --stage 1 --limit 100`
- Preview features (stage 3): `jerry serve --project example/project.yaml --stage 3 --limit 100`
- Preview vectors (stage 7): `jerry serve --project example/project.yaml --stage 7 --limit 100`

Analyze vectors
- `jerry inspect report   --project example/project.yaml` (console only)
- `jerry inspect partitions --project example/project.yaml` (writes build/partitions.json)
- `jerry inspect matrix   --project example/project.yaml --format html` (writes build/matrix.html)
- `jerry inspect expected --project example/project.yaml` (writes build/expected.txt)
- Use post-processing transforms in `postprocess.yaml` to keep coverage high
  (history/horizontal fills, constants, or drop rules) before serving vectors.
  Add `payload: targets` inside a transform when you need to mutate label vectors.

Train/Val/Test splits (deterministic)
- Configure split mechanics once in your project file:
  - Edit `example/project.yaml` and set:
    ```yaml
    globals:
      group_by: 10m          # dataset cadence; reused as contract cadence
      split:
        mode: hash            # hash|time
        key: group            # group or feature:<id> (entity-stable)
        seed: 42              # deterministic hash seed
        ratios: {train: 0.8, val: 0.1, test: 0.1}
    ```
- Select the active slice via `example/tasks/serve.<name>.yaml` (or `--keep`):
  ```yaml
  kind: serve
  name: train               # defaults to filename stem when omitted
  keep: train               # any label defined in globals.split; null disables filtering
  output:
    transport: stdout       # stdout | fs
    format: print           # print | json-lines | json | csv | pickle
  limit: 100                # cap vectors per serve run (null = unlimited)
  throttle_ms: null         # sleep between vectors (milliseconds)
  # visuals: AUTO  # AUTO | TQDM | RICH | OFF
  # progress: AUTO # AUTO | SPINNER | BARS | OFF
  ```
- Add additional `kind: serve` files (e.g., `serve.val.yaml`, `serve.test.yaml`) and the CLI will run each enabled file in order unless you pass `--run <name>`.
- Serve examples (change the serve task or pass `--keep val|test`):
  - `jerry serve -p example/project.yaml --out-transport stdout --out-format json-lines > train.jsonl`
  - `jerry serve -p example/project.yaml --keep val --out-transport stdout --out-format json-lines > val.jsonl`
  - Add `--visuals rich --progress bars` for a richer interactive UI; defaults to `AUTO`.
- For shared workspace defaults (visual renderer, progress display, build mode), drop a `jerry.yaml` next to your workspace root and set `shared.visuals`, `shared.progress`, etc. CLI commands walk up from the current directory to find it.
- The split is applied at the end (after postprocess transforms), and assignment
  is deterministic (hash-based) with a fixed seed; no overlap across runs.

Key selection guidance
- `key: group` hashes the group key (commonly the time bucket). This yields a uniform random split per group but may allow the same entity to appear in multiple splits across time.
- `key: feature:<id>` hashes a specific feature value, e.g., `feature:entity_id` or `feature:station_id`, ensuring all vectors for that entity land in the same split (recommended to avoid leakage).

Postprocess expected IDs
- Build once with `jerry build --project config/project.yaml` (or run `jerry inspect expected …`) to materialize `<paths.artifacts>/expected.txt`.
- Bootstrap registers the artifact; postprocess transforms read it automatically. Per-transform `expected:` overrides are no longer required or supported — the build output is the single source of truth.

Scaler statistics
- Jerry computes scaler stats automatically. If you need custom paths or settings, add `tasks/scaler.yaml` and override the defaults.
- The build writes `<paths.artifacts>/scaler.pkl`; runtime scaling requires this artifact. If it is missing, scaling transforms raise a runtime error.

Tips
- Keep parsers thin — mirror source schema and return DTOs; use the identity parser only if your loader already emits domain records.
- Prefer small, composable configs over monolithic ones: one YAML per source is easier to review and reuse.

Composed streams (engineered domains)
- Declare engineered streams that depend on other canonical streams directly in contracts. The runtime builds each input to stage 4, stream‑aligns by partition+timestamp, runs your composer, and emits fresh records for the derived stream.

```yaml
# example/contracts/air_density.processed.yaml
kind: composed
id: air_density.processed
inputs:
  - p=pressure.processed
  - t=temp_dry.processed
partition_by: station_id
sort_batch_size: 20000

mapper:
  entrypoint: {{PACKAGE_NAME}}.mappers.air_density:mapper
  args:
    driver: p   # optional; defaults to first input alias

# Optional post‑compose policies (same as any stream):
# record: [...]
# stream: [...]
# debug: [...]
```

Then reference the composed stream in your dataset:

```yaml
# example/dataset.yaml
group_by: ${group_by}
features:
  - id: air_density
    record_stream: air_density.processed
```
