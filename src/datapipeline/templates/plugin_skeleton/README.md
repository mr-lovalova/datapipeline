# {{DIST_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) framework.

Quick start

- Try the demo plugin immediately (creates `./demo/`):
  - `python -m pip install -U jerry-thomas`
  - `jerry demo init`
  - `python -m pip install -e demo`
  - `jerry serve --dataset demo`

- When you're ready to create your own plugin:
  - `jerry plugin init {{DIST_NAME}}`
  - `python -m pip install -e {{DIST_NAME}}`
  - Add a source via CLI (transport-specific placeholders are scaffolded):
    - File data: `jerry source create <provider>.<dataset> -t fs -f <csv|json|json-lines|pickle>`
    - HTTP data: `jerry source create <provider>.<dataset> -t http -f <json|json-lines|csv>`
    - Synthetic: `jerry source create <provider>.<dataset> -t synthetic`
  - Edit `your-dataset/sources/*.yaml` to fill in the `path`, delimiter, etc.

Notes
- `jerry.yaml` is placed in your workspace root (alongside plugin folders) so you can run CLI commands from there; `plugin_root` points at the active plugin.
- Reinstall after EP changes (`pyproject.toml`) and restart Python processes:
  - This plugin: `python -m pip install -e .`

Folder layout

- `your-dataset/`
  - `project.yaml` — project root (paths, globals, cadence/split)
  - `dataset.yaml` — feature/target declarations (uses `${group_by}` from globals)
  - `postprocess.yaml` — postprocess transforms
  - `contracts/*.yaml` — canonical stream definitions
  - `sources/*.yaml` — raw source definitions (one file per source)
  - `tasks/*.yaml` — task specs (schema/scaler/metadata/serve)
  - `reference/` — full option references for each config (not loaded by runtime)
    - `reference/reference/` — example config references
    - `reference/jerry.yaml` — workspace config reference (place next to repo root)
- Every dataset `project.yaml` declares a `name`; reference it via `${project_name}`
  inside other config files (e.g., `paths.artifacts: ../artifacts/${project_name}`) to
  avoid hard-coding per-dataset directories.
- `src/{{PACKAGE_NAME}}/`
  - `dtos/*.py` — DTO models (raw source shapes)
  - `parsers/*.py` — parse raw → DTO
  - `loaders/*.py` — custom loaders (optional; fs/http use core loader)
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

- Build artifacts once: `jerry build --project your-dataset/project.yaml`
- Preview records (stage 1): `jerry serve --project your-dataset/project.yaml --stage 1 --limit 100`
- Preview features (stage 3): `jerry serve --project your-dataset/project.yaml --stage 3 --limit 100`
- Preview vectors (stage 7): `jerry serve --project your-dataset/project.yaml --stage 7 --limit 100`

Pipeline stages (serve --stage)

Stages 0-5 operate on a single stream at a time (per feature/target config). Stages 6-7 assemble full vectors across all configured features.

- Stage 0 (DTO stream)
  - Input: source rows (loader transport + decoder)
  - Ops: loader -> decoder -> parser (raw -> DTO; return None to drop rows)
  - Output: DTO objects (or dicts) yielded by the parser

- Stage 1 (record stream)
  - Input: DTO stream
  - Ops: mapper (DTO -> domain TemporalRecord)
  - Output: TemporalRecord instances

- Stage 2 (record transforms)
  - Input: TemporalRecord stream
  - Ops: contract `record:` transforms (e.g. filter, floor_time); per-record only (no history)
  - Output: TemporalRecord stream (possibly filtered/mutated)

- Stage 3 (feature stream)
  - Input: TemporalRecord stream
  - Ops: wrap each record as `FeatureRecord(id, record)`; `id` is derived from:
    - contract `id:` (base feature id), and
    - optional `partition_by:` fields (entity-specific feature ids)
  - Output: FeatureRecord stream (not guaranteed sorted)

- Stage 4 (regularize + stream/debug transforms)
  - Input: FeatureRecord stream
  - Ops:
    - sort by `(feature_id, record.time)` (batch/in-memory sort; the main expensive step)
    - apply contract `stream:` transforms (per-feature history; e.g. ensure_cadence, rolling, fill)
    - apply contract `debug:` transforms (validation only; e.g. lint)
  - Output: FeatureRecord stream (sorted by id,time)

- Stage 5 (feature transforms)
  - Input: FeatureRecord stream (sorted by id,time)
  - Ops: dataset-level feature transforms configured per feature (e.g. `scale`, `sequence`)
  - Output: FeatureRecord or FeatureRecordSequence

- Stage 6 (vector assembly)
  - Input: all features/targets after stage 5
  - Ops:
    - merge feature streams by time bucket (`group_by`)
    - assemble `Vector` objects (feature_id -> value or sequence)
    - assemble `Sample(key, features, targets)`
    - if rectangular mode is on, align to the expected time window keys (missing buckets become empty vectors)
  - Output: Sample stream (no postprocess, no split)

- Stage 7 (postprocess)
  - Input: Sample stream
  - Ops:
    - ensure vector schema (fill missing configured feature ids, drop extras)
    - apply project `postprocess.yaml` vector transforms
  - Output: Sample stream (still not split)

Full run (no --stage)

- Runs stages 0-7, then applies the configured train/val/test split and optional throttling, then writes output.

Split timing

- Train/val/test split happens after vector assembly (stage 6) and after postprocess (stage 7).
- Feature engineering should be causal (no look-ahead) since it runs before the split.

Transform scope (mental model)

- Record transforms: operate on a single record only (no history, no look‑ahead).
- Stream transforms: operate on ordered feature streams (can use history per feature id).
- Postprocess transforms: operate on fully assembled vectors (can inspect multiple features at once).

Build tasks (when and why)

- `schema.yaml`: scans vectors to infer feature IDs/types and time bounds.
- `metadata.yaml`: collects additional stats for reporting/debugging.
- `scaler.yaml`: fits scaling stats (should use train split).
- Build tasks run before `serve` unless `--skip-build` is set.
- Build tasks run the vector pipeline up through stage 6 (vector assembly). They do not apply postprocess (stage 7) and do not apply the train/val/test split.
- If you change feature engineering (contracts, postprocess, dataset feature list), rebuild so schema/scaler artifacts match.

Custom components (where they are used)

- Loaders: source IO (Stage 0) — read raw bytes/rows.
- Parsers: raw → DTO (Stage 0) — validate/shape source rows.
- Mappers: DTO → domain record (Stage 1) — domain mapping + light derived fields.
- Record transforms: Stage 2 — per‑record cleanup/filters (no history).
- Stream transforms: Stage 4 — per‑feature history/rolling, fills, etc.
- Feature transforms: Stage 5 — scaling and sequencing configured per feature in `dataset.yaml`.
- Postprocess transforms: Stage 7 — vector‑level cleanup/fill/feature drops.
- Entry points are declared in `pyproject.toml` under `[project.entry-points."datapipeline.*"]`.

Analyze vectors

- `jerry inspect report   --project your-dataset/project.yaml` (console only)
- `jerry inspect partitions --project your-dataset/project.yaml` (writes build/partitions.json)
- `jerry inspect matrix   --project your-dataset/project.yaml --format html` (writes build/matrix.html)
- Use post-processing transforms in `postprocess.yaml` to keep coverage high
  (history/horizontal fills, constants, or drop rules) before serving vectors.
  Add `payload: targets` inside a transform when you need to mutate label vectors.

Train/Val/Test splits (deterministic)

- Configure split mechanics once in your project file:
  - Edit `your-dataset/project.yaml` and set:
    ```yaml
    globals:
      group_by: 10m # dataset cadence; reused as contract cadence
      split:
        mode: hash # hash|time
        key: group # group or feature:<id> (entity-stable)
        seed: 42 # deterministic hash seed
        ratios: { train: 0.8, val: 0.1, test: 0.1 }
    ```
- Select the active slice via `your-dataset/tasks/serve.<name>.yaml` (or `--keep`):
  ```yaml
  kind: serve
  name: train # defaults to filename stem when omitted
  keep: train # any label defined in globals.split; null disables filtering
  output:
    transport: stdout # stdout | fs
    format: print # print | json-lines | json | csv | pickle
  limit: 100 # cap vectors per serve run (null = unlimited)
  throttle_ms: null # sleep between vectors (milliseconds)
  # visuals: AUTO  # AUTO | TQDM | RICH | OFF
  # progress: AUTO # AUTO | SPINNER | BARS | OFF
  ```
- Add additional `kind: serve` files (e.g., `serve.val.yaml`, `serve.test.yaml`) and the CLI will run each enabled file in order unless you pass `--run <name>`.
- Serve examples (change the serve task or pass `--keep val|test`):
  - `jerry serve -p your-dataset/project.yaml --out-transport stdout --out-format json-lines > train.jsonl`
  - `jerry serve -p your-dataset/project.yaml --keep val --out-transport stdout --out-format json-lines > val.jsonl`
  - Add `--visuals rich --progress bars` for a richer interactive UI; defaults to `AUTO`.
- For shared workspace defaults (visual renderer, progress display, build mode), drop a `jerry.yaml` next to your workspace root and set `shared.visuals`, `shared.progress`, etc. CLI commands walk up from the current directory to find it.
- The split is applied at the end (after postprocess transforms), and assignment
  is deterministic (hash-based) with a fixed seed; no overlap across runs.

Key selection guidance

- `key: group` hashes the group key (commonly the time bucket). This yields a uniform random split per group but may allow the same entity to appear in multiple splits across time.
- `key: feature:<id>` hashes a specific feature value, e.g., `feature:entity_id` or `feature:station_id`, ensuring all vectors for that entity land in the same split (recommended to avoid leakage).

Scaler statistics

- Jerry computes scaler stats automatically. If you need custom paths or settings, add `tasks/scaler.yaml` and override the defaults.
- The build writes `<paths.artifacts>/scaler.pkl`; runtime scaling requires this artifact. If it is missing, scaling transforms raise a runtime error.

Tips

- Keep parsers thin — mirror source schema and return DTOs; use the identity parser only if your loader already emits domain records.
- Prefer small, composable configs over monolithic ones: one YAML per source is easier to review and reuse.

Composed streams (engineered domains)

- Declare engineered streams that depend on other canonical streams directly in contracts. The runtime builds each input to stage 4, stream‑aligns by partition+timestamp, runs your composer, and emits fresh records for the derived stream.

```yaml
# your-dataset/contracts/air_density.processed.yaml
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
# your-dataset/dataset.yaml
group_by: ${group_by}
features:
  - id: air_density
    record_stream: air_density.processed
```
