# Datapipeline Runtime

Named after the famous bartender, Jerry Thomas is a time-series-first data
pipeline runtime that mixes disparate data sources into fresh, ready-to-serve
vectors using declarative YAML recipes. Everything is on-demand, iterator-first:
data streams through the pipeline without pre-batching the whole dataset in
memory. Like any good bartender, Jerry obsesses over quality control and
service, offering stage-by-stage observability along the way. And no bar is
complete without proper tools: deterministic artifacts and plugin scaffolding
for custom loaders, parsers, transforms, and filters.

Contributing: PRs welcome on [GitHub](https://github.com/mr-lovalova/datapipeline).

> **Core assumptions**
>
> - Every record carries a timezone-aware `time` attribute and a numeric
>   `value`. The time-zone awareness is a quality gate to ensure correct vector assembly.
> - Grouping is purely temporal. Dimensional splits belong in `partition_by`.

---

## Why You Might Use It

- Materialize canonical time-series datasets from disparate sources.
- Preview and debug each stage of the pipeline without writing ad-hoc scripts.
- Enforce coverage/quality gates and publish artifacts (schema, scaler stats)
  for downstream ML teams.
- Extend the runtime with entry-point driven plugins for domain-specific I/O or
  feature engineering.
- Consume vectors directly from Python via iterators, Pandas DataFrames, or
  `torch.utils.data.Dataset`.

---

## Quick Start

### Serve The Demo Plugin (Recommended)

```bash
python -m pip install -U jerry-thomas
jerry demo init
python -m pip install -e demo
jerry serve --dataset demo --limit 3
```

Note: `jerry demo init` creates a workspace `jerry.yaml`. If you later run
`jerry plugin init`, it won’t overwrite that file. Remove or edit
`jerry.yaml` (or pass `--project`) to point at your new plugin.
For example: `jerry serve --project lib/my-datapipeline/project.yaml`.

### Create Your Own Plugin + First Ingest

```bash
jerry plugin init my-datapipeline --out lib/

# Note: import paths use the package name (hyphens become underscores), e.g.
# `my_datapipeline` even if the dist folder is `my-datapipeline`.

# One-stop wizard: scaffolds source YAML + DTO/parser + domain + mapper + contract.
jerry inflow create

# See `docs/cli.md` for wizard tips and identity vs custom guidance.

# Reinstall after commands that update entry points (pyproject.toml).
python -m pip install -e lib/my-datapipeline

jerry serve --dataset your-dataset --limit 3
```

---

## Pipeline Stages (serve --stage)

Stages 0-6 operate on a single stream at a time (per feature/target config). Stages 7-8 assemble full vectors across all configured features.

- Stage 0 (DTO stream)
  - Input: raw source rows (loader transport + decoder)
  - Ops: loader -> decoder -> parser (raw -> DTO; return None to drop rows)
  - Output: DTO objects yielded by the parser

- Stage 1 (record stream)
  - Input: DTO stream
  - Ops: mapper (DTO -> domain TemporalRecord)
  - Output: TemporalRecord instances (must have timezone-aware `time`)

- Stage 2 (record transforms)
  - Input: TemporalRecord stream
  - Ops: contract `record:` transforms (e.g. filter, floor_time); per-record only (no history)
  - Output: TemporalRecord stream (possibly filtered/mutated)

- Stage 3 (ordered record stream)
  - Input: TemporalRecord stream
  - Ops:
    - sort by `(partition_key, record.time)` (batch/in-memory sort; typically the expensive step)
  - Output: TemporalRecord stream (sorted by partition,time)

- Stage 4 (stream transforms)
  - Input: ordered TemporalRecord stream
  - Ops:
    - apply contract `stream:` transforms (per-partition history; e.g. ensure_cadence, rolling, fill)
    - apply contract `debug:` transforms (validation only; e.g. lint)
  - Output: TemporalRecord stream (sorted by partition,time)

- Stage 5 (feature stream)
  - Input: TemporalRecord stream
  - Ops: wrap each record as `FeatureRecord(id, record, value)`; `id` is derived from:
    - dataset `id:` (base feature id), and
    - optional `partition_by:` fields (entity-specific feature ids)
    - `value` is selected from `dataset.yaml` via `field: <record_attr>`
  - Output: FeatureRecord stream (sorted by id,time within partitions)

- Stage 6 (feature transforms)
  - Input: FeatureRecord stream (sorted by id,time)
  - Ops: dataset-level feature transforms configured per feature (e.g. `scale`, `sequence`)
  - Output: FeatureRecord or FeatureRecordSequence

- Stage 7 (vector assembly)
  - Input: all features/targets after stage 6
  - Ops:
    - merge feature streams by time bucket (`group_by`)
    - assemble `Vector` objects (feature_id -> value or sequence)
    - assemble `Sample(key, features, targets)`
    - if rectangular mode is on, align to the expected time window keys (missing buckets become empty vectors)
  - Output: Sample stream (no postprocess, no split)

- Stage 8 (postprocess)
  - Input: Sample stream
  - Ops:
    - ensure vector schema (fill missing configured feature ids, drop extras)
    - apply project `postprocess.yaml` vector transforms
  - Output: Sample stream (still not split)

Full run (no --stage)

- Runs stages 0-8, then applies the configured train/val/test split and optional throttling, then writes output.

Split timing (leakage note)

- Split is applied after stage 8 in `jerry serve` (postprocess runs before split).
- Feature engineering runs before split; keep it causal (no look-ahead, no future leakage).
- Scaler statistics are fit by the build task `scaler.yaml` and are typically restricted to the `train` split (configurable via `split_label`).

---

## CLI Cheat Sheet

- `jerry demo init`: scaffolds a standalone demo plugin at `./demo/` and wires a `demo` dataset.
- `jerry plugin init <name> --out lib/`: scaffolds `lib/<name>/` (writes workspace `jerry.yaml` when missing).
- `jerry.yaml`: sets `plugin_root` for scaffolding commands and `datasets/default_dataset` so you can omit `--project`/`--dataset`.
- `jerry serve [--dataset <alias>|--project <path>] [--limit N] [--stage 0-8] [--skip-build]`: streams output; builds required artifacts unless `--skip-build`.
- `jerry build [--dataset <alias>|--project <path>] [--force]`: materializes artifacts (schema, scaler, etc.).
- `jerry inspect report|matrix|partitions [--dataset <alias>|--project <path>]`: quality and metadata helpers.
- `jerry inflow create`: interactive wizard to scaffold an end-to-end ingest stream (source + parser/DTO + mapper + contract).
- `jerry source create <provider>.<dataset> ...`: scaffolds a source YAML (no Python code).
- `jerry domain create <domain>`: scaffolds a domain record stub.
- `jerry dto create`, `jerry parser create`, `jerry mapper create`, `jerry loader create`: scaffold Python code + register entry points (reinstall after).
- `jerry contract create [--identity]`: interactive contract scaffolder (YAML); use for canonical streams or composed streams.
- `jerry list sources|domains|parsers|mappers|loaders|dtos`: introspection helpers.
- `pip install -e lib/<name>`: rerun after commands that update `lib/<name>/pyproject.toml` (entry points), or after manual edits to it.

---

## MLOps & Reproducibility

- `jerry build` materializes deterministic artifacts (schema, scaler, metadata).
  Builds are keyed by config hashes and skip work when nothing changed unless
  you pass `--force`.
- `jerry serve` runs are named (task/run) and can write outputs to
  `<out-path>/<run_name>/` for auditing, sharing, or downstream training.
- Versioning: tag the project config + plugin code in Git and pair with a data
  versioning tool like DVC for raw sources. With those inputs pinned, interim
  datasets and artifacts can be regenerated instead of stored.

---

## Concepts

### Workspace (`jerry.yaml`)

- `datasets`: dataset aliases → `project.yaml` paths (relative to `jerry.yaml`).
- `default_dataset`: which dataset `jerry serve/build/inspect` use when you omit `--dataset/--project`.
- `plugin_root`: where scaffolding commands write Python code (`src/<package>/...`) and where they look for `pyproject.toml`.

### Plugin Package (Python Code)

These live under `lib/<plugin>/src/<package>/`:

- `dtos/*.py`: DTO models (raw source shapes).
- `parsers/*.py`: raw -> DTO parsers (referenced by source YAML via entry point).
- `domains/<domain>/model.py`: domain record models.
- `mappers/*.py`: DTO -> domain record mapping functions (referenced by contracts via entry point).
- `loaders/*.py`: optional custom loaders (fs/http usually use the built-in core loader).
- `pyproject.toml`: entry points for loaders/parsers/mappers/transforms (rerun `pip install -e lib/<plugin>` after changes).

### Loaders & Parsers

- A **loader** yields raw rows (bytes/dicts) from some transport (FS/HTTP/synthetic/etc.).
- A **parser** turns each raw row into a typed DTO (or returns `None` to drop a row).
- In most projects, your source YAML uses the built-in loader `core.io` and you only customize its `args` (`transport`, `format`, and a `path`/`url`).
- You typically only implement a custom loader when you need specialized behavior (auth/pagination/rate limits, proprietary formats, or non-standard protocols).
- `parser.args` are optional and only used when your parser supports configuration; many parsers don’t need any args since filtering etc is supported natively downstream.

### DTOs & Domains

- A **DTO** (Data Transfer Object) mirrors a single source’s schema (columns/fields) and stays “raw-shaped”; it’s what parsers emit.
- A **domain record** is the canonical shape used across the pipeline. Mappers convert DTOs into domain records so multiple sources can land in the same domain model.
- The base time-series type is `TemporalRecord` (`time` + metadata fields). Domains add identity fields (e.g. `symbol`, `station_id`) that make filtering/partitioning meaningful.
- `time` must be timezone-aware (normalized to UTC); feature values are selected from record fields in `dataset.yaml` (see `field:`); remaining fields act as the record’s “identity” (used by equality/deduping and commonly by `partition_by`).

### Transforms (Record → Stream → Feature → Vector)

- **Record transforms** run on raw canonical records before sorting or grouping (filters, time flooring, lagging). Each transform operates on one record at a time because order and partitions are not established yet. Configure in `contracts/*.yaml` under `record:`.
- **Stream transforms** run on ordered, per-stream records after record transforms (dedupe, cadence enforcement, rolling fills). These operate across a sequence of records for a partition because they depend on sorted partition/time order and cadence. Configure in `contracts/*.yaml` under `stream:`.
- **Feature transforms** run after stream regularization and shape the per-feature payload for vectorization (scalers, sequence/windowing). These occur after feature ids are finalized and payloads are wrapped. Configure in `dataset.yaml` under each feature.
- **Vector (postprocess) transforms** operate on assembled vectors (coverage/drop/fill/replace). Configure in `postprocess.yaml`.
- **Debug transforms** run after stream transforms for validation only. Configure in `contracts/*.yaml` under `debug:`.
- Custom transforms are registered in your plugin `pyproject.toml` under the matching entry-point group:
  - `datapipeline.transforms.record`
  - `datapipeline.transforms.stream`
  - `datapipeline.transforms.feature`
  - `datapipeline.transforms.vector`
  - `datapipeline.transforms.debug`
    Then reference them by name in the YAML.

### Glossary

- **Source alias**: `sources/*.yaml:id` (referenced by contracts under `source:`).
- **Stream id**: `contracts/*.yaml:id` (referenced by `dataset.yaml` under `record_stream:`).
- **Partition**: dimension keys appended to feature IDs, driven by `contract.partition_by`.
- **Group**: vector “bucket” cadence set by `dataset.group_by` (controls how records become samples).
- **Stage**: debug/preview level for `jerry serve --stage 0-8` (DTOs → domain records → features → vectors).
- **Fan-out**: when multiple features reference the same `record_stream`, the pipeline spools records to disk so each feature can read independently (records must be picklable).

## Documentation

- `docs/config.md`: config layout, resolution order, and YAML reference.
- `docs/cli.md`: CLI reference (beyond the cheat sheet).
- `docs/transforms.md`: built-in transforms and filters.
- `docs/artifacts.md`: artifacts, postprocess, and split timing.
- `docs/python.md`: Python API usage patterns.
- `docs/extending.md`: entry points and writing plugins.
- `docs/architecture.md`: pipeline diagrams.

## Development

See `CONTRIBUTING.md`.
