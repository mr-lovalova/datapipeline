# Datapipeline Runtime

Jerry Thomas is a time-series-first data pipeline runtime. It turns declarative
YAML projects into iterators that stream records, engineered features, and
model-ready vectors. The CLI lets you preview every stage, build deterministic
artifacts, inspect quality, and scaffold plugins for custom loaders, parsers,
transforms, and filters.

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
python -m pip install -e lib/my-datapipeline

# One-stop wizard: scaffolds source YAML + DTO/parser + domain + mapper + contract.
jerry inflow create

# Wizard tips (identity vs custom)
#
# - Parser:
#   - Choose "Identity parser" when the loader already yields dicts/objects that match your DTO shape,
#     and no type conversion is needed.
#   - Choose "Create new parser" when you need to parse timestamps, coerce types, rename fields,
#     or drop/validate rows.
#
# - Mapper:
#   - Choose "Identity mapper" only when your DTO already is the final domain record shape (for example you might have used jerry to output interim datasets):
#     `time` is timezone-aware, `value` is numeric, and identity fields are present.
#   - Otherwise, choose "Create new mapper" to map DTO -> domain record and add light derived fields.
#
# After scaffolding, you typically still need to:
# - Fill placeholders in `sources/*.yaml` (path/url/headers/etc.).
# - Reference your stream contract id in `dataset.yaml` under `record_stream: <contract_id>`.
#
# Reinstall after commands that update entry points (pyproject.toml).
python -m pip install -e lib/my-datapipeline

jerry serve --dataset your-dataset --limit 3
```

---

## Pipeline Stages (serve --stage)

Stages 0-5 operate on a single stream at a time (per feature/target config). Stages 6-7 assemble full vectors across all configured features.

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

- Stage 3 (feature stream)
  - Input: TemporalRecord stream
  - Ops: wrap each record as `FeatureRecord(id, record)`; `id` is derived from:
    - contract `id:` (base feature id), and
    - optional `partition_by:` fields (entity-specific feature ids)
  - Output: FeatureRecord stream (not guaranteed sorted)

- Stage 4 (regularize + stream/debug transforms)
  - Input: FeatureRecord stream
  - Ops:
    - sort by `(feature_id, record.time)` (batch/in-memory sort; typically the expensive step)
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

Split timing (leakage note)

- Split is applied after stage 7 in `jerry serve` (postprocess runs before split).
- Feature engineering runs before split; keep it causal (no look-ahead, no future leakage).
- Scaler statistics are fit by the build task `scaler.yaml` and are typically restricted to the `train` split (configurable via `split_label`).

---

## CLI Cheat Sheet

- `jerry demo init`: scaffolds a standalone demo plugin at `./demo/` and wires a `demo` dataset.
- `jerry plugin init <name> --out lib/`: scaffolds `lib/<name>/` (writes workspace `jerry.yaml` when missing).
- `jerry.yaml`: sets `plugin_root` for scaffolding commands and `datasets/default_dataset` so you can omit `--project`/`--dataset`.
- `jerry serve [--dataset <alias>|--project <path>] [--limit N] [--stage 0-7] [--skip-build]`: streams output; builds required artifacts unless `--skip-build`.
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
- The base time-series type is `TemporalRecord` (`time` + `value`). Domains typically add identity fields (e.g. `symbol`, `station_id`) that make filtering/partitioning meaningful.
- `time` must be timezone-aware (normalized to UTC); `value` is the measurement you engineer features from; all other fields act as the record’s “identity” (used by equality/deduping and commonly by `partition_by`).

### Transforms (Record → Stream → Feature → Vector)

- **Record transforms** run on raw canonical records before sorting or grouping (filters, time flooring, lagging). Each transform operates on one record at a time because order and partitions are not established yet. Configure in `contracts/*.yaml` under `record:`.
- **Stream transforms** run on ordered, per-feature streams after feature wrapping (dedupe, cadence enforcement, rolling fills). These can, unlike record transforms, operate across a sequence of records for a given feature because they depend on sorted id/time order and cadence. Configure in `contracts/*.yaml` under `stream:`.
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
- **Stage**: debug/preview level for `jerry serve --stage 0-7` (DTOs → domain records → features → vectors).

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
