# Datapipeline Runtime

Jerry Thomas is a time-series-first data pipeline runtime. It turns declarative
YAML projects into iterators that stream records, engineered features, and
model-ready vectors. The CLI lets you preview every stage, build deterministic
artifacts, inspect coverage, and scaffold plugins for custom loaders, parsers,
transforms, and filters.

> **Core assumptions**
>
> - Every record carries a timezone-aware `time` attribute and a numeric
>   `value`.
> - Grouping is purely temporal. Dimensional splits belong in `partition_by`.

---

## Why You Might Use It

- Materialize canonical time-series datasets from disparate sources.
- Preview and debug each stage of the pipeline without writing ad-hoc scripts.
- Enforce coverage/quality gates and publish artifacts (expected IDs, scaler
  stats) for downstream ML teams.
- Extend the runtime with entry-point driven plugins for domain-specific I/O or
  feature engineering.
- Consume vectors directly from Python via iterators, Pandas DataFrames, or
  `torch.utils.data.Dataset`.

---

## Quick Start

```bash
# 1. Install in editable mode (with optional dev extras for testing).
pip install -e .[dev]

# 2. Bootstrap a project (scaffolds configs, plugin package, and templates).
jerry plugin init --name my_datapipeline --out .

# 3. Create a source & domain scaffold, then declare a canonical stream.
jerry source add --provider demo --dataset weather --transport fs --format csv
jerry domain add --domain weather
# (edit config/contracts/<alias>.yaml to point at your mapper and policies)

# 4. Configure dataset/postprocess/build files in config/datasets/<name>/.
#    Then preview the pipeline and serve a few vectors:
jerry serve --project config/datasets/default/project.yaml --stage 2 --limit 5
jerry serve --project config/datasets/default/project.yaml --output print --limit 3

# 5. Inspect coverage and build artifacts:
jerry inspect report --project config/datasets/default/project.yaml
jerry build --project config/datasets/default/project.yaml
```

The skeleton project in `src/datapipeline/templates/plugin_skeleton/` mirrors the
paths expected by the CLI. Copy it or run `jerry plugin init` to get a ready-made
layout with `config/`, `src/<package>/`, and entry-point stubs.

---

## Pipeline Architecture

```text
raw source ──▶ canonical stream ──▶ record stage ──▶ feature stage ──▶ vector stage
```

1. **Raw sources** pair a loader with a parser. Loaders fetch bytes (file system,
   HTTP, synthetic generators). Parsers turn those bytes into typed DTOs.
   Register them via entry points (`loaders`, `parsers`) and declaratively wire
   them in `config/sources/*.yaml`.
2. **Canonical streams** decorate raw sources with mappers and per-stream
   policies. Contract files under `config/contracts/` define record transforms,
   feature transforms, sort hints, and partitioning.
3. **Record stage** applies canonical policies to DTOs, turning them into
   `TemporalRecord` instances (tz-aware timestamp + numeric value).
4. **Feature stage** wraps records into `FeatureRecord`s, handles per-feature
   sorting, optional scaling, and sequence windows (`FeatureRecordSequence`).
5. **Vector stage** merges all feature streams, buckets them using `group_by`
   cadence (e.g., `1h`), and emits `(group_key, Vector)` pairs ready for
   downstream consumers.

The runtime (`src/datapipeline/runtime.py`) hosts registries for sources,
transforms, artifacts, and postprocess rules. The CLI constructs lightweight
`PipelineContext` objects to build iterators without mutating global state.

---

## Configuration Files

All project configuration lives under `config/datasets/<name>/` by default.

### `project.yaml`

```yaml
version: 1
name: default
paths:
  streams: ../../contracts
  sources: ../../sources
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  artifacts: ../../build/datasets/${project_name}
  build: build.yaml
  run: run.yaml
globals:
  start_time: 2021-01-01T00:00:00Z
  end_time: 2023-01-03T23:00:00Z
  split:
    mode: hash # hash | time
    key: group # group | feature:<id>
    seed: 42
    ratios: { train: 0.8, val: 0.1, test: 0.1 }
```

- `name` provides a stable identifier you can reuse inside config files via `${project_name}`.
- `paths.*` are resolved relative to the project file unless absolute; they also support `${var}` interpolation.
- `globals` provide values for `${var}` interpolation across YAML files. Datetime
  values are normalized to strict UTC `YYYY-MM-DDTHH:MM:SSZ`.
- `split` config defines how labels are assigned; the active label is selected by `run.yaml` or CLI `--keep`.
- `paths.run` may point to a single file (default) or a directory. When it is a directory,
  every `*.yaml` file inside is treated as a run config; `jerry serve` executes them
  sequentially in alphabetical order unless you pass `--run <name>` (filename stem).
- Label names are free-form: match whatever keys you declare in `split.ratios` (hash) or `split.labels` (time).

### `run.yaml`

```yaml
version: 1
keep: train # set to any label defined in globals.split (null disables filtering)
output:
  transport: stdout # stdout | fs
  format: print # print | json-lines
limit: 100 # cap vectors per serve run (null = unlimited)
include_targets: false
throttle_ms: null # sleep between vectors (milliseconds)
log_level: INFO # DEBUG=progress bars, INFO=spinner, WARNING=quiet (null inherits CLI)
```

- `keep` selects the currently served split. This file is referenced by `project.paths.run`.
- `output`, `limit`, `include_targets`, `throttle_ms`, and `log_level` provide defaults for `jerry serve`; CLI flags still win per invocation. For filesystem outputs, set `transport: fs`, `path: /path/to/file.jsonl`, and the desired `format`.
- Override `keep` (and other fields) per invocation via `jerry serve ... --keep val` etc.
- To manage multiple runs, point `project.paths.run` at a folder (e.g., `config/datasets/default/runs/`)
  and drop additional `*.yaml` files there. `jerry serve` will run each file in order; pass
  `--run train` to execute only `runs/train.yaml`.

### `config/sources/<alias>.yaml`

Each file defines a loader/parser pair exposed under `<alias>` (also the
`source_id` the rest of the pipeline references).

```yaml
source_id: demo_weather
loader:
  entrypoint: demo.csv_loader
  args:
    path: data/weather.csv
parser:
  entrypoint: demo.weather_parser
  args:
    timezone: UTC
```

### `config/contracts/<alias>.yaml`

Canonical stream contracts describe how the runtime should map and prepare a
source. `alias` normally matches the source alias; use folders to organize by
domain.

```yaml
source_id: demo_weather
stream_id: demo_weather

mapper:
  entrypoint: weather.domain.mapper
  args: {}

partition_by: station
sort_batch_size: 50000

record:
  - filter: { operator: ge, field: time, comparand: "${start_time}" }
  - filter: { operator: lt, field: time, comparand: "${end_time}" }
  - floor_time: { resolution: 10m }

stream:
  - ensure_ticks: { tick: 10m }
  - granularity: { mode: mean }
  - fill: { statistic: median, window: 6, min_samples: 2 }

debug:
  - lint: { mode: warn, tick: 10m }
```

- `record`: ordered record-level transforms (filters, floor/lag, custom
  transforms registered under the `record` entry-point group).
- `stream`: transforms applied after feature wrapping, still per base feature.
- `debug`: instrumentation-only transforms (linters, assertions).
- `partition_by`: optional keys used to suffix feature IDs (e.g., `temp__station=XYZ`).
- `sort_batch_size`: chunk size used by the in-memory sorter when normalizing
  order before stream transforms.

### `dataset.yaml`

Defines which canonical streams become features/targets and the vector bucketing.

```yaml
group_by: 1h

features:
  - id: temp_c
    record_stream: demo_weather
    scale: true
    sequence: { size: 6, stride: 1, tick: 10m }

targets:
  - id: precip
    record_stream: demo_weather
```

- `group_by` controls the cadence for vector partitioning (accepts `Xm|min|Xh`
  — minutes or hours).
- `scale: true` inserts the standard scaler feature transform (requires scaler
  stats artifact or inline statistics).
- `sequence` emits `FeatureRecordSequence` windows (size, stride, optional
  cadence enforcement via `tick`).

### `postprocess.yaml`

Project-scoped vector transforms that run after assembly and before serving.

```yaml
- drop_missing:
    required: [temp_c__station=001]
    min_coverage: 0.95
- fill_constant: { value: 0.0 }
- fill_history:
    statistic: median
    window: 48
    min_samples: 6
- fill_horizontal:
    statistic: mean
    min_samples: 2
```

- Vector transforms rely on artifacts (expected IDs, scaler stats) to decide
  which features should be present.
- When no transforms are configured the stream passes through unchanged.

### `build.yaml`

Declares which artifacts the build step should materialize.

```yaml
version: 1
partitioned_ids:
  output: expected.txt
  include_targets: false
scaler:
  enabled: true
  output: scaler.pkl
  include_targets: false
  split_label: train
```

- `expected.txt` lists every fully partitioned feature ID observed in the latest
  run (used by vector postprocess transforms).
- `scaler.pkl` is a pickled standard scaler fitted on the requested split.

---

## CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.

### Preview Stages

- `jerry serve --project <project.yaml> --stage <0-7> --limit N [--log-level LEVEL]`
  - Stage 0: raw DTOs
  - Stage 1: domain `TemporalRecord`s
  - Stage 2: record transforms applied
  - Stage 3: feature records (before sort/regularization)
  - Stage 4: feature regularization (post stream transforms)
  - Stage 5: feature transforms/sequence outputs
  - Stage 6: vectors assembled (no postprocess)
  - Stage 7: vectors + postprocess transforms
  - Use `--log-level DEBUG` for progress bars, `--log-level INFO` for spinner + prints, or the default (`WARNING`) for minimal output.
- `jerry serve --project <project.yaml> --output print|stream|path.pt|path.csv|path.jsonl.gz --limit N [--include-targets] [--log-level LEVEL] [--run name]`
  - Applies postprocess transforms and optional dataset split before emitting.
  - Set `--log-level DEBUG` (or set `run.yaml` -> `log_level: DEBUG`) to reuse the tqdm progress bars when previewing stages.
  - When `project.paths.run` is a directory, add `--run val` (filename stem) to target a single config; otherwise every run file is executed sequentially.
  - Argument precedence: CLI flags > run.yaml > built‑in defaults.

### Build & Quality

- `jerry inspect report --project <project.yaml> [--threshold 0.95] [--include-targets]`
  - Prints coverage summary (keep/below lists) and writes `coverage.json` under
    the artifacts directory.
  - Add `--matrix csv|html` to persist an availability matrix.
- `jerry inspect partitions --project <project.yaml> [--include-targets]`
  - Writes discovered partition suffixes to `partitions.json`.
- `jerry inspect expected --project <project.yaml> [--include-targets]`
  - Writes the full set of observed feature IDs to `expected.txt`.
- `jerry build --project <project.yaml> [--force]`
  - Regenerates artifacts declared in `build.yaml` if configuration hash changed.

### Scaffolding & Reference

- `jerry plugin init --name <package> --out <dir>`
  - Generates a plugin project (pyproject, package skeleton, config templates).
- `jerry source add --provider <name> --dataset <slug> --transport fs|url|synthetic --format csv|json|json-lines|pickle`
  - Creates loader/parser stubs, updates entry points, and drops a matching
    source YAML.
- `jerry domain add --domain <name>`
  - Adds a `domains/<name>/` package with a `model.py` stub.
- `jerry filter create --name <identifier>`
  - Scaffolds an entry-point-ready filter (helpful for custom record predicates).
- `jerry list sources|domains`
  - Introspect configured source aliases or domain packages.

---

## Transform & Filter Library

### Record Filters (`config/contracts[].record`)

- Binary comparisons: `eq`, `ne`, `lt`, `le`, `gt`, `ge` (timezone-aware for ISO
  or datetime literals).
- Membership: `in`, `nin`.
  ```yaml
  - filter: { operator: ge, field: time, comparand: "${start_time}" }
  - filter: { operator: in, field: station, comparand: [a, b, c] }
  ```

### Record Transforms

- `floor_time`: snap timestamps down to the nearest resolution (`10m`, `1h`, …).
- `lag`: add lagged copies of records (see `src/datapipeline/transforms/record/lag.py` for options).

### Stream (Feature) Transforms

- `ensure_ticks`: backfill missing ticks with `value=None` records to enforce a
  strict cadence.
- `granularity`: merge duplicate timestamps using `first|last|mean|median`.
- `fill`: rolling statistic-based imputation within each feature stream.
- Custom transforms can be registered under the `stream` entry-point group.

### Feature Transforms

- `scale`: wraps `StandardScalerTransform`. Read statistics from the build
  artifact or accept inline `statistics`.
  ```yaml
  scale:
    with_mean: true
    with_std: true
    statistics:
      temp_c__station=001: { mean: 10.3, std: 2.1 }
  ```

### Sequence Transforms

- `sequence`: sliding window generator (`size`, `stride`, optional `tick` to
  enforce gaps). Emits `FeatureRecordSequence` payloads with `.records`.

### Vector (Postprocess) Transforms

- `drop_missing`: drop vectors that do not meet required IDs or coverage ratio.
- `fill_constant`: seed absent IDs with a constant.
- `fill_history`: impute using rolling statistics from prior vectors.
- `fill_horizontal`: aggregate sibling partitions in the same timestamp.

All transforms share a consistent entry-point signature and accept their config
dict as keyword arguments. Register new ones in `pyproject.toml` under the
appropriate group (`record`, `stream`, `feature`, `sequence`, `vector`,
`filters`, `debug`).

---

## Artifacts & Postprocess

- `expected.txt`: newline-delimited full feature IDs. Required by drop/fill
  transforms to know the target feature universe.
- `scaler.pkl`: pickled standard scaler fitted on the configured split. Loaded
  lazily by feature transforms at runtime.
- Build state is tracked in `artifacts/build/state.json`; config hashes avoid
  redundant runs.

If a postprocess transform needs an artifact and it is missing, the runtime will
raise a descriptive error suggesting `jerry build`.

---

## Splitting & Serving

If `project.globals.split` is present, `jerry serve` filters vectors at the
end of the pipeline:

- `mode: hash` – deterministic entity hash using either the group key or a
  specified feature ID.
- `mode: time` – boundary-based slicing using timestamp labels.
- `run.keep` (or CLI `--keep`) selects the active slice; use any label name defined in your split config.

The split configuration never mutates stored artifacts; it is only applied when
serving vectors (either via CLI or the Python integrations).

---

## Python Integrations

`datapipeline.integrations.ml` demonstrates how to reuse the runtime from
application code:

- `VectorAdapter.from_project(project_yaml)` – bootstrap once, then stream
  vectors or row dicts.
- `stream_vectors(project_yaml, limit=...)` – iterator matching `jerry serve`.
- `iter_vector_rows` / `collect_vector_rows` – handy for Pandas or custom sinks.
- `dataframe_from_vectors` – eager helper that returns a Pandas DataFrame
  (requires `pandas`).
- `torch_dataset` – builds a `torch.utils.data.Dataset` that yields tensors. See
  `examples/minimal_project/run_torch.py` for usage.

---

## Extending the Runtime

### Entry Points

Register custom components in your plugin’s `pyproject.toml`:

```toml
[project.entry-points."datapipeline.loaders"]
demo.csv_loader = "my_datapipeline.loaders.csv:CsvLoader"

[project.entry-points."datapipeline.parsers"]
demo.weather_parser = "my_datapipeline.parsers.weather:WeatherParser"

[project.entry-points."datapipeline.mappers"]
weather.domain.mapper = "my_datapipeline.mappers.weather:DomainMapper"

[project.entry-points."datapipeline.stream"]
weather.fill = "my_datapipeline.transforms.weather:CustomFill"
```

Loader, parser, mapper, and transform classes should provide a callable
interface (usually `__call__`) matching the runtime expectations. Refer to the
built-in implementations in `src/datapipeline/sources/`, `src/datapipeline/transforms/`,
and `src/datapipeline/filters/`.

### Scaffolding Helpers

- `datapipeline.services.scaffold.plugin.scaffold_plugin` – invoked by
  `jerry plugin init`.
- `datapipeline.services.scaffold.source.create_source` – writes loader/parser
  stubs and updates entry points.
- `datapipeline.services.scaffold.domain.create_domain` – domain DTO skeleton.
- `datapipeline.services.scaffold.filter.create_filter` – custom filter stub.
- `datapipeline.services.scaffold.mappers.attach_source_to_domain` – helper for
  programmatically wiring sources to domain mappers and emitting stream
  contracts (useful in custom automation or tests).

---

## Development Workflow

- Install dependencies: `pip install -e .[dev]`.
- Run tests: `pytest`.
- When iterating on configs, use `jerry serve --stage <n>` to peek into problematic
  stages.
- After tuning transforms, refresh artifacts: `jerry build`.
- Use `jerry inspect report --include-targets` to ensure targets meet coverage
  gates before handing vectors to downstream consumers.

---

## Additional Resources

- `src/datapipeline/analysis/vector_analyzer.py` – quality metrics collected by
  the inspect commands.
- `src/datapipeline/pipeline/` – pure functions that wire each stage.
- `src/datapipeline/services/bootstrap/` – runtime initialization and
  registry population (see `core.py`).
- `examples/minimal_project/` – runnable demo showing config layout and Torch
  integration.

Happy shipping! Build, inspect, and serve consistent time-series features with
confidence.
