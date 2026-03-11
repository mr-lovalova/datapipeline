# Configuration

### Dataset Project (YAML Config)

These live under the dataset “project root” directory (the folder containing `project.yaml`):

- `project.yaml`: paths + globals (single source of truth).
- `sources/*.yaml`: raw sources (loader + parser wiring).
- `contracts/*.yaml`: canonical streams (ingest or composed).
- `dataset.yaml`: feature/target declarations.
- `postprocess.yaml`: vector-level transforms.
- `profiles/serve.<name>.yaml`: serve profiles.
- `profiles/build.<name>.yaml`: build profiles.
- `profiles/inspect.<name>.yaml`: inspect profiles.
- `tasks/operations/*.yaml`: artifact operation configs.

### Configuration & Resolution Order

Defaults are layered so you can set global preferences once, keep dataset/run
files focused on per-project behavior, and still override anything from the CLI.
For both `jerry serve` and `jerry build`, options are merged in the following
order (highest precedence first):

1. **CLI flags** – anything you pass on the command line always wins.
2. **Project profile files** – profile configs under `project.paths.profiles`
   (`type: serve|build|inspect`) supply per-command defaults and selection policy.
3. **`jerry.yaml` command blocks** – settings under `jerry.serve` and `jerry.build`.
4. **`jerry.yaml.shared.observability`** – shared fallbacks for visuals/logging settings.
5. **Built-in defaults** – runtime hard-coded defaults.

## YAML Config Reference

All dataset configuration is rooted at a single `project.yaml` file. Other YAML files are discovered via `project.paths.*` (relative to `project.yaml` unless absolute).

### `project.yaml`

```yaml
version: 1
name: default
paths:
  streams: ./contracts
  sources: ./sources
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  artifacts: ../artifacts/${project_name}/v${version}
  tasks: ./tasks
  profiles: ./profiles
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
- `split` config defines how labels are assigned; serve profiles or CLI flags pick the active label via `keep`.
- `paths.tasks` points to operation task specs under `tasks/operations/*.yaml`.
  Build artifact operations (`schema`/`scaler`/`metadata`) define what can be materialized.
  Runtime operations (`serve`, `report`, `matrix`, ...) define executable runtime steps.
- `paths.profiles` points to profile specs grouped by type:
  `profiles/serve.<name>.yaml`, `profiles/build.<name>.yaml`, `profiles/inspect.<name>.yaml`.
  When multiple serve/build profiles exist, `jerry serve --run <name>` and
  `jerry build --run <name>` select by explicit profile `name`.
- Label names are free-form: match whatever keys you declare in `split.ratios` (hash) or `split.labels` (time).

### Serve Profiles (`profiles/serve.<name>.yaml`)

```yaml
type: serve
name: train # required; unique among serve profiles
target: serve # serve operation name from tasks/operations
keep: train # select active split label (null disables filtering)
output:
  transport: stdout # stdout | fs
  format: jsonl # stdout: jsonl
  # view: raw # optional; flat | raw | values (default: jsonl->raw, csv/pickle->flat)
  # encoding: utf-8 # fs jsonl/csv only
limit: 100 # cap vectors per serve run (null = unlimited)
throttle_ms: null # milliseconds to sleep between emitted vectors
# Optional overrides:
# observability:
#   visuals: ON      # ON | OFF
#   logging:
#     level: INFO    # CRITICAL | ERROR | WARNING | INFO | DEBUG
#     outputs:
#       - transport: STDERR # STDERR | STDOUT | FS
#       - transport: FS
#         scope: RUN        # GLOBAL | RUN
#         path: logs/serve.log # optional; default logs/serve.<task>.log
```

- Each serve profile is a flat file under `profiles/` with the `serve.` prefix.
- `output`, `limit`, `throttle_ms`, and `observability` provide defaults for `jerry serve`; CLI flags still win per invocation (see _Configuration & Resolution Order_). For filesystem outputs, set `transport: fs`, `directory: /path/to/root`, and omit file names—each run automatically writes to `<directory>/<run_name>/<run_name>.<ext>` unless you override the entire `output` block with a custom `filename`.
- `output.encoding` is supported for fs `jsonl`/`csv` outputs (default `utf-8`); it is invalid for `stdout` and `pickle`.
- Override `keep` (and other fields) per invocation via `jerry serve ... --keep val` etc.
- Visuals backend: set `observability.visuals: ON|OFF` in the task or use `--visuals on|off`.
- Add additional `type: serve` files under `profiles/` using the `serve.` prefix for other splits (val/test/etc.); `jerry serve` runs each enabled profile unless you pass `--run <name>`.
- Use `jerry.yaml` next to the project or workspace root to define shared defaults (`shared.observability.*` and output defaults); CLI flags still take precedence.

### Build Profiles (`profiles/build.<name>.yaml`)

```yaml
type: build
name: schema # required; unique among build profiles
target: schema # required; artifact task kind to execute
mode: AUTO # AUTO | FORCE | OFF
# enabled: true # optional; profile-level switch
# Optional overrides:
# observability:
#   visuals: OFF
#   logging:
#     level: INFO
#     outputs:
#       - transport: FS
#         scope: GLOBAL
#         path: ./logs/build.log
```

- `type: build` profiles are orchestration profiles; they do not replace artifact task definitions.
- `target` selects the artifact task kind for that profile (`schema`, `scaler`, `metadata`, ...).
- Build profile `observability.logging.outputs[].path` values are resolved relative to the dataset project root (`project.yaml` directory).
- `jerry build` runs enabled build profiles when they exist; `jerry build --run <name>` targets one profile.
- Precedence for build settings: CLI > build profile > `jerry.yaml build` > `jerry.yaml shared` > defaults.

### Runtime Operations (`tasks/operations/*.yaml`)

```yaml
id: serve
entrypoint: core.serve_pipeline
dependencies: [] # optional artifact task ids
```

- Runtime operations are executable units; profiles reference them via `target`.
- `entrypoint` must resolve to a registered operation runner (`core.serve.*`, `core.inspect.*`, ...).

### Workspace Defaults (`jerry.yaml`)

Create an optional `jerry.yaml` in the directory where you run the CLI to share settings across commands. The CLI walks up from the current working directory to find the first `jerry.yaml`.

```yaml
plugin_root: lib/my-datapipeline # active plugin workspace (relative to this file)

# Dataset aliases for --dataset; values may be dirs (auto-append project.yaml).
datasets:
  your-dataset: lib/my-datapipeline/your-dataset/project.yaml
default_dataset: your-dataset

shared:
  observability:
    visuals: ON # ON | OFF
    logging:
      level: INFO
      outputs:
        - transport: STDERR
        - transport: FS
          scope: GLOBAL
          path: ./logs/jerry.log

serve:
  observability:
    logging:
      outputs:
        - transport: FS
          scope: RUN # logs under each run directory
  limit: null
  stage: null
  output:
    transport: stdout
    format: jsonl # stdout: jsonl
    # view: raw # optional; flat | raw | values (default: jsonl->raw, csv/pickle->flat)
    # encoding: utf-8 # fs jsonl/csv only
    # directory: artifacts/serve # Required when transport=fs

build:
  observability:
    visuals: OFF
    logging:
      level: INFO
      outputs:
        - transport: FS
          scope: GLOBAL
          path: ./logs/build.log
  mode: AUTO # AUTO | FORCE | OFF
```

`jerry.yaml` sits near the root of your workspace, while dataset-specific overrides live in `profiles/{serve,build,inspect}.<name>.yaml` and `tasks/operations/*.yaml`.

### `<project_root>/sources/<alias>.yaml`

Each file defines a loader/parser pair exposed under `<alias>`. Files may live in nested
subdirectories under `<project_root>/sources/`; discovery is recursive.

```yaml
# Source identifier (commonly `provider.dataset`). Contracts reference this under `source:`.
id: stooq.ohlcv
parser:
  # Parser entry point name (registered in your plugin’s pyproject.toml).
  entrypoint: stooq.ohlcv
loader:
  # Most common loader: core.io (supports fs/http via args.transport + args.format).
  entrypoint: core.io
  args:
    transport: http
    format: csv
    url: "https://stooq.com/q/d/l/?s=aapl.us&i=d"
```

- `id`: the source alias; referenced by contracts under `source:`.
- `parser.entrypoint`: which parser to use; `parser.args` are optional.
- `loader.entrypoint`: which loader to use; `core.io` is the default for fs/http and is configured via `loader.args`.

#### Fan-out Sources (`core.foreach`)

Use `core.foreach` to expand any inner loader spec across a list without duplicating YAML. It interpolates string args and optionally injects the foreach value into each row.

```yaml
loader:
  entrypoint: core.foreach
  args:
    foreach:
      symbol: [AAPL, MSFT]
    inject_field: symbol
    loader:
      entrypoint: core.io
      args:
        transport: http
        format: csv
        url: "https://stooq.com/q/d/l/?s=${symbol}&i=d"
```

### `<project_root>/contracts/<stream_id>.yaml`

Canonical stream contracts describe how the runtime should map and prepare a raw
source. Use folders to organize by domain if you like.

```yaml
kind: ingest
id: equity.ohlcv # stream identifier (domain.dataset[.variant])
source: stooq.ohlcv # references sources/<alias>.yaml:id

mapper:
  entrypoint: equity.ohlcv
  args: {}

partition_by: station
sort_batch_size: 50000

record:
  - filter: { field: time, operator: ge, comparand: "${start_time}" }
  - filter: { field: time, operator: lt, comparand: "${end_time}" }
  - floor_time: { cadence: 10m }

stream:
  - ensure_cadence: { field: close, to: close, cadence: 10m }
  - granularity: { field: close, to: close, mode: mean }
  - fill: { field: close, to: close, statistic: median, window: 6, min_samples: 2 }

debug:
  - lint: { mode: warn, tick: 10m }
```

- `record`: ordered record-level transforms (filters, floor/lag, custom
  transforms registered under the `record` entry-point group).
- `stream`: transforms applied after record transforms; operate on record fields before feature selection.
- `debug`: instrumentation-only transforms (linters, assertions).
- `partition_by`: optional keys used to suffix feature IDs (e.g., `temp__@station_id:XYZ`).
- `sort_batch_size`: chunk size used by the in-memory sorter when normalizing
  order before stream transforms.

### Composed Streams (Engineered Domains)

Define engineered streams that depend on other canonical streams directly in contracts. The runtime builds each input to stage 4 (ordered + stream transforms applied), stream‑aligns by partition + timestamp, runs your composer, and emits fresh records for the derived stream.

```yaml
# <project_root>/contracts/air_density.processed.yaml
kind: composed
id: air_density.processed
inputs:
  - pressure.processed
  - t=temp_dry.processed
partition_by: station_id
sort_batch_size: 20000

mapper:
  # Function or class via dotted path; entry points optional
  entrypoint: mypkg.domains.air_density:compose_to_record
  args:
    driver: pressure.processed # optional; defaults to first input

# Optional post‑compose policies (run after composition like any stream)
# record: [...]
# stream: [...]
# debug:  [...]
```

Dataset stays minimal — features only reference the composed stream:

```yaml
# dataset.yaml
group_by: 1h
features:
  - id: air_density
    record_stream: air_density.processed
    field: air_density
```

Notes:

- Inputs always reference canonical stream_ids (not raw sources).
- The composed source outputs records; its own `record`/`stream`/`debug` rules still apply afterward.
- Partitioning for the engineered domain is explicit via `partition_by` on the composed contract.

### `dataset.yaml`

Defines which canonical streams become features/targets and the vector bucketing.

```yaml
group_by: 1h

features:
  - id: close
    record_stream: equity.ohlcv
    field: close
    scale: true
    sequence: { size: 6, stride: 1 }

targets:
  - id: returns_1d
    record_stream: equity.ohlcv
    field: returns_1d
```

- `group_by` controls the cadence for vector partitioning (must match `^\\d+(m|min|h|d)$`,
  e.g. `10m`, `60min`, `1h`, `1d`).
- `field` selects the record attribute used as the feature/target value.
- `scale: true` inserts the standard scaler feature transform (requires scaler
  stats artifact or inline statistics).
  - Downstream consumers can load the `build/scaler.json` artifact and call
    `StandardScaler.inverse_transform` (or `StandardScalerTransform.inverse`)
    to undo scaling.
- `sequence` emits `FeatureRecordSequence` windows (size, stride, optional
  cadence enforcement via `tick`).

### `postprocess.yaml`

Project-scoped vector transforms that run after assembly and before serving.

```yaml
- drop:
    axis: horizontal
    payload: features
    threshold: 0.95
- fill:
    statistic: median
    window: 48
    min_samples: 6
- replace:
    payload: targets
    value: 0.0
```

- Each transform receives a `Sample`; set `payload: targets` when you want to
  mutate label vectors, otherwise the feature vector is used.
- Vector transforms rely on the schema artifact (for expected IDs/cadence)
  and scaler stats when scaling is enabled. When no transforms are configured
  the stream passes through unchanged.

### Task Specs (`tasks/operations/*.yaml`)

Declare artifact and command tasks under `project.paths.tasks` (default `tasks/`).
Artifact specs are optional; if you omit them, Jerry falls back to built-in defaults.
Add a YAML file only when you need to override paths or other parameters.

`tasks/operations/scaler.yaml`

```yaml
id: scaler
entrypoint: core.build.scaler
output: build/scaler.json
split_label: train
```

- `build/scaler.json` stores standard scaler statistics fitted on the requested split.
- `build/schema.json` (from the `schema` task) enumerates the discovered feature/target identifiers (including partitions), their kinds (scalar/list), and cadence hints used to enforce ordering downstream.
  - Configure the `schema` task to choose a cadence strategy (currently `max`). Per-feature overrides will be added later; for now every list-valued feature records the max observed length as its enforcement target.
- `build/metadata.json` (from the `metadata` task) captures heavier statistics—present/null counts, inferred value types, list-length histograms, per-partition timestamps, and the dataset window. Configure `metadata.window_mode` with `union|intersection|strict|relaxed` (default `intersection`) to control how start/end bounds are derived. `union` considers base features, `intersection` uses their overlap, `strict` intersects every partition, and `relaxed` unions partitions independently.
- Artifact operation dependencies are declared explicitly on task specs via `dependencies` (for example, `metadata` defaults to `dependencies: [schema]`).
- All operation tasks share the same execution interface: `entrypoint` selects the runner; profiles select operations via `target`.
- Serve profiles (`type: serve`) must target a runtime operation whose entrypoint is `core.serve.*` or `core.serve_*` (usually `id: serve`).
- Shared run/build defaults (visuals/logging level/build mode) live in `jerry.yaml`.

---

### Versioning & Reproducibility

- Jerry outputs are deterministic given a fixed config, plugin code, and source snapshot.
- `jerry serve` runs are named by task/run and are reproducible when inputs + config are unchanged.
- A git tag on the workspace (plus the plugin repo) can represent a dataset “version” you can always rebuild.
- This pairs well with DVC: let DVC track raw inputs, and regenerate derived datasets from the tagged Jerry config when needed.
- Still use DVC for outputs when rebuilds are too expensive, transforms are non-deterministic, or sources are not snapshot-stable.

---
