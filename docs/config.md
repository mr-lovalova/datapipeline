# Configuration

### Dataset Project (YAML Config)

These live under the dataset “project root” directory (the folder containing `project.yaml`):

- `project.yaml`: paths + globals (single source of truth).
- `sources/*.yaml`: raw sources (loader + parser wiring).
- `ingests/*.yaml`: raw source DTOs mapped, record-cleaned, and time-ordered into domain streams.
- `streams/*.yaml`: derived, joined, or manual streams built from existing stream ids.
- `dataset.yaml`: feature/target declarations.
- `postprocess.yaml`: vector-level transforms.
- `profiles/serve.<name>.yaml`: serve profiles.
- `profiles/build.<name>.yaml`: build profiles.
- `profiles/inspect.<name>.yaml`: inspect profiles.
- `tasks/operations/*.yaml`: declared artifact and runtime operations.

### Configuration & Resolution Order

Defaults are layered so you can set global preferences once, keep dataset/run
files focused on per-project behavior, and still override anything from the CLI.
For both `jerry serve` and `jerry build`, options are merged in the following
order (highest precedence first):

1. **CLI flags** – anything you pass on the command line always wins.
2. **Project profile files** – profile configs under `project.paths.profiles`
   (`cmd: serve|build|inspect`) supply per-command defaults and selection policy.
3. **Per-kind profile defaults** – optional `profiles/<kind>.defaults.yaml`.
4. **Built-in defaults** – runtime hard-coded defaults.

## YAML Config Reference

All dataset configuration is rooted at a single `project.yaml` file. Other YAML files are discovered via `project.paths.*` (relative to `project.yaml` unless absolute).

### `project.yaml`

```yaml
version: 1
name: default
paths:
  ingests: ./ingests
  streams: ./streams
  sources: ./sources
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  artifacts: ../artifacts/${project_name}/v${version}
  tasks: ./tasks
  profiles: ./profiles
split:
  mode: hash # hash | time
  key: group # group | feature:<id>
  seed: 42
  ratios: { train: 0.8, val: 0.1, test: 0.1 }
globals:
  start_time: 2021-01-01T00:00:00Z
  end_time: 2023-01-03T23:00:00Z
  vendor_api_key: ${env:VENDOR_API_KEY}
  raw_root: ${env:RAW_ROOT}
```

- `name` provides a stable identifier you can reuse inside config files via `${project_name}`.
- `paths.*` are resolved relative to the project file unless absolute; they also support `${var}` interpolation.
- `paths.sources`, `paths.ingests`, and `paths.streams` may be either one path
  or a list of paths. When a list is used, discovery scans every directory in
  order and rejects duplicate source or stream ids.
- `globals` provide values for `${var}` interpolation across YAML files. Datetime
  values are normalized to strict UTC `YYYY-MM-DDTHH:MM:SSZ`.
- External references use `${env:NAME}`. Resolution checks the process
  environment first and then an optional project-root `.env` file.
- New scaffolded dataset projects include a `.env.example` next to `project.yaml`.
- `split` config defines how labels are assigned; serve profiles or CLI flags pick the active label via `keep`.
  Legacy `globals.split` is still accepted with a deprecation warning.
- `paths.tasks` points to operation task specs under `tasks/operations/*.yaml`.
  Artifact operations (`vector_inputs`, `schema`, `scaler`, `metadata`, `stats`,
  and optional tick artifacts) define what can be materialized. Runtime operations
  (`pipeline`, `coverage`, `matrix`, `thresholds`, ...) define executable steps.
- `paths.profiles` points to profile specs grouped by type:
  `profiles/serve.<name>.yaml`, `profiles/build.<name>.yaml`, `profiles/inspect.<name>.yaml`.
  Optional defaults files may also be declared once per kind:
  `profiles/serve.defaults.yaml`, `profiles/build.defaults.yaml`, `profiles/inspect.defaults.yaml`.
  When multiple serve/build profiles exist, `jerry serve --run <name>` and
  `jerry build --run <name>` select by explicit profile `name`.
- Label names are free-form: match whatever keys you declare in `split.ratios` (hash) or `split.labels` (time).

### Serve Profiles (`profiles/serve.<name>.yaml`)

```yaml
cmd: serve
name: splits # required; unique among serve profiles
target: pipeline # serve operation name from tasks/operations
splits: [train, val, test] # optional; write one fs output per project split label
output:
  transport: fs # stdout | fs; splits require fs
  format: jsonl
  directory: runs
  # view: raw # optional; flat | raw (default: jsonl->raw, csv/pickle->flat)
  # encoding: utf-8 # fs jsonl/csv only
limit: 100 # cap vectors per serve run (null = unlimited)
throttle_ms: null # milliseconds to sleep between emitted vectors
# Optional overrides:
# observability:
#   visuals: ON      # ON | OFF
#   heartbeat_interval_seconds: 60 # 0 disables node heartbeat
#   logging:
#     level: INFO    # CRITICAL | ERROR | WARNING | INFO | DEBUG
#     outputs:
#       - transport: STDERR # STDERR | STDOUT | FS
#       - transport: FS
#         scope: EXECUTION  # GLOBAL | EXECUTION
#         path: logs/serve.log # optional; default logs/serve.<task>.log
```

- Each serve profile is a flat file under `profiles/` with the `serve.` prefix.
- `output`, `limit`, `throttle_ms`, and `observability` provide defaults for `jerry serve`; CLI flags still win per invocation (see _Configuration & Resolution Order_). For filesystem outputs, set `transport: fs`, `directory: /path/to/root`, and omit file names—each run automatically writes to `<directory>/<run_name>/<run_name>.<ext>` unless you override the entire `output` block with a custom `filename`.
- `output.encoding` is supported for fs `jsonl`/`csv` outputs (default `utf-8`); it is invalid for `stdout` and `pickle`.
- `splits` consumes the pipeline once and writes one output file per label, using
  filenames derived from the labels (for example `train.jsonl`, `val.jsonl`).
  Split fanout requires filesystem output and does not allow `output.filename`.
- `keep` remains supported for legacy single-output split filtering and can be
  overridden per invocation via `jerry serve ... --keep val`.
- Visuals backend: set `observability.visuals: ON|OFF` in the profile or use `--visuals on|off`.
- Node heartbeat: set `observability.heartbeat_interval_seconds` or use `--heartbeat-interval`; `0` disables heartbeat.
- Add additional `cmd: serve` files under `profiles/` using the `serve.` prefix
  for distinct serve policies; `jerry serve` runs each enabled profile unless
  you pass `--run <name>`.
- Use `profiles/serve.defaults.yaml` for common serve defaults shared across all serve profiles in a project. CLI flags and concrete profiles still take precedence.

### Build Profiles (`profiles/build.<name>.yaml`)

```yaml
cmd: build
name: schema # required; unique among build profiles
target: schema # required; artifact task kind to execute
mode: AUTO # AUTO | FORCE | OFF
# enabled: true # optional; profile-level switch
# Optional overrides:
# observability:
#   visuals: OFF
#   heartbeat_interval_seconds: 60 # 0 disables node heartbeat
#   logging:
#     level: INFO
#     outputs:
#       - transport: FS
#         scope: GLOBAL
#         path: ./logs/build.log
```

- `cmd: build` profiles are orchestration profiles; they do not replace artifact task definitions.
- `target` selects the artifact task kind for that profile (`schema`, `scaler`, `metadata`, ...).
- Build profile `observability.logging.outputs[].path` values are resolved relative to the dataset project root (`project.yaml` directory).
- `jerry build` runs enabled build profiles when they exist; `jerry build --run <name>` targets one profile.
- Precedence for build settings: CLI > `build.<name>.yaml` > `build.defaults.yaml` > built-ins.

### Profile Defaults (`profiles/<kind>.defaults.yaml`)

- Defaults files are optional and non-executable.
- They must include only `cmd` plus non-routing defaults for that kind.
- They must not include execution identity fields such as `name`, `target`, `enabled`, or `order`.
- Recommended precedence for runtime/build settings: CLI > concrete profile > `<kind>.defaults.yaml` > built-ins.

### Runtime Operations (`tasks/operations/*.yaml`)

```yaml
id: pipeline
kind: runtime
entrypoint: core.runtime.pipeline
```

- Runtime operations are executable units; profiles reference them via `target`.
- `entrypoint` must resolve in the `datapipeline.operations.runtime` entry-point
  group. Built-ins use `core.runtime.*`; plugins may register their own names.

### Workspace Routing (`jerry.yaml`)

Create an optional `jerry.yaml` in the directory where you run the CLI to share settings across commands. The CLI walks up from the current working directory to find the first `jerry.yaml`.

```yaml
plugin_root: lib/my-datapipeline # active plugin workspace (relative to this file)

# Dataset aliases for --dataset; values may be dirs (auto-append project.yaml).
datasets:
  your-dataset: lib/my-datapipeline/your-dataset/project.yaml
default_dataset: your-dataset
```

`jerry.yaml` sits near the root of your workspace and is only used for workspace routing (`plugin_root`, dataset aliases, default dataset).
- Command/runtime settings belong in profile files under `profiles/`.
- Execution-scoped logs (`scope: EXECUTION`) are configured on profiles or via
  CLI `--log-output execution[:<relative-path>]`.

### `<project_root>/sources/<alias>.yaml`

Each file defines a loader/parser pair exposed under `<alias>`. Files may live in nested
subdirectories under `<project_root>/sources/`; discovery is recursive.

```yaml
# Source identifier (commonly `provider.dataset`). Ingests reference this under `from.source`.
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
    headers:
      Authorization: "Bearer ${env:STOOQ_API_KEY}"
```

- `id`: the source alias; referenced by ingests under `from.source`.
- `parser.entrypoint`: which parser to use; `parser.args` are optional.
- `loader.entrypoint`: which loader to use; `core.io` is the default for fs/http and is configured via `loader.args`.
- Keep secrets and machine-local paths out of source files. Prefer `${env:...}`
  directly or route them through `project.yaml.globals` aliases like `${raw_root}`.

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

### `<project_root>/ingests/<stream_id>.yaml`

Ingest configs describe how the runtime should load raw DTOs, map them to
domain records, apply record-level cleanup, and normalize record order. Use
folders to organize by domain if you like.

```yaml
id: equity.ohlcv # stream identifier (domain.dataset[.variant])
from:
  source: stooq.ohlcv # references sources/<alias>.yaml:id

map:
  entrypoint: equity.ohlcv
  args: {}

partition_by: station
feature_id_by: []
sort_batch_size: 50000

record:
  - where: { field: time, operator: ge, comparand: "${start_time}" }
  - where: { field: time, operator: lt, comparand: "${end_time}" }
  - floor_time: { cadence: 10m }
```

### `<project_root>/streams/<stream_id>.yaml`

Stream configs consume existing stream ids and run partition-aware stream
transforms. They cannot reference raw sources and cannot define `record:`.

```yaml
id: equity.ohlcv.liquid
from:
  stream: equity.ohlcv
partition_by: station
feature_id_by: []
sort_batch_size: 50000
stream:
  - ensure_cadence: { field: close, to: close, cadence: 10m }
  - granularity: { field: close, to: close, mode: mean }
  - fill: { field: close, to: close, method: median, window: 6, min_samples: 2 }
  - fill: { field: close, to: close_asof, method: forward }

debug:
  - lint: { mode: warn, tick: 10m }
```

- `record`: per-record transforms applied before ordering (`where`, `floor_time`,
  and custom transforms registered under the `record` entry-point group).
  Ingest-only.
- `stream`: transforms applied after ingest ordering; operate on record fields before feature selection.
- `debug`: instrumentation-only transforms (linters, assertions).
- Each item in these lists must contain exactly one transform name whose value
  is a parameter mapping or `null`. Scalar/list parameters and multi-transform
  items are invalid. See [Transforms](transforms/index.md) for the entry-point
  callable contract.
- `partition_by`: optional stream state keys used by ordering and history-based
  transforms.
- `feature_id_by`: optional fields used to suffix feature IDs (e.g.,
  `temp__@station_id:XYZ`). If a partitioned stream is used as a dataset
  feature, set this explicitly: `[]` for scalar keyed-row features or a field
  list for wide feature IDs.
- `sort_batch_size`: batch size used by the stable sorter before it spills
  temporary runs while normalizing order.

### Manual Streams (Engineered Domains)

Define engineered streams that depend on other streams. The runtime builds each input as a prepared stream, exposes them as iterators keyed by alias, and calls your mapper to emit fresh records for the derived stream.

```yaml
# <project_root>/streams/air_density.processed.yaml
id: air_density.processed
from:
  streams:
    pressure: pressure.processed
    t: temp_dry.processed
partition_by: station_id
feature_id_by: []
sort_batch_size: 20000

map:
  # Mapper entrypoint required for joined/manual streams.
  entrypoint: mypkg.domains.air_density:map_to_air_density
  args:
    driver: pressure           # optional; defaults to first input alias

# Optional policies run after mapping like any stream.
# stream: [...]
# debug:  [...]
```

Dataset stays minimal — features only reference the derived stream:

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
- Manual mapper signature is `mapper(inputs, *, context, driver, **params)`.
- Use a joined stream when the framework should align records by timestamp.
- The derived stream outputs records; its own `stream`/`debug` rules still apply afterward.
- Partitioning for the engineered domain is explicit via `partition_by` on the derived stream.

### `dataset.yaml`

Defines which canonical streams become features/targets and the vector bucketing.

```yaml
sample:
  cadence: 1h
  keys: [security_id]

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

- `sample.cadence` controls the time bucket for vector samples (must match
  `^\\d+(m|min|h|d)$`, e.g. `10m`, `60min`, `1h`, `1d`).
- `sample.keys` optionally adds record fields to the sample key. For example,
  `keys: [security_id]` emits one sample per `(time, security_id)`.
- Stateful stream transforms such as `lag`, `lead`, `rolling`, `fill`, and
  `ensure_cadence` use stream `partition_by` as their entity partition. Add
  `partition_by: security_id` when transform state must stay per security.
- `group_by: 1h` is still accepted as the legacy time-only form and is
  equivalent to `sample: { cadence: 1h, keys: [] }`.
- `sample.keys`, stream `partition_by`, and stream `feature_id_by` are separate:
  `sample.keys` controls output row identity, `partition_by` controls stream
  transform state, and `feature_id_by` controls feature-id suffixes such as
  `close__@security_id:AAPL`.
- `field` selects the record attribute used as the feature/target value.
- `scale: true` inserts the standard scaler feature transform (requires scaler
  stats artifact or inline statistics).
  - Downstream consumers can load the `build/scaler.json` artifact and call
    `StandardScaler.inverse_transform` (or `StandardScalerTransform.inverse`)
    to undo scaling.
- `sequence` emits `FeatureRecordSequence` windows and accepts `size` plus
  optional `stride` (default `1`). Regularize cadence with stream transforms
  before feature extraction when contiguous ticks are required.
- Feature configuration exposes only `scale` and `sequence`; it does not accept
  arbitrary feature transform entry-point clauses.

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

- Each top-level item must contain exactly one transform name whose value is a
  parameter mapping or `null`.
- Each transform receives the sample stream; set `payload: targets` when you
  want the built-in transform to mutate label vectors, otherwise the feature
  vector is used.
- Vector transforms rely on the schema artifact (for expected IDs/cadence)
  and scaler stats when scaling is enabled. When no transforms are configured
  the stream passes through unchanged.

### Task Specs (`tasks/operations/*.yaml`)

Declare artifact and command tasks under `project.paths.tasks` (default `tasks/`).
Every artifact used by a selected profile must have a declared producer task.
The built-in task models provide default entry points and output paths, but they
do not create undeclared tasks.

`tasks/operations/scaler.yaml`

```yaml
id: scaler
kind: artifact
entrypoint: core.artifact.scaler
output: build/scaler.json
split_label: train
```

Folded temporal scaler:

```yaml
id: scaler
kind: artifact
entrypoint: core.artifact.scaler
output: build/scaler.json
folds:
  - fit: [train_0]
    apply: [train_0, val_0]
  - fit: [train_1]
    apply: [train_1, val_1]
```

- `build/scaler.json` stores either one standard scaler fitted on `split_label`
  or a folded temporal scaler container fitted from `folds`.
- Folded temporal scaling requires `project.split.mode: time`. `fit` and
  `apply` labels must exist in `project.split`; `apply` labels cannot overlap
  across folds.
- `build/schema.json` (from the `schema` task) enumerates the discovered feature/target identifiers (including partitions), their kinds (scalar/list), and cadence hints used to enforce ordering downstream.
  - Configure the `schema` task to choose a cadence strategy (currently `max`). Per-feature overrides will be added later; for now every list-valued feature records the max observed length as its enforcement target.
- `build/metadata.json` (from the `metadata` task) captures heavier statistics—present/null counts, inferred value types, list-length histograms, per-partition timestamps, and the dataset window. Configure `metadata.window_mode` with `union|intersection|strict|relaxed` (default `intersection`) to control how start/end bounds are derived. `union` considers base features, `intersection` uses their overlap, `strict` intersects every partition, and `relaxed` unions partitions independently.
- Artifact task execution order comes from the typed dependency graph. Selecting
  a target expands and builds its required producers in topological order.
- All operation tasks share the same execution interface: `entrypoint` selects the runner; profiles select operations via `target`.
- Serve profiles may target an artifact task or a runtime task. The built-in
  pipeline runner is `core.runtime.pipeline`; custom runtime entry points are
  also supported.
- Observability defaults (visuals/logging outputs) belong in profile files (`serve.<name>.yaml`, `build.<name>.yaml`, `inspect.<name>.yaml`) or per-kind defaults (`<kind>.defaults.yaml`).

---

### Versioning & Reproducibility

- Jerry outputs are deterministic given a fixed config, plugin code, and source snapshot.
- `jerry serve` runs are named by task/run and are reproducible when inputs + config are unchanged.
- A git tag on the workspace (plus the plugin repo) can represent a dataset “version” you can always rebuild.
- This pairs well with DVC: let DVC track raw inputs, and regenerate derived datasets from the tagged Jerry config when needed.
- Still use DVC for outputs when rebuilds are too expensive, transforms are non-deterministic, or sources are not snapshot-stable.

---
