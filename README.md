# Datapipeline Runtime

Jerry Thomas is a time-series-first data pipeline runtime. It turns declarative
YAML projects into iterators that stream records, engineered features, and
model-ready vectors. The CLI lets you preview every stage, build deterministic
artifacts, inspect quality, and scaffold plugins for custom loaders, parsers,
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
jerry plugin init my_datapipeline --out .

# 3. Create a source & domain scaffold, then declare a canonical stream.
# Simple forms
jerry source add demo weather --transport fs --format csv
jerry source add demo.weather --transport http --format json

# Flag form (explicit)
jerry source add --provider demo --dataset weather --transport fs --format csv
jerry domain add weather
# (edit config/contracts/<alias>.yaml to point at your mapper and policies)

# 4. Configure dataset/postprocess/build files under config/.
#    Then preview the pipeline and serve a few vectors:
#    Add --skip-build when you only need a quick feature peek.
jerry serve --project config/project.yaml --stage 2 --limit 5
jerry serve --project config/project.yaml --limit 3

# 5. Inspect coverage and build artifacts:
jerry inspect report --project config/project.yaml
jerry build --project config/project.yaml
```

The skeleton project in `src/datapipeline/templates/plugin_skeleton/` mirrors the
paths expected by the CLI. Copy it or run `jerry plugin init` to get a ready-made
layout with `config/`, `src/<package>/`, and entry-point stubs.

---

## Pipeline Architecture

```text
raw source ──▶ loader/parser DTOs ──▶ canonical stream ──▶ record policies
      └──▶ feature wrapping ──▶ stream regularization ──▶ feature transforms/sequence
      └──▶ vector assembly ──▶ postprocess transforms
```

1. **Loader/parser (Stage 0)** – raw bytes become typed DTOs. Loaders fetch from
   FS/HTTP/synthetic sources; parsers map bytes to DTOs. Register them via entry
   points (`loaders`, `parsers`) and wire them in `config/sources/*.yaml`.
2. **Canonical stream mapping (Stage 1)** – mappers attach domain semantics and
   partition keys, producing domain `TemporalRecord`s.
3. **Record policies (Stage 2)** – contract `record` rules (filters, floor, lag)
   prune and normalize DTO-derived records.
4. **Feature wrapping (Stage 3)** – records become `FeatureRecord`s before
   sort/regularization.
5. **Stream regularization (Stage 4)** – contract `stream` rules ensure cadence,
   deduplicate timestamps, and impute where needed.
6. **Feature transforms/sequence (Stage 5)** – dataset transforms (scale,
   sequence windows) produce per-feature tensors or windows.
7. **Vector assembly (Stage 6)** – features merge by `group_by` cadence into
   `(group_key, Vector)` pairs, prior to postprocess tweaks.
8. **Postprocess (Stage 7)** – optional vector transforms (fill/drop/etc.) run
   before results are emitted to the configured output.

#### Visual Flowchart

```mermaid
flowchart TB
  subgraph CLI & Project config
    cliSource[jerry source add]
    cliDomain[jerry domain add]
    cliContract[jerry contract]
    cliServe[jerry serve]
    project[[project.yaml]]
    sourcesCfg[config/sources/*.yaml]
    contractsCfg[config/contracts/*.yaml]
    datasetCfg[dataset.yaml]
    postprocessCfg[postprocess.yaml]
  end

  cliSource --> sourcesCfg
  cliDomain --> domainPkg
  cliContract --> contractsCfg
  cliServe --> vectorSamples
  project -.->|paths.sources| sourcesCfg
  project -.->|paths.streams| contractsCfg
  project -.->|paths.dataset| datasetCfg
  project -.->|paths.postprocess| postprocessCfg

  subgraph Plugin code
    domainPkg[domains/*]
    mappersPkg[mappers/*]
  end

  cliContract --> mappersPkg
  domainPkg -. domain models .-> mappersPkg

  subgraph Registries
    registrySources[sources]
    registryStreamSources[stream_sources]
    registryMappers[mappers]
    registryRecordOps[record_ops]
    registryStreamOps[stream_ops]
    registryDebugOps[debug_ops]
  end

  subgraph Source wiring
    rawData[(external data)]
    transportSpec[transport + format]
    loaderEP[loader ep]
    parserEP[parser ep]
    sourceArgs[loader args]
    sourceNode[Source]
    dtoStream[(DTOs)]
  end

  sourcesCfg --> transportSpec
  sourcesCfg --> loaderEP
  sourcesCfg --> parserEP
  sourcesCfg --> sourceArgs
  transportSpec -. select fs/http/synth .-> loaderEP
  loaderEP -. build loader .-> sourceNode
  parserEP -. build parser .-> sourceNode
  sourceArgs -. paths/creds .-> sourceNode
  rawData --> sourceNode --> dtoStream
  sourcesCfg -. build_source_from_spec .-> registrySources
  contractsCfg -. stream_id + source .-> registryStreamSources
  registrySources -. alias -> Source .-> registryStreamSources

  subgraph Canonical stream
    mapperEP[mapper ep]
    recordRules[record rules]
    streamRules[stream rules]
    debugRules[debug rules]
    canonical[DTO -> record]
    domainRecords((TemporalRecord))
    recordStage[record xforms]
    featureWrap[record -> feature]
    featureRecords((FeatureRecord))
    regularization[stream xforms]
  end

  dtoStream --> canonical --> domainRecords --> recordStage --> featureWrap --> featureRecords --> regularization
  contractsCfg --> mapperEP
  mappersPkg -. ep target .-> mapperEP
  mapperEP -. build_mapper_from_spec .-> registryMappers
  registryMappers --> canonical
  contractsCfg --> recordRules
  contractsCfg --> streamRules
  contractsCfg --> debugRules
  registryRecordOps --> recordRules
  registryStreamOps --> streamRules
  registryDebugOps --> debugRules
  recordRules --> recordStage
  streamRules --> regularization
  debugRules --> regularization

  subgraph Dataset shaping
    featureSpec[feature cfg]
    groupBySpec[group_by]
    streamRefs[record_stream ids]
    featureTrans[feature/seq xforms]
    sequenceStream((seq/features))
    vectorStage[vector assembly]
    vectorSamples((samples))
  end

  datasetCfg --> featureSpec
  datasetCfg --> groupBySpec
  datasetCfg --> streamRefs
  streamRefs -.->|build_feature_pipeline| registryStreamSources
  registryStreamSources -.->|open_source_stream| sourceNode
  featureRecords --> regularization --> featureTrans --> sequenceStream --> vectorStage --> vectorSamples
  featureSpec -. scale/sequence .-> featureTrans
  groupBySpec -. cadence .-> vectorStage

  subgraph Postprocess
    vectorTransforms[vector xforms]
    postprocessNode[postprocess]
  end

  postprocessCfg --> vectorTransforms -. drop/fill .-> postprocessNode
  vectorStage --> postprocessNode
```

style cliSource width:120px
style cliDomain width:120px
style cliContract width:120px
style cliServe width:120px
style sourcesCfg width:200px
style contractsCfg width:200px
style datasetCfg width:180px
style postprocessCfg width:200px
style registrySources width:160px
style registryStreamSources width:180px
style registryMappers width:160px
style registryRecordOps width:180px
style registryStreamOps width:180px
style registryDebugOps width:180px
style transportSpec width:180px
style loaderEP width:140px
style parserEP width:140px
style sourceArgs width:160px
style canonical width:180px
style featureTrans width:180px
style domainRecords width:140px
style featureRecords width:140px
style sequenceStream width:180px
style vectorStage width:160px
style vectorSamples width:180px
style recordRules width:160px
style streamRules width:160px
style debugRules width:160px
style domainPkg width:160px
style mappersPkg width:160px

Solid arrows trace runtime data flow; dashed edges highlight how the config files
inject transports, entry points, or policies into each stage.

CLI quick path:
- `jerry source add <provider> <dataset> --transport fs|http|synthetic --format ...` → scaffolds DTO/parser/loader and writes `config/sources/*.yaml`.
- `jerry domain add <name>` → creates `src/<pkg>/domains/<name>/model.py`.
- `jerry contract` → picks a source + domain, scaffolds/links a mapper under `mappers/`, registers its entry point, and writes `config/contracts/<stream>.yaml`.
- `jerry serve --project <project.yaml>` → builds/streams vectors using dataset `record_stream` IDs, registry wiring, and postprocess rules.

`config/sources/*.yaml` determines both the transport and parsing strategy:
you define transport (`fs`, `http`, `synthetic`, etc.), the payload format
(`csv`, `json`, ...), and the loader/parser entry points. Loader `args`
typically include file paths, bucket prefixes, or credential references—the
runtime feeds those arguments into the instantiated loader so it knows exactly
which external data store to read. Contracts bind each canonical stream to a
`source` alias (connecting back to the loader/parser pair) and register a
stream ID; they also specify mapper entry points, record/stream rules,
partitioning, and batch sizes. Dataset features reference those canonical
stream IDs via `record_stream`, so each feature config reuses the registered
stream (and, by extension, the raw source) when you call
`build_feature_pipeline()` (`src/datapipeline/pipeline/pipelines.py`). Finally,
`postprocess.yaml` decorates the vector stream with additional filters/fills so
serve/build outputs inherit the full set of policies. When you run the CLI,
`bootstrap()` (`src/datapipeline/services/bootstrap/core.py`) loads each
directory declared in `project.yaml`, instantiates loaders/parsers via
`build_source_from_spec()` and `load_ep()`, attaches contract registries, and
hands a fully wired `Runtime` to the pipeline stages in
`src/datapipeline/pipeline/stages.py`.

Every `record_stream` identifier ultimately resolves to the stream entry revived
by the contract bootstrap step, so requesting stage outputs for a feature always
walks the entire chain from dataset config → canonical contract → source
definition. That is why `build_feature_pipeline()` starts by calling
`open_source_stream(context, record_stream_id)` before stepping through record
policies, stream policies, and feature transforms.

The runtime (`src/datapipeline/runtime.py`) hosts registries for sources,
transforms, artifacts, and postprocess rules. The CLI constructs lightweight
`PipelineContext` objects to build iterators without mutating global state.

---

## Configuration Files

All project configuration for a dataset lives under a single project root directory (for example `config/`), which contains `project.yaml` and its siblings.

### `project.yaml`

```yaml
version: 1
name: default
paths:
  streams: ./contracts
  sources: ./sources
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  artifacts: ../build/datasets/${project_name}
  tasks: ./tasks
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
- `split` config defines how labels are assigned; serve tasks or CLI flags pick the active label via `keep`.
- `paths.tasks` points to a directory of task specs. Each `*.yaml` file declares `kind: ...`
  (`scaler`, `schema`, `metadata`, `serve`, …). Artifact tasks drive `jerry build`; command
  tasks (currently `kind: serve`) provide presets for `jerry serve`. When multiple serve tasks
  exist, `jerry serve --run <name>` selects by `name`/filename stem.
- Label names are free-form: match whatever keys you declare in `split.ratios` (hash) or `split.labels` (time).

### Serve Tasks (`tasks/serve.<name>.yaml`)

```yaml
kind: serve
name: train        # defaults to filename stem when omitted
keep: train        # select active split label (null disables filtering)
output:
  transport: stdout  # stdout | fs
  format: print      # print | json-lines | json | csv | pickle
limit: 100           # cap vectors per serve run (null = unlimited)
throttle_ms: null    # milliseconds to sleep between emitted vectors
# Optional overrides:
# log_level: INFO   # DEBUG=progress bars, INFO=spinner, WARNING=quiet
# visuals: AUTO     # AUTO | TQDM | RICH | OFF
# progress: AUTO    # AUTO | SPINNER | BARS | OFF
```

- Each serve task lives alongside artifact tasks under `paths.tasks`. Files are independent—no special directory structure required.
- `output`, `limit`, `throttle_ms`, and `log_level` provide defaults for `jerry serve`; CLI flags still win per invocation (see *Configuration Resolution Order*). For filesystem outputs, set `transport: fs`, `directory: /path/to/root`, and omit file names—each run automatically writes to `<directory>/<run_name>/<run_name>.<ext>` unless you override the entire `output` block with a custom `filename`.
- Override `keep` (and other fields) per invocation via `jerry serve ... --keep val` etc.
- Visuals backend: set `visuals: AUTO|TQDM|RICH|OFF` in the task or use `--visuals`. Pair with `progress: AUTO|SPINNER|BARS|OFF` or `--progress` to control progress layouts.
- Add additional `kind: serve` files to the tasks directory for other splits (val/test/etc.); `jerry serve` runs each enabled file unless you pass `--run <name>`.
- Use `jerry.yaml` next to the project or workspace root to define shared defaults (visuals/progress/log level/output); CLI flags still take precedence.

### Workspace Defaults (`jerry.yaml`)

Create an optional `jerry.yaml` in the directory where you run the CLI to share settings across commands. The CLI walks up from the current working directory to find the first `jerry.yaml`.

```yaml
plugin_root: lib/power_plugin # optional repo path for scaffolding (relative to this file)
config_root: configs/default # directory containing project.yaml (relative paths ok)

shared:
  visuals: rich # default visual renderer (auto|tqdm|rich|off)
  progress: bars # spinner|bars|auto|off

serve:
  log_level: INFO
  output:
    transport: stdout
    format: print
    # directory: artifacts/serve # Required when transport=fs

build:
  log_level: INFO
  mode: AUTO # AUTO | FORCE | OFF
```

`jerry.yaml` sits near the root of your workspace, while dataset-specific overrides still live in individual `tasks/serve.*.yaml` files as needed.

### Configuration Resolution Order

Defaults are layered so you can set global preferences once, keep dataset/run
files focused on per-project behavior, and still override anything from the CLI.
For both `jerry serve` and `jerry build`, options are merged in the following
order (highest precedence first):

1. **CLI flags** – anything you pass on the command line always wins, even if a
   value is already specified elsewhere.
2. **Project task files** – `kind: serve` specs (under `project.paths.tasks`)
   supply serve defaults; artifact tasks in the same directory drive `jerry build`.
   These only apply to the dataset that owns the config directory.
3. **`jerry.yaml` command blocks** – settings under `jerry.serve` and
   `jerry.build` provide workspace-wide defaults for their respective commands.
4. **`jerry.yaml.shared`** – shared fallbacks for visuals/progress/log-level
   style settings that apply to every command when a more specific value is not
   defined.
5. **Built-in defaults** – the runtime’s hard-coded values used when nothing else
   sets an option.

This hierarchy lets you push opinionated defaults up to the workspace (so every
project or dataset behaves consistently) while still giving each dataset and
every CLI invocation the ability to tighten or override behaviors.

### `config/sources/<alias>.yaml`

Each file defines a loader/parser pair exposed under `<alias>` (also the
`id` the rest of the pipeline references). Files may live in nested
subdirectories under `config/sources/`; discovery is recursive.

```yaml
id: demo_weather
parser:
  entrypoint: demo.weather_parser
  args:
    timezone: UTC
loader:
  entrypoint: demo.csv_loader
  args:
    path: data/weather.csv
```

### `config/contracts/<alias>.yaml`

Canonical stream contracts describe how the runtime should map and prepare a
source. Use folders to organize by domain.

```yaml
kind: ingest
id: demo_weather
source: demo_weather

mapper:
  entrypoint: weather.domain.mapper
  args: {}

partition_by: station
sort_batch_size: 50000

record:
  - filter: { operator: ge, field: time, comparand: "${start_time}" }
  - filter: { operator: lt, field: time, comparand: "${end_time}" }
  - floor_time: { cadence: 10m }

stream:
  - ensure_cadence: { cadence: 10m }
  - granularity: { mode: mean }
  - fill: { statistic: median, window: 6, min_samples: 2 }

debug:
  - lint: { mode: warn, tick: 10m }
```

- `record`: ordered record-level transforms (filters, floor/lag, custom
  transforms registered under the `record` entry-point group).
- `stream`: transforms applied after feature wrapping, still per base feature.
- `debug`: instrumentation-only transforms (linters, assertions).
- `partition_by`: optional keys used to suffix feature IDs (e.g., `temp__@station_id:XYZ`).
- `sort_batch_size`: chunk size used by the in-memory sorter when normalizing
  order before stream transforms.

### Composed Streams (Engineered Domains)

Define engineered streams that depend on other canonical streams directly in contracts. The runtime builds each input to stage 4 (ordered + regularized), stream‑aligns by partition + timestamp, runs your composer, and emits fresh records for the derived stream.

```yaml
# contracts/air_density.processed.yaml
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
  - id: temp_c
    record_stream: demo_weather
    scale: true
    sequence: { size: 6, stride: 1 }

targets:
  - id: precip
    record_stream: demo_weather
```

- `group_by` controls the cadence for vector partitioning (accepts `Xm|min|Xh`
  — minutes or hours).
- `scale: true` inserts the standard scaler feature transform (requires scaler
  stats artifact or inline statistics).
  - Downstream consumers can load the `scaler.pkl` artifact and call
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

### Task Specs (`tasks/*.yaml`)

Declare artifact and command tasks under `project.paths.tasks` (default `tasks/`).
Artifact specs are optional; if you omit them, Jerry falls back to built-in defaults.
Add a YAML file only when you need to override paths or other parameters.

`tasks/scaler.yaml`

```yaml
kind: scaler
output: scaler.pkl
split_label: train
enabled: true
```

- `scaler.pkl` is a pickled standard scaler fitted on the requested split.
- `schema.json` (from the `schema` task) enumerates the discovered feature/target identifiers (including partitions), their kinds (scalar/list), and cadence hints used to enforce ordering downstream.
  - Configure the `schema` task to choose a cadence strategy (currently `max`). Per-feature overrides will be added later; for now every list-valued feature records the max observed length as its enforcement target.
- `metadata.json` (from the `metadata` task) captures heavier statistics—present/null counts, inferred value types, list-length histograms, per-partition timestamps, and the dataset window. Configure `metadata.window_mode` with `union|intersection|strict|relaxed` (default `intersection`) to control how start/end bounds are derived. `union` considers base features, `intersection` uses their overlap, `strict` intersects every partition, and `relaxed` unions partitions independently.
- Command tasks (`kind: serve`) live alongside artifact tasks; `jerry serve` reads them directly.
- Shared run/build defaults (visuals/progress/log level/build mode) live in `jerry.yaml`.

---

## CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.

### Preview Stages

- `jerry serve --project <project.yaml> --stage <0-7> --limit N [--log-level LEVEL] [--visuals auto|tqdm|rich|off] [--progress auto|spinner|bars|off]`
  - Stage 0: raw DTOs
  - Stage 1: domain `TemporalRecord`s
  - Stage 2: record transforms applied
  - Stage 3: feature records (before sort/regularization)
  - Stage 4: feature regularization (post stream transforms)
  - Stage 5: feature transforms/sequence outputs
  - Stage 6: vectors assembled (no postprocess)
  - Stage 7: vectors + postprocess transforms
  - Use `--log-level DEBUG` for progress bars, `--log-level INFO` for spinner + prints, or the default (`WARNING`) for minimal output.
  - Ensures build artifacts are current before streaming; the build step only runs when the configuration hash changes unless you pass `--stage` 0-5 (auto-skip) or opt out with `--skip-build`.
- `jerry serve --project <project.yaml> --out-transport stdout --out-format json-lines --limit N [--include-targets] [--log-level LEVEL] [--visuals ...] [--progress ...] [--run name]`
  - Applies postprocess transforms and optional dataset split before emitting.
  - Use `--out-transport fs --out-format json-lines --out-path build/serve` (or `csv`, `pickle`, etc.) to write artifacts to disk instead of stdout; files land under `<out-path>/<run_name>/`.
- `--out-payload vector` emits only the vector payload with features/targets
  flattened into schema-ordered lists (no identifier keys) when you don't need
  the group key or metadata. Default is `sample`.
  - Set `--log-level DEBUG` (or set your serve task `log_level: DEBUG`) to reuse the tqdm progress bars when previewing stages.
  - When multiple serve tasks exist, add `--run val` (task name or filename stem) to target a single config; otherwise every enabled task is executed sequentially.
  - Argument precedence follows the order described under *Configuration Resolution Order*.
  - Combine with `--skip-build` when you already have fresh artifacts and want to jump straight into streaming.

### Build & Quality

- `jerry inspect report --project <project.yaml> [--threshold 0.95] [--include-targets]`
  - Prints coverage summary (keep/below lists) and writes `coverage.json` under
    the artifacts directory.
  - Add `--matrix csv|html` to persist an availability matrix.
- `jerry inspect partitions --project <project.yaml> [--include-targets]`
  - Writes discovered partition suffixes to `partitions.json`.
- `jerry inspect expected --project <project.yaml> [--include-targets]`
  - Writes the full set of observed feature IDs to `expected.txt` (for external tooling; runtime uses `schema.json`).
- `jerry build --project <project.yaml> [--force] [--visuals ...] [--progress ...]`
  - Regenerates artifact tasks declared under `project.paths.tasks` when the configuration hash changes.

### Scaffolding & Reference

- `jerry plugin init <package> --out <dir>` (also supports `-n/--name`)
  - Generates a plugin project (pyproject, package skeleton, config templates).
- `jerry source add <provider> <dataset> --transport fs|http|synthetic --format csv|json|json-lines|pickle`
  - Also supports `<provider>.<dataset>` via `--alias` or as the first positional
  - Flag form remains available: `--provider/--dataset`
  - Creates loader/parser stubs, updates entry points, and drops a matching
    source YAML.
- `jerry domain add <name>` (also supports `-n/--name`)
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

- `floor_time`: snap timestamps down to the nearest cadence (`10m`, `1h`, …).
- `lag`: add lagged copies of records (see `src/datapipeline/transforms/record/lag.py` for options).

### Stream (Feature) Transforms

- `ensure_cadence`: backfill missing ticks with `value=None` records to enforce a
  strict cadence.
- `granularity`: merge duplicate timestamps using `first|last|mean|median`.
- `dedupe`: drop exact duplicate records (same id, timestamp, and payload) from
  an already sorted feature stream.
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

- `sequence`: sliding window generator (`size`, `stride`, optional `cadence` to
  enforce contiguous windows). Emits `FeatureRecordSequence` payloads with `.records`.

### Vector (Postprocess) Transforms

- `drop`: apply coverage thresholds along the horizontal axis (vectors) or
  vertical axis (features/partitions) using `axis: horizontal|vertical` and
  `threshold`. Vertical mode requires the optional `metadata.json`
  artifact and internally prunes weak partitions.
- `fill`: impute using rolling statistics from prior vectors (history-based).
- `replace`: seed missing IDs with a constant or literal value.
  (Jerry automatically enforces the `schema.json` vector schema—ordering +
  cadence—before any configured vector transforms run.)

All transforms share a consistent entry-point signature and accept their config
dict as keyword arguments. Register new ones in `pyproject.toml` under the
appropriate group (`record`, `stream`, `feature`, `sequence`, `vector`,
`filters`, `debug`).

---

## Artifacts & Postprocess

- `expected.txt`: newline-delimited full feature IDs, generated on demand via
  `jerry inspect expected`. Not required at runtime; transforms derive the
  expected universe from `schema.json`.
- `schema.json`: output of the `schema` task. Jerry automatically
  enforces this schema during postprocess to impose deterministic ordering and
  list cadence metadata (targets appear whenever the dataset defines them). Window metadata now lives in `metadata.json`.
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
time.ticks = "my_datapipeline.mappers.synthetic.ticks:map"

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
