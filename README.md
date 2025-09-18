# Jerry Thomas

Jerry Thomas pours a mixology theme over the original datapipeline runtime. It keeps the
same Python package (`datapipeline`) and plugin architecture, but reshakes the CLI so every
step feels like crafting a cocktail. Declarative YAML configs still describe projects,
sources and datasets, pipelines remain composable across record/feature/vector stages, and
setuptools entry points keep the system pluggable.

---

## How data flows

```text
raw source → canonical stream → record stage → feature stage → vector stage
```

1. **Raw sources** wrap a loader + parser pair. Loaders handle I/O (files, URLs or synthetic
generators) and parsers map rows into typed records while swallowing bad rows (`src/datapipeline/sources/models/loader.py`,
`src/datapipeline/sources/models/source.py`). The bootstrapper registers each source under an alias so it can be opened later by
the pipeline (`src/datapipeline/streams/raw.py`, `src/datapipeline/services/bootstrap.py`).
2. **Canonical streams** optionally apply a mapper on top of a raw source to normalize payloads before the dataset consumes them
(`src/datapipeline/streams/canonical.py`, `src/datapipeline/services/factories.py`).
3. **Dataset stages** read the configured canonical streams. Record stages apply filters/transforms, feature stages convert
records into keyed features (with optional sequence transforms), and vector stages group aligned features into dense dictionaries
ready for export (`src/datapipeline/pipeline/pipelines.py`, `src/datapipeline/pipeline/stages.py`, `src/datapipeline/config/dataset/feature.py`).
4. **Vectors** carry grouped feature values; downstream analyzers can inspect them for completeness and quality
(`src/datapipeline/domain/vector.py`, `src/datapipeline/analysis/vector_analyzer.py`).

---

## Repository tour

| Path | What lives here |
| --- | --- |
| `src/datapipeline/cli` | Argparse-powered CLI with commands for running pipelines, inspecting output, scaffolding plugins and visualizing source progress (`cli/app.py`, `cli/openers.py`, `cli/visuals.py`). |
| `src/datapipeline/services` | Bootstrapping (project loading, YAML interpolation), runtime factories and scaffolding helpers for plugin code generation (`services/bootstrap.py`, `services/factories.py`, `services/scaffold/plugin.py`). |
| `src/datapipeline/pipeline` | Pure functions that build record/feature/vector iterators plus supporting utilities for ordering and transform wiring (`pipeline/pipelines.py`, `pipeline/utils/transform_utils.py`). |
| `src/datapipeline/domain` | Data structures representing records, feature records and vectors produced by the pipeline (`domain/record.py`, `domain/feature.py`, `domain/vector.py`). |
| `src/datapipeline/transforms` & `src/datapipeline/filters` | Built-in transforms (lagging timestamps, sliding windows) and filter helpers exposed through entry points (`transforms/transforms.py`, `transforms/sequence.py`, `filters/filters.py`). |
| `src/datapipeline/sources/synthetic/time` | Example synthetic time-series loader/parser pair plus helper mappers you can reuse while prototyping (`sources/synthetic/time/loader.py`, `sources/synthetic/time/parser.py`, `mappers/synthetic/time.py`). |

---

## Getting started

### 1. Install the project

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .
```

The editable install exposes the `jerry` CLI (backed by the `datapipeline` package) and pulls in core dependencies like Pydantic,
PyYAML, tqdm and Jinja2 (see `pyproject.toml`). Verify everything imports:

```bash
python -c "import datapipeline; print('jerry ready')"
```

### 2. Describe your project

Create a `config/project.yaml` so the runtime knows where to find sources, canonical streams and the dataset definition. Globals
are optional but handy for sharing values—they are interpolated into downstream YAML files during bootstrap
(`src/datapipeline/config/project.py`, `src/datapipeline/services/bootstrap.py`).

```yaml
version: 1
paths:
  sources: config/sources
  streams: config/streams
  dataset: config/dataset.yaml
globals:
  start_time: "2024-01-01T00:00:00Z"
  end_time: "2024-01-02T00:00:00Z"
```

> The helper functions in `src/datapipeline/services/project_paths.py` resolve relative paths against the project root and ensure
the directories exist, so stick with those defaults unless you have a custom layout.

### 3. Register raw sources

Create `config/sources/<alias>.yaml` files. Each must expose a `parser` and `loader` block pointing at entry points plus any
constructor arguments (`src/datapipeline/services/bootstrap.py`). Here is a synthetic clock source:

```yaml
# config/sources/time_ticks.yaml
parser:
  entrypoint: "synthetic.time"
  args: {}
loader:
  entrypoint: "synthetic.time"
  args:
    start: "${start_time}"
    end: "${end_time}"
    frequency: "1h"
```

That file wires up the built-in `TimeTicksGenerator` + parser pair that yields timezone-aware timestamps
(`sources/synthetic/time/loader.py`, `sources/synthetic/time/parser.py`).

### 4. Define canonical streams (optional but recommended)

Canonical specs live under `config/streams/` and reference a raw source alias plus an optional mapper entry point
(`src/datapipeline/services/bootstrap.py`, `src/datapipeline/streams/canonical.py`). This example encodes each timestamp into a
sine wave feature:

```yaml
# config/streams/time/encode.yaml
source: time_ticks
...
```

The mapper uses the provided mode to create a new `TimeFeatureRecord` stream ready for feature processing
(`mappers/synthetic/time.py`).

### 5. Configure the dataset

Datasets describe which canonical streams should be read at each stage and how outputs are grouped
(`src/datapipeline/config/dataset/dataset.py`). A minimal hourly dataset might look like:

```yaml
# config/dataset.yaml
group_by:
  keys:
    - type: time
      field: time
      resolution: 1h
features:
  - stream: time.encode
    feature_id: hour_sin
    partition_by: null
    filters: []
    transforms:
      - time_lag: "0h"
```

Use the sample `dataset` template as a starting point if you prefer scaffolding before filling in concrete values. Group keys
support time bucketing (with automatic flooring to the requested resolution) and categorical splits
(`src/datapipeline/config/dataset/group_by.py`, `src/datapipeline/config/dataset/normalize.py`). You can also attach feature or
sequence transforms—such as the sliding `TimeWindowTransformer`—directly in the YAML by referencing their entry point names
(`src/datapipeline/transforms/sequence.py`).

When you are ready to execute, run the bootstrapper once (the CLI does this automatically) to materialize all registered sources
and streams (`src/datapipeline/services/bootstrap.py`).

---

## Running and inspecting pipelines

### Prep any stage (with visuals)

```bash
jerry prep pour   --project config/project.yaml --limit 20
jerry prep build  --project config/project.yaml --limit 20
jerry prep stir   --project config/project.yaml --limit 20
```

* `prep pour` prints the record-stage payloads per feature.
* `prep build` shows `FeatureRecord` entries after feature/sequence transforms.
* `prep stir` emits grouped vectors.

All variants respect `--limit` and display tqdm-powered progress bars for the underlying loaders. The CLI wires up
`build_record_pipeline`, `build_feature_pipeline` and `build_vector_pipeline`, so the results mirror what downstream consumers see
(`cli/app.py`, `cli/commands/run.py`, `cli/openers.py`, `cli/visuals.py`, `pipeline/pipelines.py`).

### Serve vectors (production mode)

```bash
jerry serve --project config/project.yaml --output print
jerry serve --project config/project.yaml --output stream
jerry serve --project config/project.yaml --output exports/batch.pt
```

Production mode skips the tqdm visuals and focuses on throughput. `print` writes friendly summaries to stdout, `stream` emits
newline-delimited JSON (with values coerced to strings when necessary), and a `.pt` destination stores a pickle-compatible payload
for later loading.

### Taste vector quality

```bash
jerry taste --project config/project.yaml
```

This command reuses the vector pipeline, collects presence counts for every configured feature and highlights empty or incomplete
vectors so you can diagnose upstream issues quickly (`cli/commands/analyze.py`, `analysis/vector_analyzer.py`). Use `--limit` to
inspect a subset during development.

---

## Extending the system

### Scaffold a plugin package

```bash
jerry station init --name energy_data_pipeline --out .
```

The generator copies a ready-made skeleton (pyproject, README, package directory) and swaps placeholders for your package name so
you can start adding entry points immediately (`cli/app.py`, `services/scaffold/plugin.py`). Install the resulting project in
editable mode to expose your new loaders, parsers, mappers and transforms.

### Create new sources, domains and contracts

Use the CLI helpers to scaffold boilerplate code in your plugin workspace:

```bash
jerry distillery add --provider dmi --dataset metobs --transport fs --format csv
jerry spirit add --domain metobs --time-aware
jerry contract --time-aware
```

The distillery command writes DTO/parser stubs, updates entry points and drops a matching YAML file pre-filled with composed-loader
defaults for the chosen transport (`cli/app.py`, `services/scaffold/source.py`).

### Add custom filters or transforms

Register new functions/classes under the appropriate entry point group in your plugin’s `pyproject.toml`. The runtime resolves them
through `load_ep`, applies record-level filters first, then record/feature/sequence transforms in the order declared in the dataset
config (`pyproject.toml`, `src/datapipeline/utils/load.py`, `src/datapipeline/pipeline/utils/transform_utils.py`). Built-in helpers
cover common comparisons (including timezone-aware comparisons) and time-based transforms (lags, sliding windows) if you need quick
wins (`filters/filters.py`, `transforms/transforms.py`, `transforms/sequence.py`).

### Prototype with synthetic time-series data

Need sample data while wiring up transforms? Reuse the bundled synthetic time loader + parser and enrich it with the `encode_time`
mapper for engineered temporal features (`sources/synthetic/time/loader.py`, `sources/synthetic/time/parser.py`,
`mappers/synthetic/time.py`). Pair it with the `time_window` sequence transform to build sliding-window feature vectors without
external dependencies (`transforms/sequence.py`).

---

## Data model cheat sheet

| Type | Description |
| --- | --- |
| `Record` | Canonical payload containing a `value`; extended by other record types (`domain/record.py`). |
| `TimeFeatureRecord` | A record with a timezone-aware `time` attribute, normalized to UTC to avoid boundary issues (`domain/record.py`). |
| `FeatureRecord` | Links a record (or list of records from sequence transforms) to a `feature_id` and `group_key` (`domain/feature.py`). |
| `Vector` | Final grouped payload: a mapping of feature IDs to scalars or ordered lists plus helper methods for shape/key access (`domain/vector.py`). |

---

## Developer workflow

These commands mirror the tooling used in CI and are useful while iterating locally:

```bash
pip install -e .[dev]
pytest
```
