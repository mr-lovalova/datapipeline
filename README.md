# Jerry Thomas

Jerry Thomas turns the datapipeline runtime into a cocktail program. You still install the
same Python package (`datapipeline`) and tap into the plugin architecture, but every CLI
dance step nods to a craft bar. Declarative YAML menus describe projects, sources and
datasets, pipelines move payloads through record/feature/vector stations, and setuptools
entry points keep the back bar stocked with new ingredients.

---

## How the bar is set up

```text
raw source → canonical stream → record stage → feature stage → vector stage
```

1. **Raw sources (bottles on the shelf)** bundle a loader + parser recipe. Loaders handle
   the I/O (files, URLs or synthetic runs) and parsers map rows into typed records while
   skimming the dregs (`src/datapipeline/sources/models/loader.py`,
   `src/datapipeline/sources/models/source.py`). The bootstrapper registers each source under
   an alias so you can order it later in the service flow (`src/datapipeline/streams/raw.py`,
   `src/datapipeline/services/bootstrap.py`).
2. **Canonical streams (house infusions)** optionally apply a mapper on top of a raw
   source to normalize payloads before the dataset drinks them
   (`src/datapipeline/streams/canonical.py`, `src/datapipeline/services/factories.py`).
3. **Dataset stages (prep stations)** read the configured canonical streams. Record stages
   are your strainers and shakers, feature stages bottle the clarified spirits into keyed
   features (with optional sequence transforms), and vector stages line up the flights ready
   for service (`src/datapipeline/pipeline/pipelines.py`, `src/datapipeline/pipeline/stages.py`,
   `src/datapipeline/config/dataset/feature.py`).
4. **Vectors (tasting flights)** carry grouped feature values; downstream tasters can
   inspect them for balance and completeness
   (`src/datapipeline/domain/vector.py`, `src/datapipeline/analysis/vector_analyzer.py`).

---

## Bar back cheat sheet

| Path                                                       | What lives here                                                                                                                                                                                                               |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/datapipeline/cli`                                     | Argparse-powered bar program with commands for running pipelines, inspecting pours, scaffolding plugins and projecting service flow (`cli/app.py`, `cli/openers.py`, `cli/visuals.py`).                                       |
| `src/datapipeline/services`                                | Bootstrapping (project loading, YAML interpolation), runtime factories and scaffolding helpers for new bar tools (`services/bootstrap.py`, `services/factories.py`, `services/scaffold/plugin.py`).                           |
| `src/datapipeline/pipeline`                                | Pure functions that build record/feature/vector iterators plus supporting utilities for ordering and transform wiring (`pipeline/pipelines.py`, `pipeline/utils/transform_utils.py`).                                         |
| `src/datapipeline/domain`                                  | Data structures representing records, feature records and vectors coming off the line (`domain/record.py`, `domain/feature.py`, `domain/vector.py`).                                                                          |
| `src/datapipeline/transforms` & `src/datapipeline/filters` | Built-in transforms (lagging timestamps, sliding windows) and filter helpers exposed through entry points (`transforms/transforms.py`, `transforms/sequence.py`, `filters/filters.py`).                                       |
| `src/datapipeline/sources/synthetic/time`                  | Example synthetic time-series loader/parser pair plus helper mappers for experimentation while the real spirits arrive (`sources/synthetic/time/loader.py`, `sources/synthetic/time/parser.py`, `mappers/synthetic/time.py`). |

---

## Opening the bar

### 1. Install the tools

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install jerry-thomas
```

The published wheel exposes the `jerry` CLI (backed by the `datapipeline` package) and
pulls in core dependencies like Pydantic, PyYAML, tqdm and Jinja2 (see
`pyproject.toml`). Prefer `pip install -e .` only when you are actively developing this
repository. Double-check the back bar is reachable:

```bash
python -c "import datapipeline; print('bar ready')"
```

### 2. Draft your bar book

Create a `config/project.yaml` so the runtime knows where to find ingredients, infusions
and the tasting menu. Globals are optional but handy for sharing values—they are
interpolated into downstream YAML specs during bootstrap
(`src/datapipeline/config/project.py`, `src/datapipeline/services/bootstrap.py`).

```yaml
version: 1
paths:
  sources: config/distilleries
  streams: config/contracts
  dataset: config/recipe.yaml
globals:
  opening_time: "2024-01-01T16:00:00Z"
  last_call: "2024-01-02T02:00:00Z"
```

> Helper functions in `src/datapipeline/services/project_paths.py` resolve relative paths
> against the project root and ensure the mise en place folders exist.

### 3. Stock the bottles (raw sources)

Create `config/distilleries/<alias>.yaml` files. Each must expose a `parser` and `loader`
pointing at entry points plus any constructor arguments
(`src/datapipeline/services/bootstrap.py`). Here is a synthetic clock source that feels
like a drip of barrel-aged bitters:

```yaml
# config/distilleries/time_ticks.yaml
parser:
  entrypoint: "synthetic.time"
  args: {}
loader:
  entrypoint: "synthetic.time"
  args:
    start: "${opening_time}"
    end: "${last_call}"
    frequency: "1h"
```

That file wires up the built-in `TimeTicksGenerator` + parser pair that yields
timezone-aware timestamps (`sources/synthetic/time/loader.py`,
`sources/synthetic/time/parser.py`).

### 4. Mix house infusions (canonical streams)

Canonical specs live under `config/contracts/` and reference a raw source alias plus an
optional mapper entry point (`src/datapipeline/services/bootstrap.py`,
`src/datapipeline/streams/canonical.py`). This example turns each timestamp into a citrus
spritz feature:

```yaml
# config/contracts/time/encode.yaml
source: time_ticks
mapper:
  entrypoint: "synthetic.time.encode"
  args:
    mode: spritz
```

The mapper uses the provided mode to create a new `TimeFeatureRecord` stream ready for the
feature stage (`mappers/synthetic/time.py`).

### 5. Script the tasting menu (dataset)

Datasets describe which canonical streams should be read at each station and how flights
are grouped (`src/datapipeline/config/dataset/dataset.py`). A minimal hourly menu might
look like:

```yaml
# config/recipe.yaml
group_by:
  keys:
    - type: time
      field: time
      resolution: 1h
features:
  - stream: time.encode
    feature_id: hour_spritz
    partition_by: null
    filters: []
    transforms:
      - time_lag: "0h"
```

Use the sample `dataset` template as a starting point if you prefer scaffolding before
pouring concrete values. Group keys support time bucketing (with automatic flooring to the
requested resolution) and categorical splits
(`src/datapipeline/config/dataset/group_by.py`,
`src/datapipeline/config/dataset/normalize.py`). You can also attach feature or sequence
transforms—such as the sliding `TimeWindowTransformer`—directly in the YAML by referencing
their entry point names (`src/datapipeline/transforms/sequence.py`).

Once the book is ready, run the bootstrapper (the CLI does this automatically) to
materialize all registered sources and streams
(`src/datapipeline/services/bootstrap.py`).

---

## Running service

### Prep any station (with visuals)

```bash
jerry prep pour   --project config/project.yaml --limit 20
jerry prep build  --project config/project.yaml --limit 20
jerry prep stir   --project config/project.yaml --limit 20
```

- `prep pour` shows the record-stage ingredients headed for each feature.
- `prep build` highlights `FeatureRecord` entries after the shake/strain sequence.
- `prep stir` emits grouped vectors—the tasting flight before it leaves the pass.

All variants respect `--limit` and display tqdm-powered progress bars for the underlying
loaders. The CLI wires up `build_record_pipeline`, `build_feature_pipeline` and
`build_vector_pipeline`, so what you see mirrors the service line
(`src/datapipeline/cli/app.py`, `src/datapipeline/cli/commands/run.py`,
`src/datapipeline/cli/openers.py`, `src/datapipeline/cli/visuals.py`,
`src/datapipeline/pipeline/pipelines.py`).

### Serve the flights (production mode)

```bash
jerry serve --project config/project.yaml --output print
jerry serve --project config/project.yaml --output stream
jerry serve --project config/project.yaml --output exports/batch.pt
```

Production mode skips the bar flair and focuses on throughput. `print` writes tasting
notes to stdout, `stream` emits newline-delimited JSON (with values coerced to strings when
necessary), and a `.pt` destination stores a pickle-compatible payload for later pours.

### Taste the balance (vector quality)

```bash
jerry taste --project config/project.yaml
```

This command reuses the vector pipeline, collects presence counts for every configured
feature and flags empty or incomplete flights so you can diagnose upstream issues quickly
(`src/datapipeline/cli/commands/analyze.py`, `src/datapipeline/analysis/vector_analyzer.py`).
Use `--limit` to spot-check during service.

---

## Extending the bar program

### Scaffold a plugin package

```bash
jerry station init --name my_datapipeline --out .
```

The generator copies a ready-made skeleton (pyproject, README, package directory) and
swaps placeholders for your package name so you can start adding new spirits immediately
(`src/datapipeline/cli/app.py`, `src/datapipeline/services/scaffold/plugin.py`). Install the
resulting project in editable mode to expose your loaders, parsers, mappers and
transforms.

### Create new sources, domains and contracts

Use the CLI helpers to scaffold boilerplate code in your plugin workspace:

```bash
jerry distillery add --provider dmi --dataset metobs --transport fs --format csv
jerry spirit add --domain metobs --time-aware
jerry contract --time-aware
```

The distillery command writes DTO/parser stubs, updates entry points and drops a matching
YAML file in `config/distilleries/` pre-filled with composed-loader defaults for the chosen
transport (`src/datapipeline/cli/app.py`, `src/datapipeline/services/scaffold/source.py`).

### Add custom filters or transforms

Register new functions/classes under the appropriate entry point group in your plugin’s
`pyproject.toml`. The runtime resolves them through `load_ep`, applies record-level
filters first, then record/feature/sequence transforms in the order declared in the
dataset config (`pyproject.toml`, `src/datapipeline/utils/load.py`,
`src/datapipeline/pipeline/utils/transform_utils.py`). Built-in helpers cover common
comparisons (including timezone-aware checks) and time-based transforms (lags, sliding
windows) if you need quick wins (`src/datapipeline/filters/filters.py`,
`src/datapipeline/transforms/transforms.py`, `src/datapipeline/transforms/sequence.py`).

### Prototype with synthetic time-series data

Need sample pours while wiring up transforms? Reuse the bundled synthetic time loader +
parser and season it with the `encode_time` mapper for engineered temporal features
(`src/datapipeline/sources/synthetic/time/loader.py`,
`src/datapipeline/sources/synthetic/time/parser.py`,
`src/datapipeline/mappers/synthetic/time.py`). Pair it with the `time_window` sequence
transform to build sliding-window feature flights without external datasets
(`src/datapipeline/transforms/sequence.py`).

---

## Data model tasting notes

| Type                | Description                                                                                                                                                 |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Record`            | Canonical payload containing a `value`; extended by other record types (`src/datapipeline/domain/record.py`).                                               |
| `TimeFeatureRecord` | A record with a timezone-aware `time` attribute, normalized to UTC to avoid boundary issues (`src/datapipeline/domain/record.py`).                          |
| `FeatureRecord`     | Links a record (or list of records from sequence transforms) to a `feature_id` and `group_key` (`src/datapipeline/domain/feature.py`).                      |
| `Vector`            | Final grouped payload: a mapping of feature IDs to scalars or ordered lists plus helper methods for shape/key access (`src/datapipeline/domain/vector.py`). |

---

## Developer shift checklist

These commands mirror the tooling used in CI and are useful while iterating locally:

```bash
pip install -e .[dev]
pytest
```
