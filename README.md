# Jerry Thomas

Time‑Series First
- This runtime is time‑series‑first. Every domain record must include a timezone‑aware `time` and a `value`.
- Grouping is defined by time buckets only (`group_by.keys: [ { type: time, ... } ]`).
- Feature streams are sorted by time; sequence transforms assume ordered series.
- Categorical dimensions (e.g., station, zone, ticker) belong in `partition_by` so they become partitions of the same time series.
- Non‑temporal grouping is not supported.

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
| `src/datapipeline/transforms` & `src/datapipeline/filters` | Built-in transforms (lagging timestamps, scaling, sliding windows) and filter helpers exposed through entry points (`transforms/record.py`, `transforms/feature.py`, `transforms/sequence.py`, `filters/filters.py`). |
| `src/datapipeline/sources/synthetic/time`                  | Example synthetic time-series loader/parser pair plus helper mappers for experimentation while the real spirits arrive (`sources/synthetic/time/loader.py`, `sources/synthetic/time/parser.py`, `mappers/synthetic/time.py`). |

---

## Built-in DSL identifiers

The YAML DSL resolves filters and transforms by entry-point name. These ship with the
template out of the box:

| Kind              | Identifiers                                                                                     | Notes |
| ----------------- | ----------------------------------------------------------------------------------------------- | ----- |
| Filters           | `eq`/`equals`, `ne`/`not_equal`, `lt`, `le`, `gt`, `ge`, `in`/`contains`, `nin`/`not_in`        | Use as `- gt: { field: value }` or `- in: { field: [values...] }`. Synonyms map to the same implementation. |
| Record transforms | `time_lag`, `drop_missing`                                                                       | `time_lag` expects a duration string (e.g. `1h`), `drop_missing` removes `None`/`NaN` records. |
| Feature transforms| `standard_scale`                                                                                | Options: `with_mean`, `with_std`, optional `statistics`. |
| Sequence transforms | `time_window`, `time_fill_mean`, `time_fill_median`                                           | `time_window` builds sliding windows; the fill transforms impute missing values from running mean/median with optional `window`/`min_samples`. |
| Vector transforms   | `fill_history`, `fill_horizontal`, `fill_constant`, `drop_missing`                           | History fill uses prior buckets, horizontal fill aggregates sibling partitions, constant sets a default, and drop removes vectors below coverage thresholds. |

Extend `pyproject.toml` with additional entry points to register custom logic under your
own identifiers.

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

Create a `config/recipes/<name>/project.yaml` so the runtime knows where to find
ingredients, infusions and the tasting menu. Globals are optional but handy for sharing
values—they are interpolated into downstream YAML specs during bootstrap
(`src/datapipeline/config/project.py`, `src/datapipeline/services/bootstrap.py`).

```yaml
version: 1
paths:
  sources: ../../sources
  streams: ../../contracts
  dataset: dataset.yaml
globals:
  opening_time: "2024-01-01T16:00:00Z"
  last_call: "2024-01-02T02:00:00Z"
```

> Helper functions in `src/datapipeline/services/project_paths.py` resolve relative paths
> against the project root and ensure the mise en place folders exist.

### 3. Stock the bottles (raw sources)

Create `config/sources/<alias>.yaml` files. Each must expose a `parser` and `loader`
pointing at entry points plus any constructor arguments
(`src/datapipeline/services/bootstrap.py`). Here is a synthetic clock source that feels
like a drip of barrel-aged bitters:

```yaml
# config/sources/time_ticks.yaml
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

The mapper uses the provided mode to create a new `TimeSeriesRecord` stream ready for the
feature stage (`mappers/synthetic/time.py`).

### 5. Script the tasting menu (dataset)

Datasets describe which canonical streams should be read at each station and how flights
are grouped (`src/datapipeline/config/dataset/dataset.py`). A minimal hourly menu might
look like:

```yaml
# config/recipes/default/dataset.yaml
group_by:
  keys:
    - type: time
      field: time
      resolution: 1h
features:
  - id: hour_spritz
    stream: time.encode
    transforms:
      - record:
          transform: time_lag
          args: 0h
      - feature:
          transform: standard_scale
          with_mean: true
          with_std: true
      - sequence:
          transform: time_window
          size: 4
          stride: 1
      - sequence:
          transform: time_fill_mean
          window: 24
          min_samples: 6
```

Use the sample `dataset` template as a starting point if you prefer scaffolding before
pouring concrete values. Group keys now require explicit time bucketing (with automatic
flooring to the requested resolution) so every pipeline is clock-driven. You can attach
feature or sequence transforms—such as the sliding `TimeWindowTransformer` or the
`time_fill_mean`/`time_fill_median` imputers—directly in the YAML by referencing their
entry point names (`src/datapipeline/transforms/sequence.py`).

When vectors are assembled you can optionally apply `vector_transforms` to enforce schema
guarantees. The built-ins cover:

- `fill_history` – use running means/medians from prior buckets (per partition) with
  configurable window/minimum samples.
- `fill_horizontal` – aggregate sibling partitions at the same timestamp (e.g. other
  stations) using mean/median.
- `fill_constant` – provide a constant default for missing features/partitions.
- `drop_missing` – drop vectors that fall below a coverage threshold or omit required
  features.

Transforms accept either an explicit `expected` list or a manifest path to discover the
full partition set (`build/partitions.json` produced by `jerry inspect partitions`).

Once the book is ready, run the bootstrapper (the CLI does this automatically) to
materialize all registered sources and streams
(`src/datapipeline/services/bootstrap.py`).

---

## Running service

### Prep any station (with visuals)

```bash
jerry prep pour   --project config/datasets/default/project.yaml --limit 20
jerry prep build  --project config/datasets/default/project.yaml --limit 20
jerry prep stir   --project config/datasets/default/project.yaml --limit 20
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
# Features only (default)
jerry serve --project config/datasets/default/project.yaml --output print
jerry serve --project config/datasets/default/project.yaml --output stream
jerry serve --project config/datasets/default/project.yaml --output exports/batch.pt

# Include targets in vectors (training-friendly)
jerry serve --project config/datasets/default/project.yaml --include-targets --output stream
```

Production mode skips the bar flair and focuses on throughput. `print` writes tasting
notes to stdout, `stream` emits newline-delimited JSON (with values coerced to strings when
necessary), and a `.pt` destination stores a pickle-compatible payload for later pours.

## Funnel vectors into ML projects

Data scientists rarely want to shell out to the CLI; they need a programmatic
hand-off that plugs vectors straight into notebooks, feature stores or training
loops. The `datapipeline.integrations` package wraps the existing iterator
builders with ML-friendly adapters without pulling pandas or torch into the
core runtime.

```python
from datapipeline.integrations import (
    VectorAdapter,
    dataframe_from_vectors,
    iter_vector_rows,
    torch_dataset,
)

# Bootstrap once and stream ready-to-use rows.
adapter = VectorAdapter.from_project("config/project.yaml")
for row in adapter.iter_rows(limit=32, flatten_sequences=True, include_targets=True):
    send_to_feature_store(row)

# Helper functions cover ad-hoc jobs as well.
rows = iter_vector_rows(
    "config/project.yaml",
    include_group=True,
    group_format="mapping",
    flatten_sequences=True,
    include_targets=True,
)

# Optional extras materialize into common ML containers if installed.
df = dataframe_from_vectors("config/project.yaml", include_targets=True)                # Requires pandas
dataset = torch_dataset(
    "config/project.yaml",
    dtype=torch.float32,
    include_targets=True,  # auto-derive target columns from dataset.targets
)  # Requires torch
```

Everything still flows through `build_vector_pipeline`; the integration layer
normalizes group keys, optionally flattens sequence features and demonstrates
how to turn the iterator into DataFrames or `torch.utils.data.Dataset`
instances. ML teams can fork the same pattern for their own stacks—Spark, NumPy
or feature store SDKs—without adding opinionated glue to the runtime itself.

### Inspect the balance (vector quality)

Use the inspect helpers for different outputs:

- `jerry inspect report --project config/datasets/default/project.yaml` — print a
  human-readable quality report (totals, keep/below lists, optional partition detail).
- Add `--include-targets` to include dataset.targets in the analysis.
- `jerry inspect coverage --project config/datasets/default/project.yaml` — persist the
  coverage summary to `build/coverage.json` (keep/below feature and partition lists plus
  coverage percentages).
- Add `--include-targets` to include dataset.targets in the coverage computation.
- `jerry inspect matrix --project config/datasets/default/project.yaml --format html` —
  export availability matrices (CSV or HTML) for deeper analysis.
- Add `--include-targets` to include dataset.targets in the matrix.
- `jerry inspect partitions --project config/datasets/default/project.yaml` — write the
  observed partition manifest to `build/partitions.json` for use in configs.
- Add `--include-targets` to include dataset.targets in partition discovery.

Note: `jerry prep taste` has been removed; use `jerry inspect report` and friends.

---

## Extending the CLI

### Scaffold a plugin package

```bash
jerry plugin init --name my_datapipeline --out .
```

The generator copies a ready-made skeleton (pyproject, README, package directory) and
swaps placeholders for your package name so you can start adding new spirits immediately
(`src/datapipeline/cli/app.py`, `src/datapipeline/services/scaffold/plugin.py`). Install the
resulting project in editable mode to expose your loaders, parsers, mappers and
transforms.

### Create new sources, domains and contracts

Use the CLI helpers to scaffold boilerplate code in your plugin workspace:

```bash
jerry source add --provider dmi --dataset metobs --transport fs --format csv
jerry domain add --domain metobs
jerry contract
```

The source command writes DTO/parser stubs, updates entry points and drops a matching
YAML file in `config/sources/` pre-filled with composed-loader defaults for the chosen
transport (`src/datapipeline/cli/app.py`, `src/datapipeline/services/scaffold/source.py`).
`jerry domain add` now always scaffolds `TimeSeriesRecord` domains so every mapper carries
an explicit timestamp alongside its value, and `jerry contract` wires that source/domain
pair up for canonical stream generation.

### Add custom filters or transforms

Register new functions/classes under the appropriate entry point group in your plugin’s
`pyproject.toml`. The runtime resolves them through `load_ep`, applies record filters first,
then record/feature/sequence transforms in the order declared in the dataset config
(`pyproject.toml`, `src/datapipeline/utils/load.py`,
`src/datapipeline/pipeline/utils/transform_utils.py`). Built-in helpers cover common
comparisons (including timezone-aware checks) and time-based transforms (lags, sliding
windows) if you need quick wins (`src/datapipeline/filters/filters.py`,
`src/datapipeline/transforms/record.py`, `src/datapipeline/transforms/feature.py`,
`src/datapipeline/transforms/sequence.py`).

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
| `TimeSeriesRecord`  | Canonical record with `time` (tz-aware, normalized to UTC) and `value`; the pipeline treats streams as ordered series (`src/datapipeline/domain/record.py`).|
| `FeatureRecord`     | Links a record (or list of records from sequence transforms) to a `feature_id` and `group_key` (`src/datapipeline/domain/feature.py`).                      |
| `Vector`            | Final grouped payload: a mapping of feature IDs to scalars or ordered lists plus helper methods for shape/key access (`src/datapipeline/domain/vector.py`). |

---

## Developer shift checklist

These commands mirror the tooling used in CI and are useful while iterating locally:

```bash
pip install -e .[dev]
pytest
```
