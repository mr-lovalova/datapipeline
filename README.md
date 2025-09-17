# datapipeline

Stream-first, plugin-friendly data pipeline. Build **sources** from a **loader** + **parser**, transform them into **features**, and assemble **vectors** for modeling.

---

## Table of contents

- [Quick start](#quick-start)
- [Commands](#commands)
  - [List plugins](#list-plugins)
  - [Validate a dataset](#validate-a-dataset)
  - [Show the plan (dry-run)](#show-the-plan-dry-run)
  - [Run the pipeline](#run-the-pipeline)
  - [Plugin scaffolding](#plugin-scaffolding)
- [Project layout](#project-layout)
- [Core concepts](#core-concepts)
- [Example: CSV source (loader + parser + ComposedSource)](#example-csv-source-loader--parser--composedsource)
- [Minimal dataset.yaml](#minimal-datasetyaml)
- [Developer guide](#developer-guide)
- [Tips & gotchas](#tips--gotchas)

---

## Quick start

```bash
# 1) Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2) Install the project (editable) and dev tools
python -m pip install --upgrade pip
pip install -e ./lib/datapipeline
```

Verify:

```bash
python -c "import datapipeline; print('datapipeline OK')"
```

---

## Commands

> Depending on your entry point, you may have a `datapipeline` console command.  
> If not, you can always run via `python -m datapipeline.cli2 ...`. Both forms are shown below.

### List plugins

```bash
# console script
datapipeline plugins list

# module form
python -m datapipeline.cli2 plugins list
```

### Validate a dataset

```bash
datapipeline validate --config examples/dataset.yaml
# or
python -m datapipeline.cli2 validate --config examples/dataset.yaml
```

### Show the plan (dry-run)

```bash
datapipeline plan --config examples/dataset.yaml
# or
python -m datapipeline.cli2 plan --config examples/dataset.yaml
```


### Run the pipeline

```bash
datapipeline run --config examples/dataset.yaml --out ./out
# or
python -m datapipeline.cli2 run --config examples/dataset.yaml --out ./out
```

## Plugin scaffolding

Create and install your own plugin package (sources, parsers, DTOs, mappers) so the CLI can discover them via entry points.

```bash
# 1) Scaffold a plugin package (in your pipelines workspace)
datapipeline plugin init --name energy_data_pipeline --out .
cd energy_data_pipeline

# 2) Scaffold a source (provider + dataset → loader/parser boilerplate)
datapipeline source create \
  --provider dmi \
  --dataset metobs \
  --transport fs \
  --format csv

# 3) Create a domain package (defaults to Record; --time-aware uses TimeFeatureRecord)
datapipeline domain create --domain metobs --time-aware

# 4) Link the source to the domain (interactive mapper + canonical stream)
datapipeline link --time-aware

# 5) Install your plugin so entry points are registered
python -m pip install -e .

# 6) Confirm the CLI sees your registrations
datapipeline list sources
datapipeline list domains
```

Open the generated `config/sources/dmi_metobs.yaml` (path depends on your `project.yaml`) and replace the placeholder strings for path/delimiter with values that match your data source.

Use the plugin in `dataset.yaml` (type is `<origin>.<domain>`):

```yaml
sources:
  dmi_metobs:
    type: dmi.metobs  # provided by energy_data_pipeline
    data:
      path: "path/to/file.csv"
      delimiter: ","
      encoding: "utf-8"
```

**Notes**
- After changing entry points (pyproject.toml) in either the core or a plugin, reinstall in your active venv:
  - Core: `cd lib/datapipeline && python -m pip install -e .`
  - Plugin: `cd your-plugin && python -m pip install -e .`
- Restart Python processes (so importlib metadata refreshes).
- Prefer running commands via the active venv (`python -m ...`) if your PATH has multiple Python installs.

---

## Project layout

```
datapipeline/
  sources/
    models/
      base.py          # SourceInterface (abstract)
      loader.py        # RawDataLoader base (no-arg load(), optional count())
      parser.py        # DataParser base (parse(raw)->DTO|None)
      source.py        # Source: generic loader+parser → stream()
    transports.py      # fs/url transports yielding text streams
    decoders.py        # CsvDecoder/JsonDecoder/JsonLinesDecoder
    composed_loader.py # Compose transport+decoder into a RawDataLoader
    <origin>/<domain>/
      dto.py           # domain DTO for this source
      parser.py        # parse raw row → DTO (validate, drop on None)
      source.py        # subclass of ComposedSource
  transformers/        # record/feature transformers (optional)
  domain/              # Record/Vector models
  utils/               # progress bars, sorting, helpers
templates/
  stubs/               # Jinja stubs for new sources/parsers/DTOs
examples/
  dataset.yaml         # minimal runnable config
  data.csv            # optional sample data
```

---

## Core concepts

- **Loader** — how to enumerate raw data (files, globs, HTTP, DB). Implements:
  - `load() -> Iterator[Any]`
  - optional `count() -> int` (if cheap/available)
- **Parser** — convert a raw row into a domain DTO. Implements:
  - `parse(raw) -> DTO | None` (return `None` to drop bad rows)
- **Source** — generic source that wires **loader + parser** and provides `stream()` for you.
- **Transformers** — optional record/feature transforms.
- **Assembler** — builds aligned feature vectors by group/time keys.

---

## Example: CSV source (composed loader + parser + Source)

**DTO**

```python
# datapipeline/sources/myorigin/mydomain/dto.py
from dataclasses import dataclass
from datetime import datetime

@dataclass
class MyDto:
    time: datetime
    value: float
```

**Parser**

```python
# datapipeline/sources/myorigin/mydomain/parser.py
from typing import Optional
from datetime import datetime
from datapipeline.sources.models.parser import DataParser
from .dto import MyDto

class MyParser(DataParser[MyDto]):
    def parse(self, raw: dict) -> Optional[MyDto]:
        try:
            return MyDto(
                time=datetime.fromisoformat(raw["timestamp"]),
                value=float(raw["value"]),
            )
        except Exception:
            return None  # drop malformed rows
```

**Source (inherits the ready-made loop)**

```python
# datapipeline/sources/myorigin/mydomain/source.py
from datapipeline.sources.models.source import Source
from datapipeline.sources.composed_loader import ComposedRawLoader
from datapipeline.sources.transports import FsFileSource
from datapipeline.sources.decoders import CsvDecoder
from .parser import MyParser
from .dto import MyDto

class MySource(Source[MyDto]):
    def __init__(self, path: str, delimiter: str = ",", encoding: str = "utf-8", show_progress: bool = True):
        loader = ComposedRawLoader(
            FsFileSource(path, encoding=encoding),
            CsvDecoder(delimiter=delimiter),
        )
        parser = MyParser()
        super().__init__(loader=loader, parser=parser, show_progress=show_progress)
```

**Use the generic loader entry point** (the scaffolder pre-populates placeholders):

```toml
# In your source YAML
loader:
  entrypoint: "composed.loader"
  args:
    transport: fs
    format: "<FORMAT (csv|json|json-lines)>"
    path: "examples/data.csv"
    glob: false
    delimiter: ","
    encoding: "utf-8"
```

---

## Minimal `dataset.yaml`

```yaml
# examples/dataset.yaml
globals:
  time: &time
    start_time: "2024-01-01T00:00:00Z"
    end_time: "2024-01-02T00:00:00Z"

group_by:
  keys:
    - type: time
      field: time
      resolution: 1h

sources:
  example_csv:
    type: myorigin.mydomain
    data:
      path: "examples/data.csv"
      delimiter: ","
      encoding: "utf-8"

features:
  - feature_id: value
    origin: myorigin
    domain: mydomain
    source: example_csv

targets: []
```

Run it:

```bash
datapipeline validate --config examples/dataset.yaml
datapipeline run --config examples/dataset.yaml --out ./out
```

---

## Developer guide

**Run tests**

```bash
pytest -q
```

**Lint & format** (if included in `.[dev]`)

```bash
ruff check .
ruff fix .          # safe autofixes
black .
```

**Type-check**

```bash
mypy datapipeline
```

**Reinstall locally (editable)**

```bash
pip uninstall -y datapipeline
pip install -e ".[dev]"
```

**Build (optional)**

```bash
python -m build
```

---

## Tips & gotchas

- `RawDataLoader.load()` is **no-arg**; the base stores `path`/`pattern`.
- Prefer `if parsed is not None:` (don’t drop valid falsy values like `0`).
- Implement `count()` only if it’s cheap; otherwise return nothing and let progress bars run without totals.
- Keep class names aligned with entry-point strings in `pyproject.toml`.
- You usually **don’t** need to override `stream()`—that’s what `Source` is for. Only override when you need special behavior (pagination, retries, merging multiple loaders, etc.).
