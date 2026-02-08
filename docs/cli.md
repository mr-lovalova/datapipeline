# CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.
All commands that take a project accept either `--project <path/to/project.yaml>` or `--dataset <alias>` (from `jerry.yaml datasets:`).

### Preview Stages

- `jerry serve --project <project.yaml> --stage <0-8> --limit N [--log-level LEVEL] [--visuals auto|tqdm|rich|off] [--progress auto|spinner|bars|off]`
  - Stage 0: raw DTOs
  - Stage 1: domain `TemporalRecord`s
  - Stage 2: record transforms applied
  - Stage 3: ordered record stream
  - Stage 4: stream transforms (ensure_cadence, fill, granularity, debug)
  - Stage 5: feature records (field selected)
  - Stage 6: feature transforms/sequence outputs
  - Stage 7: vectors assembled (no postprocess)
  - Stage 8: vectors + postprocess transforms
  - Use `--log-level DEBUG` for progress bars; the default is typically `INFO` (or `jerry.yaml.shared.log_level` when set).
  - Ensures build artifacts are current before streaming; the build step only runs when the configuration hash changes unless you pass `--stage` 0-6 (auto-skip) or opt out with `--skip-build`. Stage 6 may require scaler artifacts.
- `jerry serve --project <project.yaml> --output-transport stdout --output-format jsonl --output-view flat|raw|numeric --output-encoding <codec> --limit N [--log-level LEVEL] [--visuals ...] [--progress ...] [--run name]`
  - Applies postprocess transforms and optional dataset split before emitting.
  - Use `--output-transport fs --output-format jsonl --output-directory build/serve` (or `csv`, `pickle`) to write artifacts to disk instead of stdout; files land under `<output-directory>/<run_name>/`.
  - `--output-view` controls payload shape:
    - `flat`: key + kind + flattened fields
    - `raw`: key + kind + raw object
    - `numeric`: key + kind + ordered numeric values
  - If `--output-view` is omitted: `print/jsonl -> raw`, `csv/pickle -> flat`.
  - `csv` supports `flat` and `numeric` views.
  - `--output-encoding` applies to fs `jsonl`/`csv` outputs (default `utf-8`).
  - Set `--log-level DEBUG` (or set your serve task `log_level: DEBUG`) to reuse the tqdm progress bars when previewing stages.
  - When multiple serve tasks exist, add `--run val` (task name or filename stem) to target a single config; otherwise every enabled task is executed sequentially.
  - Argument precedence follows the order described under _Configuration & Resolution Order_.
  - Combine with `--skip-build` when you already have fresh artifacts and want to jump straight into streaming.

### Build & Quality

- `jerry inspect report --project <project.yaml> [--threshold 0.95] [--match-partition base|full] [--mode final|raw] [--sort missing|nulls]`
  - Prints a quality/coverage report to stdout.
  - `--mode final` applies postprocess transforms; `--mode raw` skips them.
- `jerry inspect matrix --project <project.yaml> [--format html|csv] [--output <path>] [--rows N] [--cols N] [--quiet] [--mode final|raw]`
  - Writes an availability matrix (defaults to `build/matrix.html`).
- `jerry inspect partitions --project <project.yaml> [--output <path>]`
  - Writes a partitions manifest JSON (defaults to `build/partitions.json`).
- `jerry build --project <project.yaml> [--force] [--visuals ...] [--progress ...]`
  - Regenerates artifact tasks declared under `project.paths.tasks` when the configuration hash changes.

### Scaffolding & Reference

- `jerry plugin init <package> --out <dir>` (also supports `-n/--name`)
  - Generates a plugin project (pyproject, package skeleton, config templates).
- `jerry demo init`
  - Generates a standalone demo plugin at `./demo/` and wires a `demo` dataset alias.
- `jerry inflow create`
  - Wizard to scaffold a complete ingest flow (source YAML + parser/DTO + mapper + contract).
- `jerry source create <provider>.<dataset> --transport fs|http|synthetic --format csv|json|jsonl|pickle`
  - Also supports positional `<provider> <dataset>` and `--alias <provider>.<dataset>`.
  - Creates a source YAML only (no Python code).
- `jerry domain create <name>` (also supports `-n/--name`)
  - Adds a `domains/<name>/` package with a `model.py` stub.
- `jerry filter create --name <identifier>`
  - Scaffolds an entry-point-ready filter (helpful for custom record predicates).
- `jerry list sources|domains`
  - Introspect configured source aliases or domain packages.

---
