# CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.
All commands that take a project accept either `--project <path/to/project.yaml>` or `--dataset <alias>` (from `jerry.yaml datasets:`).

### Preview Indices

- `jerry serve --project <project.yaml> --preview-index <0-11> --limit N [--log-level LEVEL] [--visuals on|off]`
  - Index 0: raw DTOs
  - Index 1: domain `TemporalRecord`s
  - Index 2: record transforms applied
  - Index 3: ordered record stream
  - Index 4: stream transforms (ensure_cadence, fill, granularity)
  - Index 5: debug transforms
  - Index 6: feature records (field selected)
  - Index 7: feature transforms/sequence outputs
  - Index 8: ordered feature records
  - Index 9: vectors/samples assembled across configured features and targets
  - Index 10: postprocess transforms applied
  - Index 11: configured split applied
  - Omit `--preview-index` to run the full pipeline and output persistence.
  - Use `--log-level DEBUG` for full debug output; default is `INFO`.
  - Artifact preparation is profile-driven: keep artifact-targeted serve profiles (`serve.schema`, `serve.metadata`, `serve.scaler`) ahead of runtime targets (`serve.train`/`serve.val`/`serve.test`) using `order`.
- `jerry serve --project <project.yaml> --output-transport stdout --output-format jsonl --output-view flat|raw|values --output-encoding <codec> --limit N [--log-level LEVEL] [--visuals on|off] [--run name]`
  - Applies postprocess transforms and optional dataset split before emitting.
  - Use `--output-transport fs --output-format jsonl --output-directory build/serve` (or `csv`, `pickle`) to write artifacts to disk instead of stdout; files land under `<output-directory>/<run_name>/`.
  - `--output-view` controls payload shape:
    - `flat`: key + kind + flattened fields
    - `raw`: key + kind + raw object
    - `values`: key + kind + ordered values (mixed primitive types allowed)
  - If `--output-view` is omitted: `jsonl -> raw`, `csv/pickle -> flat`.
  - `csv` supports `flat` and `values` views.
  - `--output-encoding` applies to fs `jsonl`/`csv` outputs (default `utf-8`).
  - Set `--log-level DEBUG` (or set `observability.logging.level: DEBUG` in the serve profile) to increase log detail for preview indices.
  - When multiple serve profiles exist, add `--run <profile-name>` to target a single profile; otherwise every enabled profile is executed sequentially.
  - `--run` does not implicitly execute earlier serve profiles, so selecting a runtime profile assumes required artifacts already exist (or are produced by that selected profile).
  - Argument precedence follows the order described under _Configuration & Resolution Order_.
  - Combine with `--skip-build` when you already have fresh artifacts and want to jump straight into streaming.

### Build & Quality

- `jerry inspect --project <project.yaml> [--run <inspect-profile>] [--skip-build] [--visuals on|off]`
  - Runs inspect profiles declared as `profiles/inspect.<name>.yaml` (`cmd: inspect`).
  - Without `--run`, executes all enabled inspect profiles.
  - Use `--run report`, `--run matrix`, etc to execute one profile.
  - Profile targets map to inspect operations in `tasks/operations/` (`core.inspect.report`, `core.inspect.matrix`).
- `jerry inspect --project <project.yaml> --run matrix`
  - Typical matrix profile run. Matrix output format/path is controlled via the target operation `options` and/or output flags.
- `jerry inspect --project <project.yaml> --run report`
  - Typical report profile run. Report behavior is controlled by the target operation `options` (for example `mode`, `sort`, `threshold`).
- `jerry build --project <project.yaml> [--run <profile>] [--force] [--visuals on|off]`
  - Regenerates artifact tasks declared under `project.paths.tasks` when the configuration hash changes.
  - If `kind: build` profiles are defined, enabled profiles run by default; use `--run` to target one profile.
  - Each build profile executes one configured `target` operation task.

### Scaffolding & Reference

- `jerry plugin init <package> --out <dir>` (also supports `-n/--name`)
  - Generates a plugin project (pyproject, package skeleton, config templates).
- `jerry demo init`
  - Generates a standalone demo plugin at `./demo/` and wires a `demo` dataset alias.
- `jerry inflow create`
  - Wizard to scaffold a complete ingest flow (source YAML + parser/DTO + mapper + stream).
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
