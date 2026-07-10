# CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.
All commands that take a project accept either `--project <path/to/project.yaml>` or `--dataset <alias>` (from `jerry.yaml datasets:`).

### Preview Indices

- `jerry serve --project <project.yaml> --preview-index <0-14> --limit N [--log-level LEVEL] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Index 0: ingest source records
  - Index 1: ingest mapped domain records
  - Index 2: ingest record transforms applied
  - Index 3: ingest ordered records
  - Index 4: stream input records
  - Index 5: stream mapped records
  - Index 6: stream ordered records
  - Index 7: stream transforms applied
  - Index 8: debug transforms applied
  - Index 9: feature records
  - Index 10: feature transforms/sequence outputs
  - Index 11: ordered feature records
  - Index 12: vectors/samples assembled across configured features and targets
  - Index 13: postprocess transforms applied
  - Index 14: configured split applied
  - Omit `--preview-index` to run the full pipeline and output persistence.
  - Use `--log-level DEBUG` for full debug output; the CLI default is `WARNING`.
  - In build mode `AUTO`, a runtime target builds its required artifact graph
    before it runs. Artifact-targeted profiles remain useful when you want an
    explicit named build step, but profile order is not the dependency mechanism.
- `jerry serve --project <project.yaml> --output-transport stdout --output-format jsonl --output-view flat|raw --output-encoding <codec> --limit N [--log-level LEVEL] [--visuals on|off] [--heartbeat-interval SECONDS] [--run name]`
  - Applies postprocess transforms and optional dataset split before emitting.
  - Use `--output-transport fs --output-format jsonl --output-directory build/serve` (or `csv`, `pickle`) to write artifacts to disk instead of stdout; files land under `<output-directory>/<run_name>/`.
  - `--output-view` controls payload shape:
    - `flat`: key + kind + flattened fields
    - `raw`: key + kind + raw object
  - If `--output-view` is omitted: `jsonl -> raw`, `csv/pickle -> flat`.
  - `csv` supports `flat` view.
  - `--output-encoding` applies to fs `jsonl`/`csv` outputs (default `utf-8`).
  - Set `--log-level DEBUG` (or set `observability.logging.level: DEBUG` in the serve profile) to increase log detail for preview indices.
  - Set `--heartbeat-interval 0` to disable node heartbeat; profile config uses `observability.heartbeat_interval_seconds`.
  - When multiple serve profiles exist, add `--run <profile-name>` to target a single profile; otherwise every enabled profile is executed sequentially.
  - `--run` selects only that profile. Its artifact closure is built according
    to `AUTO|FORCE|OFF`; with `--skip-build` or `OFF`, every required artifact
    must already be current.
  - Argument precedence follows the order described under _Configuration & Resolution Order_.
  - Combine with `--skip-build` when you already have fresh artifacts and want to jump straight into streaming.

### Build & Quality

- `jerry inspect --project <project.yaml> [--run <inspect-profile>] [--skip-build] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Runs inspect profiles declared as `profiles/inspect.<name>.yaml` (`cmd: inspect`).
  - Without `--run`, executes all enabled inspect profiles.
  - Use `--run coverage`, `--run matrix`, or `--run thresholds` to execute one profile.
  - Profile targets map to runtime operations in `tasks/operations/`
    (`core.runtime.coverage`, `core.runtime.matrix`, and
    `core.runtime.thresholds`).
- `jerry inspect --project <project.yaml> --run matrix`
  - Typical matrix profile run. Matrix output format/path is controlled via the target operation `options` and/or output flags.
- `jerry inspect --project <project.yaml> --run thresholds`
  - Runs the threshold report. Behavior is controlled by the target operation
    `options` (for example `sort` and `threshold`).
- `jerry build --project <project.yaml> [--run <profile>] [--force] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Regenerates artifact tasks declared under `project.paths.tasks` when the configuration hash changes.
  - If `cmd: build` profiles are defined, enabled profiles run by default; use `--run` to target one profile.
  - Each build profile executes one configured `target` operation task.
- `jerry materialize [--heartbeat-interval SECONDS] stream <stream_id> --output <path.jsonl> [--as <new_stream_id>] [--force]`
  - Writes a durable JSONL record stream.
  - With `--as`, also writes reusable source and ingest YAML using explicit `ordered_by`.
- `jerry clean [--yes] [--older-than <age>]`
  - Lists stale sort spill directories by default.
  - Add `--yes` to remove them.

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
