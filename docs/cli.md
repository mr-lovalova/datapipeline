# CLI Reference

All commands live under the `jerry` entry point (`src/datapipeline/cli/app.py`).
Pass `--help` on any command for flags.
All commands that take a project accept either `--project <path/to/project.yaml>` or `--dataset <alias>` (from `jerry.yaml datasets:`).

With `--visuals on` in an interactive terminal, runtime commands show one live
DAG row with the active node. `--log-level DEBUG` expands that view to one row
per active node. File-backed sources include the current file and position
(`2/17`); bars remain indeterminate when the record total is not known without
reading the data. Visuals are independent of log filtering.

### Preview Indices

- `jerry serve --project <project.yaml> --preview-index <0-13> --limit N [--log-level LEVEL] [--visuals on|off] [--heartbeat-interval SECONDS]`
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
  - Omit `--preview-index` to run the full pipeline and output persistence.
  - Use `--log-level DEBUG` for full debug output; the CLI default is `WARNING`.
  - Before runtime execution, Jerry combines the artifact requirements of all
    selected profiles and prepares that union once. The artifact graph orders
    those internal jobs; it never changes profile order.
- `jerry serve --project <project.yaml> --output-transport stdout --output-format jsonl --output-view flat|raw --output-encoding <codec> --limit N [--artifact-mode AUTO|FORCE|OFF] [--log-level LEVEL] [--visuals on|off] [--heartbeat-interval SECONDS] [--run name]`
  - Applies postprocess transforms before emitting. A profile with `splits:` routes samples to one fs output per requested split label, names files `<profile>.<label>.<ext>`, and cannot be combined with `--preview-index`. `--limit` applies separately to each selected label.
  - Use `--output-transport fs --output-format jsonl --output-directory build/serve` (or `csv`, `pickle`) to write outputs under `<output-directory>/runs/<run_id>/dataset/`.
  - `--output-view` controls payload shape:
    - `flat`: key + kind + flattened fields
    - `raw`: key + kind + raw object
  - If `--output-view` is omitted: `jsonl -> raw`, `csv/pickle -> flat`.
  - `csv` supports `flat` view.
  - `--output-encoding` applies to fs `jsonl`/`csv` outputs (default `utf-8`).
  - Set `--log-level DEBUG` (or set `observability.logging.level: DEBUG` in the serve profile) to increase log detail for preview indices.
  - Set `--heartbeat-interval 0` to disable persistent node heartbeat records. Live progress remains enabled when visuals are on. The CLI value also controls the shared artifact prerequisite phase; profile `observability.heartbeat_interval_seconds` begins applying only when that profile runs.
  - When multiple serve profiles exist, add `--run <profile-name>` to target a
    single profile; otherwise every enabled profile is executed in its exact
    configured order.
  - `artifact_mode` controls the command-level prerequisite phase: `AUTO`
    builds missing or stale artifacts, `FORCE` rebuilds the required closure,
    and `OFF` only accepts artifacts that are already current. The CLI
    `--artifact-mode` override applies to the whole command.
  - Without a CLI override, all selected profiles must resolve to the same
    `artifact_mode`; conflicting modes fail before artifact or runtime work starts.
  - Argument precedence follows the order described under _Configuration & Resolution Order_.

### Build & Quality

- `jerry inspect --project <project.yaml> [--run <inspect-profile>] [--artifact-mode AUTO|FORCE|OFF] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Runs inspect profiles declared as `profiles/inspect.<name>.yaml` (`cmd: inspect`).
  - Without `--run`, executes all enabled inspect profiles.
  - Use `--run coverage`, `--run matrix`, or `--run thresholds` to execute one profile.
  - Like `serve`, prepares the union of selected profiles' artifact requirements
    once, then executes the profiles in their exact configured order.
  - Profile targets map to runtime operations in `tasks/operations/`
    (`core.runtime.coverage`, `core.runtime.matrix`, and
    `core.runtime.thresholds`).
- `jerry inspect --project <project.yaml> --run matrix`
  - Typical matrix profile run. Matrix output format/path is controlled by the
    inspect profile and output flags; the matrix operation has no task options.
- `jerry inspect --project <project.yaml> --run thresholds`
  - Runs the threshold report. Behavior is controlled by the target operation
    `options` (for example `sort` and `threshold`).
- `jerry build --project <project.yaml> [--run <profile>] [--force] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Regenerates artifact tasks declared under `project.paths.tasks` when the configuration hash changes.
  - If `cmd: build` profiles are defined, enabled profiles run by default; use `--run` to target one profile.
  - Each build profile executes one configured artifact task `target`; selected
    profiles must have distinct targets.
  - Build profiles remain explicit artifact roots and execute in their configured
    profile order. The graph orders only the internal dependency jobs needed by
    each root; it never reorders the profiles. A selected dependency profile
    must be ordered before a selected dependent profile.
- `jerry materialize [--run <profile>] [--output <path.jsonl>] [--overwrite|--no-overwrite] [--visuals on|off] [--heartbeat-interval SECONDS]`
  - Runs every enabled `profiles/materialize.<name>.yaml` entry in configured
    order, or one profile selected by `--run`.
  - Checks every selected output and metadata file before the first profile
    starts writing.
  - A CLI overwrite choice applies to every selected profile. Without it, each
    profile uses its own `overwrite` setting or `materialize.defaults.yaml`.
  - `--output` overrides one selected profile and therefore requires `--run`.
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
