# Artifacts

Artifact tasks are declared under `tasks/operations/`; built-in task models use
these default outputs:

An artifact's registry key is its task `id` (`scaler`, `vector_inputs`,
`schema`, `metadata`, or `stats`). Custom tick artifacts likewise use their
task ID, such as `model_grid`.

- `build/vector_inputs/manifest.json` plus compressed feature/target shards:
  durable inputs consumed by vector assembly. Values may contain only `None`,
  `bool`, `int`, `float`, `str`, lists, and string-keyed dictionaries;
  sample-key components may use only those scalar types. Other Python objects
  fail the build instead of being converted to strings.
- `build/scaler.json`: managed scaler statistics fitted on the configured split,
  either as one standard scaler or a folded temporal scaler container. Features
  with an explicit `scale.model_path` do not use this artifact.
- `build/schema.json`: discovered feature/target identifiers, value shapes, and
  cadence hints used during postprocess.
- `build/metadata.json`: counts, sample domain, and resolved dataset window.
- `build/stats.json`: raw or final vector statistics used by coverage, matrix,
  and threshold inspections.
- Tick task outputs: named timestamp grids used by artifact-backed
  `ensure_cadence` transforms. Their output paths are task-defined.

The dependency graph is explicit: tick artifacts feed scaler/vector inputs;
vector inputs feed schema and metadata; stats depend on metadata and, in final
mode, schema. Nested tick artifacts are rejected because that dependency is not
yet representable safely.

Build state lives at `artifacts/_system/build/state.json`. An entry is current
only when its config and local-source snapshot, declared output path, file, and
dependency chain are all current. Runtime hydration registers only those current
artifacts; orphaned, missing, stale, and incomplete chains are left unavailable.

Runtime profiles use a flat `artifact_mode` for their prerequisite phase:

- `AUTO`: build missing/stale requirements and reuse current dependencies.
- `FORCE`: rebuild the selected dependency closure.
- `OFF`: do not build; fail if a selected runtime requirement is unavailable.

Before any selected serve or inspect profile runs, Jerry unions their artifact
requirements and prepares that union once. The graph orders internal artifact
jobs; profile `order` remains authoritative for the subsequent runtime actions.
Without a command-line override, selected profiles must resolve to the same
mode. Artifact tasks must be declared; built-in defaults do not create missing
producer tasks implicitly.

Runtime operation tasks may add `requires: [artifact_task_id, ...]` when they
need artifacts beyond the built-in operation requirements. These IDs enter the
same dependency closure and must have declared, active producers. A CLI
`--heartbeat-interval` applies to the shared prerequisite build; heartbeat
values defined on individual profiles apply only when those profiles run.

Build profiles remain explicit roots for `jerry build` and retain their own
`mode`. They are not consulted by `serve` or `inspect`.

---

## Splitting & Serving

`project.split` defines how samples receive labels:

- `mode: hash` – deterministic entity hash using either the group key or a
  specified feature ID.
- `mode: time` – boundary-based slicing using timestamp labels.
- `splits` on a serve profile applies those labels and writes one filesystem
  output per selected label in one pipeline run. Without `splits`, serve emits
  the full stream.
- Scaler filtering supports time splits and hash splits keyed by `group`. A hash
  `feature:<id>` key requires the scaler task to use `split_label: all`.

The split configuration never mutates stored artifacts.

---
