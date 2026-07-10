# Artifacts

Artifact tasks are declared under `tasks/operations/`; built-in task models use
these default outputs:

- `build/vector_inputs/manifest.json` plus compressed feature/target shards:
  durable inputs consumed by vector assembly.
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

Build modes are:

- `AUTO`: build missing/stale requirements and reuse current dependencies.
- `FORCE`: rebuild the selected dependency closure.
- `OFF`: do not build; fail if a selected runtime requirement is unavailable.

`--skip-build` likewise skips execution but still verifies that runtime
requirements are current. Artifact tasks must be declared; built-in defaults do
not create missing producer tasks implicitly.

---

## Splitting & Serving

If `project.split` is present, `jerry serve` filters vectors at the
end of the pipeline:

- `mode: hash` – deterministic entity hash using either the group key or a
  specified feature ID.
- `mode: time` – boundary-based slicing using timestamp labels.
- `splits` on a serve profile writes one filesystem output per selected split
  label in one pipeline run.
- `run.keep` (or CLI `--keep`) remains available for legacy single-output split
  filtering; use any label name defined in your split config.

The split configuration never mutates stored artifacts; it is only applied when
serving vectors (either via CLI or the Python integrations).

---
