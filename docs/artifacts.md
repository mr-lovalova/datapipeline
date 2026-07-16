# Artifacts

Core artifact operations are registered by Jerry and use these default outputs.
Projects declare YAML only for overrides and custom operations:

An artifact's registry key is its operation ID (`scaler`, `vector_inputs`,
`schema`, `metadata`, or `stats`). Custom operation IDs come from their YAML
filenames, such as `operations/model_grid.yaml`.

- `build/vector_inputs/manifest.json` plus compressed feature/target shards under
  `build/vector_inputs/manifest.shards/`:
  durable inputs consumed by vector assembly. Values may contain only `None`,
  `bool`, `int`, `float`, `str`, lists, and string-keyed dictionaries;
  sample-key components may use only those scalar types. Other Python objects
  fail the build instead of being converted to strings. Each successful build
  publishes a new immutable shard generation. Project commands hold one
  artifact-workspace lock; generations no longer referenced by the manifest
  are pruned only after the locked command finishes.
- `build/scaler.json`: managed scaler statistics. Unsplit datasets store one
  standard scaler; split datasets store one scaler fitted from each fold's
  training labels.
- `build/schema.json`: discovered feature/target identifiers, value shapes, and
  cadence hints used during postprocess.
- `build/metadata.json`: counts, sample domain, and resolved dataset window.
- `build/stats.json`: bounded assembled or postprocessed availability counters
  used by coverage inspection. It never stores per-sample status maps.
- Tick operation outputs: named timestamp grids used by `ensure_ticks`
  transforms. Their output paths are operation-defined.

The dependency graph is explicit: tick artifacts referenced by configured
dataset streams feed scaler/vector inputs;
vector inputs feed metadata; schema is derived from metadata without rescanning
the vectors; stats depend on metadata and, in postprocessed stage, schema.
Matrix inspection reads vector inputs directly and enforces a configured cell
bound instead of expanding the stats artifact. Nested tick
artifacts are rejected because that dependency is not yet representable safely.

Coverage treats null values as uncovered. Base and scalar-column coverage is
the number of non-null samples divided by total samples. List-column coverage
is observed non-null elements divided by `total_samples * sequence_length`, so
absent samples and null list elements share the same explicit denominator.

Build state lives at `artifacts/_system/build/state.json`. An entry is current
only when its own artifact hash, declared output path, primary file,
companion-file fingerprints, and dependency chain are all current. Each core
artifact hash covers its typed configuration and dependency closure, including
only streams and local source snapshots that can feed that artifact. Unrelated
operations, streams, and sources therefore do not invalidate it. Every hash
includes `project.yaml:artifact_revision`; runtime-operation settings do not
enter it. Plugin artifact operations receive the full runtime but cannot declare
inputs, so their hashes conservatively cover the complete dataset and stream
catalog. Runtime hydration registers only current artifacts;
orphaned, missing, altered, stale, and incomplete chains are left unavailable.
Commands targeting the same artifacts root cannot overlap; a second command
fails before reading or mutating managed artifacts.

Serve, inspect, and materialize use one command-wide `artifact_mode` for their
prerequisite phase. Its precedence is CLI `--artifact-mode`, then the matching
`<command>.defaults.yaml`, then the built-in `AUTO`:

- `AUTO`: build missing/stale requirements and reuse current dependencies.
- `FORCE`: rebuild the selected dependency closure.
- `OFF`: do not build; fail if a selected runtime requirement is unavailable.

Before any selected serve or inspect profile runs, Jerry unions their artifact
requirements and prepares that union once. The graph orders internal artifact
jobs; profile `order` remains authoritative for the subsequent runtime actions.
Concrete profiles do not carry individual artifact modes. Custom artifact
dependencies must have a configured producer; core artifact producers are
always available.

Before any selected materialize profile runs, Jerry similarly unions the
artifact requirements of its selected streams and prepares them once.
For built-in transforms, these are tick IDs referenced by `ensure_ticks`
operations on the selected or upstream streams. Dependencies
hidden inside plugin code are not inferred. Materialize profiles do not carry
individual artifact modes.

The shared prerequisite phase has its own visual and logging envelope. Its
observability precedence is CLI, then `<command>.defaults.yaml`, then built-ins;
settings on a concrete profile apply only while that profile runs. A bare
execution-scoped log target writes the shared phase to
`logs/<command>.artifacts.log`.

Runtime operations may add `requires: [artifact_operation_id, ...]` when they
need artifacts beyond the built-in operation requirements. These IDs enter the
same dependency closure and must have declared, active producers. A CLI or
command-default heartbeat applies to the shared prerequisite build; heartbeat
values defined on individual profiles apply only when those profiles run.

Build profiles remain explicit roots for `jerry build` and retain their own
`mode`. They are not consulted by `serve`, `inspect`, or `materialize`.

---

## Splitting & Serving

`dataset.yaml:split` has two explicit responsibilities:

- `mode: hash` assigns one deterministic label from the complete sample key.
- `mode: time` assigns one interval ID from ordered, exclusive `until` timestamps.
- `folds` groups hash labels or time-interval IDs into named train, validation,
  and test outputs. Entries may be reused across folds, which supports expanding
  walk-forward training windows. Entries omitted from every fold are
  purge/embargo intervals and are not published.
- Output IDs are `<fold-id>.<role>`, such as `fold_1.train`. A full serve
  publishes every configured fold output; profile `include_outputs` can narrow
  that set. Without a dataset split, serve emits one combined stream.
- Preview bypasses split fanout and emits one combined stage output.
- Split datasets fit one scaler from each fold's `train` labels. Every output in
  a fold uses that fold's scaler.
- Hash splits cannot be combined with sequence features because overlapping
  windows could share observations across hash partitions. Use a time split for
  sequence datasets.

Canonical vector-input artifacts remain unscaled and independent of fold
selection. The scaler artifact records the fold-specific statistics needed at
runtime.

---
