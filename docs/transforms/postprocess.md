# Postprocess Transforms

Postprocess runs after vector assembly and before split/output persistence.
Configure it in `postprocess.yaml`.

Jerry first enforces `build/schema.json` when available. That makes vector
ordering deterministic, fills missing configured feature IDs, drops unexpected
IDs unless allowed, and enforces recorded list cadence metadata.

## Built-In Transforms

- `drop`: apply coverage thresholds along the horizontal axis (vectors) or
  vertical axis (features/partitions) using `axis: horizontal|vertical` and
  `threshold`. Vertical mode uses `build/metadata.json` when partition coverage
  needs metadata.
- `fill`: impute missing vector values from prior vectors using rolling
  history.
- `replace`: seed missing IDs with a constant or literal value.

## Artifacts

- `build/schema.json` is produced by the schema task and used automatically
  before configured postprocess transforms.
- `build/metadata.json` is produced by the metadata task and required by some
  vertical coverage workflows.

If a required artifact is missing, Jerry fails with a message pointing to
`jerry build`.
