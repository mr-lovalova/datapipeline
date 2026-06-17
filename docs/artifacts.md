# Artifacts

- `build/schema.json`: output of the `schema` task. Jerry automatically
  enforces this schema during postprocess to impose deterministic ordering and
  list cadence metadata (targets appear whenever the dataset defines them). Window metadata now lives in `build/metadata.json`.
- `build/scaler.json`: JSON scaler statistics fitted on the configured split.
  This can be one standard scaler or a folded temporal scaler container. Loaded
  lazily by feature transforms at runtime.
- Build state is tracked in `artifacts/_system/build/state.json`; config hashes avoid
  redundant runs.

If runtime processing needs an artifact and it is missing, Jerry raises a
descriptive error suggesting `jerry build`.

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
