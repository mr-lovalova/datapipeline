# {{DIST_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) runtime.

## Quick start

```bash
python -m pip install -U jerry-thomas

jerry plugin init {{DIST_NAME}} --out .
python -m pip install -e {{DIST_NAME}}

# One-stop wizard: source YAML + DTO/parser + domain + mapper + ingest.
jerry inflow create

# If a workspace-level `jerry.yaml` was created (fresh workspace), you can use the dataset alias:
jerry serve --dataset your-dataset --limit 3
#
# If you already had a workspace `jerry.yaml`, `jerry plugin init` will not overwrite it.
# In that case, either add a dataset alias to your existing `jerry.yaml` or pass `--project`:
# jerry serve --project your-dataset/project.yaml --limit 3
```

## After scaffolding: what you must edit

- `your-dataset/sources/*.yaml`
  - Replace placeholders (`path`/`url`, headers/params, delimiter, etc.)
  - Prefer `${env:NAME}` for secrets or machine-local paths instead of literal values
- `your-dataset/ingests/*.yaml`
  - Map raw source records into canonical domain records.
- `your-dataset/streams/*.yaml`
  - Add single-input transforms or align multiple streams with a combine function.
- `your-dataset/.env.example`
  - Copy to `.env` next to `project.yaml` for local dataset-specific secrets and paths
- `your-dataset/dataset.yaml`
  - Ensure `record_stream:` points at the stream id you created.
  - Select a `field:` for each feature/target (record attribute to use as value).
  - Ensure `sample.cadence` matches `^\d+(m|min|h|d)$` (e.g. `10m`, `1h`, `1d`). The scaffold fills it from the `${group_by}` project global.

If you add/edit entry points in `pyproject.toml`, reinstall the plugin:

```bash
python -m pip install -e .
```

## Folder layout

YAML config (dataset project root):

- `your-dataset/`
  - `project.yaml` (paths, split, globals)
  - `sources/*.yaml` (raw source definitions)
  - `ingests/*.yaml` (source-backed canonical streams)
  - `streams/*.yaml` (derived and aligned streams)
  - `dataset.yaml` (features/targets)
  - `postprocess.yaml` (column selection and sample filtering)
  - `profiles/{serve,build,inspect,materialize}.<name>.yaml` (profiles; optional overrides)
  - `profiles/{serve,build,inspect,materialize}.defaults.yaml` (optional per-kind defaults)
  - `tasks/operations/*.yaml` (declared artifact and runtime operations)

Profile sequencing:

- Profiles execute by `order` (ascending); unset falls back to filename order.
- Materialize profiles name a stream and an exact durable JSONL output; they do
  not target runtime tasks.
- Build profiles target artifact tasks; serve and inspect profiles target runtime
  tasks.
- Before selected serve or inspect profiles run, their artifact requirements are
  combined and prepared once according to `artifact_mode: AUTO|FORCE|OFF`.
- The dependency graph orders only internal artifact jobs. It never changes
  profile order; build profiles remain explicit artifact roots.
- Selected build profiles must have distinct targets.
- A selected dependency build profile must precede a selected dependent profile.
- Use multiple ordered profiles only when you want separate named build/runtime
  steps or different per-profile output and observability settings.

Python plugin code:

- `src/{{PACKAGE_NAME}}/`
  - `dtos/` (DTO models)
  - `parsers/` (raw -> DTO)
  - `domains/<domain>/model.py` (domain record models)
  - `mappers/` (iterator mappers and aligned combine functions)
  - `loaders/` (optional custom loaders)

## Learn more

- Preview stages and split/build timing: the Jerry Thomas runtime `README.md` ("Preview stages (serve --preview)").
- Deep dives: runtime `docs/config.md`, `docs/transforms/`, `docs/artifacts.md`, `docs/extending.md`, `docs/architecture.md`.
