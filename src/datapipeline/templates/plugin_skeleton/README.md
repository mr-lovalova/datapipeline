# {{DIST_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) runtime.

## Quick start

```bash
python -m pip install -U jerry-thomas

jerry plugin init {{DIST_NAME}} --out .
python -m pip install -e {{DIST_NAME}}

# One-stop wizard: source YAML + DTO/parser + domain + mapper + contract.
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
- `your-dataset/dataset.yaml`
  - Ensure `record_stream:` points at the contract id you created.
  - Select a `field:` for each feature/target (record attribute to use as value).
  - Ensure `group_by` matches `^\d+(m|min|h|d)$` (e.g. `10m`, `1h`, `1d`).

If you add/edit entry points in `pyproject.toml`, reinstall the plugin:

```bash
python -m pip install -e .
```

## Folder layout

YAML config (dataset project root):

- `your-dataset/`
  - `project.yaml` (paths, globals, split)
  - `sources/*.yaml` (raw source definitions)
  - `contracts/*.yaml` (canonical streams)
  - `dataset.yaml` (features/targets)
  - `postprocess.yaml` (vector-level transforms)
  - `tasks/*.yaml` (serve/build tasks; optional overrides)

Python plugin code:

- `src/{{PACKAGE_NAME}}/`
  - `dtos/` (DTO models)
  - `parsers/` (raw -> DTO)
  - `domains/<domain>/model.py` (domain record models)
  - `mappers/` (DTO -> domain records)
  - `loaders/` (optional custom loaders)

## Learn more

- Pipeline stages and split/build timing: the Jerry Thomas runtime `README.md` ("Pipeline Stages (serve --stage)").
- Deep dives: runtime `docs/config.md`, `docs/transforms.md`, `docs/artifacts.md`, `docs/extending.md`, `docs/architecture.md`.
