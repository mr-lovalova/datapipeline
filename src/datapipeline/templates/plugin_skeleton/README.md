# {{PACKAGE_NAME}}

Minimal plugin skeleton for the Jerry Thomas (datapipeline) framework.

Quick start
- Initialize a plugin (already done if you’re reading this here):
  - `jerry station init --name {{PACKAGE_NAME}}`
- Add a source via CLI (transport-specific placeholders are scaffolded):
  - File data: `jerry distillery add -p <provider> -d <dataset> -t fs -f <csv|json|json-lines>`
  - URL data: `jerry distillery add -p <provider> -d <dataset> -t url -f <json|json-lines|csv>`
  - Synthetic: `jerry distillery add -p <provider> -d <dataset> -t synthetic`
- Edit the generated `config/sources/*.yaml` to fill in the `path`, delimiter, etc.
- Reinstall after EP changes (pyproject.toml) and restart Python processes:
  - Core: `cd lib/datapipeline && python -m pip install -e .`
  - This plugin: `python -m pip install -e .`

Folder layout
- `config/`
  - `project.yaml` — paths for sources/streams
  - `sources/*.yaml` — raw source definitions (one file per source)
  - `streams/*.yaml` — canonical stream definitions
- `src/{{PACKAGE_NAME}}/`
  - `sources/<provider>/<dataset>/dto.py` — DTO model for the source
  - `sources/<provider>/<dataset>/parser.py` — parse raw → DTO
  - Optional: `sources/<provider>/<dataset>/loader.py` for synthetic sources
  - `domains/<domain>/model.py` — domain record models
  - `mappers/*.py` — map DTOs → domain records

How loaders work
- For fs/url, sources use the generic loader entry point:
  - `loader.entrypoint: "{{COMPOSED_LOADER_EP}}"`
- `loader.args` include `transport`, `format`, and source-specific args (placeholders are provided):
    - fs: `path`, `glob`, `encoding`, plus `delimiter` for csv
    - url: `url`, `headers`, `encoding`, optional `count_by_fetch`
- Synthetic sources generate data in-process and keep a small loader stub.

Run data flows
- Records: `jerry prep pour -p config/project.yaml -n 100`
- Features: `jerry prep build -p config/project.yaml -n 100`
- Vectors: `jerry prep stir -p config/project.yaml -n 100`

Analyze vectors
- `jerry taste -p config/project.yaml -n 10000`
- Prints missing features per group and overall stats.

Tips
- Keep parsers thin — mirror source schema and return DTOs; use the identity parser only if your loader already emits domain records.
- Prefer small, composable configs over monolithic ones: one YAML per source is easier to review and reuse.
