# {{PACKAGE_NAME}}

Minimal plugin skeleton for the DataPipeline framework.

Quick start
- Initialize a plugin (already done if you’re reading this here):
  - `datapipeline plugin init --name {{PACKAGE_NAME}}`
- Add a source via CLI (transport + format):
  - File CSV: `datapipeline source create -p <provider> -d <dataset> -t fs -f csv`
  - File NDJSON: `datapipeline source create -p <provider> -d <dataset> -t fs -f json-lines`
  - URL JSON: `datapipeline source create -p <provider> -d <dataset> -t url -f json`
  - Synthetic: `datapipeline source create -p <provider> -d <dataset> -t synthetic`
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
  - `loader.args` include `transport`, `format`, and source-specific args:
    - fs: `path`, `glob`, `encoding`, plus `delimiter` for csv
    - url: `url`, `headers`, `encoding`, optional `count_by_fetch`
- Synthetic sources generate data in-process and keep a small loader stub.

Run data flows
- Records: `dp run -p config/project.yaml -s records -n 100`
- Features: `dp run -p config/project.yaml -s features -n 100`
- Vectors: `dp run -p config/project.yaml -s vectors -n 100`
- Only specific streams: add `--only <stream1> <stream2>`

Analyze vectors
- `dp analyze -p config/project.yaml -n 10000`
- Prints missing features per group and overall stats.

Tips
- Keep parsers thin — mirror source schema and return DTOs; use the identity parser only if your loader already emits domain records.
- Prefer small, composable configs over monolithic ones: one YAML per source is easier to review and reuse.
