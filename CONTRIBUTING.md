# Contributing

This repo ships the Jerry Thomas runtime (`jerry-thomas`) and its CLI (`jerry`).

## Development Setup

```bash
python -m pip install -e .[dev]
pytest
```

When iterating on configs and transforms, `jerry serve --stage <n>` is the fastest
way to validate pipeline behavior.

## Scaffolding Internals (Internal API)

The CLI scaffolding commands are implemented under `src/datapipeline/services/scaffold/`.
These functions are considered *internal* (they may change without a deprecation
policy); prefer using the CLI unless you are working on the runtime itself.

Key entrypoints:

- Plugin/demo
  - `datapipeline.services.scaffold.plugin.scaffold_plugin` (used by `jerry plugin init`)
  - `datapipeline.services.scaffold.demo.scaffold_demo` (used by `jerry demo init`)

- YAML config
  - `datapipeline.services.scaffold.source_yaml.create_source_yaml` (used by `jerry source create` and `jerry inflow create`)
  - `datapipeline.services.scaffold.contract_yaml.write_ingest_contract` / `write_composed_contract` (used by `jerry contract create`)

- Python stubs + entry points
  - `datapipeline.services.scaffold.dto.create_dto` (used by `jerry dto create`)
  - `datapipeline.services.scaffold.parser.create_parser` (used by `jerry parser create`)
  - `datapipeline.services.scaffold.mapper.create_mapper` (used by `jerry mapper create`)
  - `datapipeline.services.scaffold.loader.create_loader` (used by `jerry loader create`)
  - `datapipeline.services.scaffold.domain.create_domain` (used by `jerry domain create`)
  - `datapipeline.services.scaffold.filter.create_filter` (used by `jerry filter create`)

- Composition (wizard)
  - `datapipeline.services.scaffold.stream_plan.execute_stream_plan` (used by `jerry inflow create`)

Entry points are injected/updated in plugin `pyproject.toml` via:

- `datapipeline.services.entrypoints.inject_ep`

