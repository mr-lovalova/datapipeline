# Data Flow (YAML Reference Chain)

This page shows how config files link together from workspace selection to final output files.
The goal is to make the reference chain explicit and easy to debug.

## End-to-end Reference Chain

```text
jerry.yaml: default_dataset
  -> datasets.<alias> = <path/to/project.yaml>
    -> project.yaml: paths.sources / paths.streams / paths.dataset / paths.tasks
      -> sources/*.yaml: id
        -> contracts/*.yaml: source: <sources.id>, id: <stream_id>
          -> dataset.yaml: record_stream: <contracts.id>, field: <record_field>
            -> jerry serve
              -> runs/<run_id>/dataset/<split>.jsonl|json|csv|...
```

## 1) Workspace selects dataset project

`jerry.yaml` picks which `project.yaml` to run when `--dataset`/`--project` is omitted.

```yaml
plugin_root: demo
datasets:
  demo: demo/demo/project.yaml
default_dataset: demo
```

Expected behavior:
- `jerry serve` resolves to `datasets.demo`.
- Relative paths here are resolved from the workspace root (directory containing `jerry.yaml`).

Screenshot slot:
`docs/assets/dataflow-01-workspace.png`

## 2) Project maps to config folders/files

`project.yaml` is the root map for all dataset config.

```yaml
paths:
  sources: ./sources
  streams: ./contracts
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  tasks: ./tasks
  artifacts: ../artifacts/${project_name}/v${version}
```

Expected behavior:
- All relative `paths.*` values are resolved relative to this `project.yaml`.

Screenshot slot:
`docs/assets/dataflow-02-project-paths.png`

## 3) Source id links source YAML to contract

A source file declares the raw source id plus loader/parser wiring.

```yaml
# sources/sandbox.ohlcv.yaml
id: "sandbox.ohlcv"
parser:
  entrypoint: "sandbox_ohlcv_dto_parser"
loader:
  entrypoint: "core.io"
  args:
    transport: fs
    format: jsonl
    path: data/*.jsonl
    glob: true
```

Expected behavior:
- Contract `source: sandbox.ohlcv` resolves to this source spec.
- For fs loaders, relative `args.path` is normalized via runtime path policy.

Screenshot slot:
`docs/assets/dataflow-03-source-yaml.png`

## 4) Contract id links canonical stream to dataset

Contracts define canonical stream ids and source mapping.

```yaml
# contracts/equity.ohlcv.yaml
kind: ingest
id: equity.ohlcv
source: sandbox.ohlcv
mapper:
  entrypoint: map_sandbox_ohlcv_dto_to_equity
```

Expected behavior:
- `source` must match a `sources/*.yaml:id`.
- `id` is what `dataset.yaml` references under `record_stream`.

Screenshot slot:
`docs/assets/dataflow-04-contract-yaml.png`

## 5) Dataset selects fields from stream ids

Dataset config chooses which contract streams become features/targets and which record field is used as value.

```yaml
group_by: ${group_by}
features:
  - id: closing_price
    record_stream: equity.ohlcv
    field: close
  - id: opening_price
    record_stream: equity.ohlcv
    field: open
```

Expected behavior:
- `record_stream` must match a contract `id`.
- `field` must exist on emitted records.

Screenshot slot:
`docs/assets/dataflow-05-dataset-yaml.png`

## 6) Serve writes run-scoped outputs

Run command:

```bash
jerry serve --output-transport fs --output-format json --output-directory vectors
```

Output layout:

```text
vectors/
  runs/<run_id>/
    dataset/
      test.json
      train.json
      val.json
```

Expected behavior:
- Relative output directory resolves from workspace root.
- Output format extension follows `--output-format` or configured format.

Screenshot slot:
`docs/assets/dataflow-06-run-output.png`

## Quick Debug Checklist

1. Dataset not found:
- Verify `jerry.yaml` `default_dataset` and `datasets.<alias>`.

2. Unknown stream/source ids:
- Verify `contracts/*.yaml:source` matches `sources/*.yaml:id`.
- Verify `dataset.yaml:record_stream` matches `contracts/*.yaml:id`.

3. Empty output:
- Check source loader `path/url`.
- Check parser/mapper output and stage previews (`jerry serve --stage 0..8`).

4. Wrong output location:
- Check workspace root and `--output-directory` value.
