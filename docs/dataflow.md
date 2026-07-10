# Data Flow (YAML Reference Chain)

This page shows how config files link together from workspace selection to final output files.
The goal is to make the reference chain explicit and easy to debug.

## End-to-end Reference Chain

```text
jerry.yaml: default_dataset
  -> datasets.<alias> = <path/to/project.yaml>
    -> project.yaml: paths.sources / paths.ingests / paths.streams / paths.dataset / paths.tasks
      -> sources/*.yaml: id
        -> ingests/*.yaml: from.source: <sources.id>, id: <stream_id>
          -> streams/*.yaml: from.stream|from.join|from.streams, id: <stream_id>
          -> dataset.yaml: record_stream: <streams.id>, field: <record_field>
            -> jerry serve
              -> runs/<run_id>/dataset/<split>.jsonl|csv|...
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

## 2) Project maps to config folders/files

`project.yaml` is the root map for all dataset config.

```yaml
paths:
  ingests: ./ingests
  sources: ./sources
  streams: ./streams
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  tasks: ./tasks
  artifacts: ../artifacts/${project_name}/v${version}
```

Expected behavior:
- All relative `paths.*` values are resolved relative to this `project.yaml`.

## 3) Source id links source YAML to ingest

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
- Ingest `from.source: sandbox.ohlcv` resolves to this source spec.
- For fs loaders, relative `args.path` is normalized via runtime path policy.

## 4) Ingest or stream id links canonical records to dataset

Ingests define source-backed stream ids.

```yaml
# ingests/equity.ohlcv.yaml
id: equity.ohlcv
from:
  source: sandbox.ohlcv
map:
  entrypoint: map_sandbox_ohlcv_dto_to_equity
```

Expected behavior:
- `from.source` must match a `sources/*.yaml:id`.
- `id` is what `dataset.yaml` references under `record_stream`.

Derived streams consume existing stream ids:

```yaml
# streams/equity.daily_liquid.yaml
id: equity.daily_liquid
from:
  stream: equity.ohlcv
partition_by: ticker
feature_id_by: []
stream:
  - dedupe: {}
```

## 5) Dataset selects fields from stream ids

Dataset config chooses which streams become features/targets and which record field is used as value.

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
- `record_stream` must match a stream `id`.
- `field` must exist on emitted records.

## 6) Serve writes run-scoped outputs

Run command:

```bash
jerry serve --output-transport fs --output-format jsonl --output-directory vectors
```

Output layout:

```text
vectors/
  runs/<run_id>/
    dataset/
      test.jsonl
      train.jsonl
      val.jsonl
```

Expected behavior:
- Relative output directory resolves from workspace root.
- Output format extension follows `--output-format` or configured format.

## Quick Debug Checklist

1. Dataset not found:
- Verify `jerry.yaml` `default_dataset` and `datasets.<alias>`.

2. Unknown stream/source ids:
- Verify `ingests/*.yaml:from.source` matches `sources/*.yaml:id`.
- Verify `dataset.yaml:record_stream` matches an id in `ingests/` or `streams/`.

3. Empty output:
- Check source loader `path/url`.
- Check parser/mapper output and preview indices (`jerry serve --preview-index 0..14`).

4. Wrong output location:
- Check workspace root and `--output-directory` value.
