# Transforms

Transforms are split by the stage where they run:

- [Record transforms](record.md): one record at a time, before ordering.
- [Stream transforms](stream.md): ordered record streams, usually with
  per-partition history.
- [Feature transforms](feature.md): feature payload shaping before vector
  assembly.
- [Postprocess policies](postprocess.md): column selection and sample filtering
  before split and output persistence.

Record and stream transforms are explicit built-in operations. Their config is
validated before pipeline execution; arbitrary transform entry points are not
loaded at runtime.

## Configuration Shape

Each record or stream transform is one flat mapping. `operation` identifies the
built-in operation and its configuration fields are siblings:

```yaml
stream:
  - operation: dedupe
  - operation: rolling
    field: close
    to: close_mean_20
    window: 20
    statistic: mean
```

Missing or unknown operations, unknown fields, and invalid field values are
rejected while loading the project. Postprocess instead has a fixed structural
shape separating feature selection, target selection, and final sample filters.
See [Postprocess policies](postprocess.md) for its complete shape.
