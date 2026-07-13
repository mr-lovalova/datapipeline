# Feature Transforms

Feature processing runs after records are wrapped as feature records and before
vector assembly. `dataset.yaml` exposes two explicit stages: `scale` and
`sequence`. Scaling always runs before sequence construction.

## Built-In Transforms

- `scale`: standardize scalar feature values using the managed
  `build/scaler.json` artifact.
- `sequence`: emit sliding windows as `FeatureRecordSequence` payloads.

```yaml
features:
  - id: temperature
    record_stream: weather.hourly
    field: temp_c
    scale: true

  - id: monthly_return
    record_stream: equity.monthly_returns
    field: return_1m
    sequence:
      size: 12
      stride: 1
```

`scale` is a boolean. `with_mean`, `with_std`, and `epsilon` are configured once
on the scaler build task and recorded in the managed artifact; individual
features cannot override them. A `None` value remains `None`. Other nonnumeric
or non-finite values are rejected.

`sequence` accepts strictly positive integer `size` and optional `stride`
(default `1`). It creates independent windows per feature ID and entity from
the source stream's record order. Cadence regularization belongs in that
stream's `stream:` transforms when it is required.
