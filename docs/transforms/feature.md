# Feature Transforms

Feature processing runs after records are wrapped as feature records and before
vector assembly. `dataset.yaml` exposes two explicit stages: `scale` and
`sequence`. Scaling always runs before sequence construction.

## Built-In Transforms

- `scale`: standardize scalar feature values using the managed
  `build/scaler.json` artifact.
- `sequence`: emit sliding windows as `FeatureSequence` payloads.

```yaml
features:
  - id: temperature
    stream: weather.hourly
    field: temp_c
    scale: true

  - id: monthly_return
    stream: equity.monthly_returns
    field: return_1m
    sequence:
      size: 12
      stride: 1
```

`scale` is a boolean. `with_mean`, `with_std`, and `epsilon` are configured once
on the scaler build operation and recorded in the managed artifact; individual
features cannot override them. A `None` value remains `None`. Other nonnumeric
or non-finite values are rejected.

`sequence` accepts strictly positive integer `size` and optional `stride`
(default `1`). It creates independent windows per feature ID and entity from
the source stream's record order. The stream's complete `partition_by` identity
keeps each series contiguous and sequence memory bounded to one window. Dataset
`sample.keys` select the partition fields represented in each row; remaining
partition fields are appended to the feature ID. Cadence regularization belongs
in that stream's `stream:` transforms when it is required.
