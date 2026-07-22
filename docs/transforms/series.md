# Series Shaping

`dataset.yaml` exposes two distinct policies. `sequence` shapes series
before sample assembly. `scale` marks assembled vector values for output
scaling with the selected dataset fold.

## Built-In Policies

- `sequence`: emit sliding windows as `SeriesSequence` payloads.
- `scale`: standardize a feature or target with the scaler fitted from the selected
  dataset fold's training labels.

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

`scale` is a boolean. It does not alter canonical series artifacts or
preview output. During a full dataset serve, every scalar or sequence value in
one fold output is scaled with that fold's scaler. `with_mean`, `with_std`, and
`epsilon` are configured once on the scaler build operation and recorded in the
managed artifact; individual features cannot override them. `None` is the
canonical missing value. A transient floating `NaN` is converted to `None`
when the field is projected; other nonnumeric values and infinity are rejected.

`sequence` accepts strictly positive integer `size` and optional `stride`
(default `1`). It creates independent windows per series ID and entity from
the source stream's record order. The stream's complete `partition_by` identity
keeps each series contiguous and sequence memory bounded to one window. Dataset
`sample.keys` select the partition fields represented in each row; remaining
partition fields are appended to the series ID. Cadence regularization belongs
in that stream's `transforms:` when it is required.
