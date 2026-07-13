# Feature Transforms

Feature transforms run after records are wrapped as feature records and before
vector assembly. `dataset.yaml` exposes two explicit feature-stage options:
`scale` and `sequence`. It does not accept an arbitrary transform clause list.

## Built-In Transforms

- `scale`: standardize feature values using `build/scaler.json` or inline
  statistics.
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

`sequence` accepts `size` and optional `stride` (default `1`). It creates
windows from the ordered feature stream; cadence regularization belongs in the
source stream's `stream:` transforms when it is required.
