# Feature Transforms

Feature transforms run after records are wrapped as feature records and before
vector assembly. Configure them on each feature or target in `dataset.yaml`.

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

`sequence` accepts `size`, `stride`, and optional `cadence` when windows must be
contiguous.
