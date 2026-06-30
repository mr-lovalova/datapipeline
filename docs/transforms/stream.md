# Stream Transforms

Stream transforms run on ordered records. Configure them in `streams/*.yaml`
under `stream:`.

Transforms that depend on history operate within a partition. Set
`partition_by` on the stream when state must be isolated by an entity such as
`security_id`. `partition_by` does not control feature names; use
`feature_id_by` when a stream should suffix feature ids.

## Field-Writing Transforms

Field-writing transforms accept `field` and optional `to`. If `to` is omitted,
the transform writes back to `field`.

```yaml
stream:
  - rolling: { field: dollar_volume, to: adv20, window: 20, statistic: mean }
```

## Built-In Transforms

- `ensure_cadence`: insert placeholder ticks so timestamps are exactly one
  cadence apart per partition.
- `floor_time`: snap timestamps down to a cadence.
- `where`: filter ordered records using the record `where` operator language.
- `lag` / `lead`: copy a prior or future field value into `to` by `periods`
  within each partition.
- `derive`: write `to` from binary arithmetic on `left` and exactly one of
  `right_field` or `right_value`. Operators: `add`, `sub`, `mul`, `div`.
- `granularity`: merge duplicate timestamps using `first`, `last`, `mean`, or
  `median`.
- `dedupe`: drop exact duplicate records from an already sorted stream.
- `fill`: impute missing values from rolling history using `mean` or `median`.
- `rolling`: compute `mean`, `median`, `stdev`, `pstdev`, `max`, or `min` over
  a rolling window.

```yaml
stream:
  - lag: { field: close, to: close_lag_21, periods: 21 }
  - lag: { field: close, to: close_lag_189, periods: 189 }
  - derive: { left: close_lag_21, operator: div, right_field: close_lag_189, to: close_ratio }
  - derive: { left: close_ratio, operator: sub, right_value: 1, to: momentum_189_21 }
```
