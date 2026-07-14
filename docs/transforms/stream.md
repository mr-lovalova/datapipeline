# Stream Transforms

Stream transforms run on ordered records. Configure them in `streams/*.yaml`
under `stream:`.

Transforms that depend on history operate within a partition. Set
`partition_by` to the complete identity of an independent series, such as
`[security_id]` or `[security_id, metric]`; single-input streams inherit it
unless they provide an explicit replacement. Dataset `sample.keys` select which
partition fields identify output rows. Remaining partition fields suffix feature
IDs in their declared order.

## Field-Writing Transforms

Field-writing transforms accept `field` and optional `to`. If `to` is omitted,
the transform writes back to `field`. They cannot write `time` or a resolved
`partition_by` field because those fields define canonical record order.
Identity changes belong in a map or combine function, before the ordering stage.

```yaml
stream:
  - { operation: rolling, field: dollar_volume, to: adv20, window: 20, statistic: mean }
```

## Built-In Transforms

- `ensure_cadence`: insert placeholder ticks at a fixed duration within each
  partition.
- `ensure_ticks`: reindex records against a resolved tick-grid artifact.
- `where`: filter ordered records using the record `where` operator language.
- `lag` / `lead`: copy a prior or future field value into `to` by `periods`
  within each partition.
- `derive`: write `to` from binary arithmetic on `left` and exactly one of
  `right_field` or `right_value`. Operators: `add`, `sub`, `mul`, `div`.
- `collapse`: keep the `first` or `last` adjacent record for each partition and
  timestamp.
- `dedupe`: drop exact duplicate records from an already sorted stream.
- `fill`: impute missing values from rolling history using an explicit `mean`
  or `median` statistic.
- `forward_fill`: carry the last known value within each partition.
- `rolling`: compute `mean`, `median`, `stdev`, `pstdev`, `max`, or `min` over
  a rolling window. Missing ticks occupy a window position but do not count
  toward `min_samples`, which defaults to `window`. Values must be finite;
  `None` and `NaN` are treated as missing.

```yaml
stream:
  - { operation: lag, field: close, to: close_lag_21, periods: 21 }
  - { operation: lag, field: close, to: close_lag_189, periods: 189 }
  - { operation: derive, left: close_lag_21, operator: div, right_field: close_lag_189, to: close_ratio }
  - { operation: derive, left: close_ratio, operator: sub, right_value: 1, to: momentum_189_21 }
```

```yaml
stream:
  - { operation: ensure_ticks, artifact: model_grid }
  - { operation: forward_fill, field: gross_margin }
```
