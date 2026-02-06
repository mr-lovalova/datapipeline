# Transform & Filter Library

### Record Filters (`<project_root>/contracts/*.yaml:record`)

- Binary comparisons: `eq`, `ne`, `lt`, `le`, `gt`, `ge` (timezone-aware for ISO
  or datetime literals).
- Membership: `in`, `nin`.
  ```yaml
  - filter: { field: time, operator: ge, comparand: "${start_time}" }
  - filter: { field: station, operator: in, comparand: [a, b, c] }
  ```

### Record Transforms

- `floor_time`: snap timestamps down to the nearest cadence (`10m`, `1h`, …).
- `lag`: shift record timestamps backward (see `src/datapipeline/transforms/record/lag.py`).

### Stream Transforms

- **Shared interface**: field-writing stream transforms accept `field` and
  `to` keyword arguments (`to` defaults to `field`). Put these first in YAML
  for clarity.
  ```yaml
  - rolling: { field: dollar_volume, to: adv5, window: 5, statistic: mean }
  ```
  Class signature shape:
  ```python
  class MyTransform(FieldStreamTransformBase):
      def __init__(self, field: str, to: str | None = None, **params) -> None:
          super().__init__(field=field, to=to)
          ...
  ```
  ABCs live in `src/datapipeline/transforms/interfaces.py`.

- `ensure_cadence`: backfill missing ticks with `field=None` records to enforce a
  strict cadence. Supports `field`/`to`.
- `granularity`: merge duplicate timestamps using `first|last|mean|median`.
  Supports `field`/`to`.
- `dedupe`: drop exact duplicate records (same timestamp + payload) from an
  already sorted stream.
- `fill`: rolling statistic-based imputation within each partition stream.
  Supports `field`/`to`.
- `rolling`: rolling statistic over the selected `field`, writing to `to`.
- Custom transforms can be registered under the `datapipeline.transforms.stream`
  entry-point group.

### Feature Transforms

- `scale`: wraps `StandardScalerTransform`. Read statistics from the build
  artifact or accept inline `statistics`.
  ```yaml
  scale:
    with_mean: true
    with_std: true
    statistics:
      temp_c__station=001: { mean: 10.3, std: 2.1 }
  ```

### Sequence Transforms

- `sequence`: sliding window generator (`size`, `stride`, optional `cadence` to
  enforce contiguous windows). Emits `FeatureRecordSequence` payloads with
  `.records` and `.values`.

### Vector (Postprocess) Transforms

- `drop`: apply coverage thresholds along the horizontal axis (vectors) or
  vertical axis (features/partitions) using `axis: horizontal|vertical` and
  `threshold`. Vertical mode requires the optional `metadata.json`
  artifact and internally prunes weak partitions.
- `fill`: impute using rolling statistics from prior vectors (history-based).
- `replace`: seed missing IDs with a constant or literal value.
  (Jerry automatically enforces the `schema.json` vector schema—ordering +
  cadence—before any configured vector transforms run.)

All transforms share a consistent entry-point signature and accept their config
dict as keyword arguments. Register new ones in `pyproject.toml` under the
appropriate group (`datapipeline.transforms.record`, `.stream`, `.feature`,
`.vector`, `.debug`, and `datapipeline.filters`).

---
