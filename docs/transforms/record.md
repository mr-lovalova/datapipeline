# Record Transforms

Record transforms run on mapped domain records before ordering. Configure them
in `ingests/*.yaml` under `record:`.

## `where`

- Binary comparisons: `eq`, `ne`, `lt`, `le`, `gt`, `ge`.
- Membership: `in`, `not_in`.
- ISO or datetime literals are compared with timezone awareness.

```yaml
record:
  - { operation: where, field: time, operator: ge, comparand: "${start_time}" }
  - { operation: where, field: station, operator: in, comparand: [a, b, c] }
```

## Built-In Transforms

- `floor_time`: snap timestamps down to a cadence (`10m`, `1h`, `1d`).
- `shift_time`: shift timestamps by a duration (`by: 1d`, `by: -1h`).
- `where`: filter records with the operator language above.
