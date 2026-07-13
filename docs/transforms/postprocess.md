# Sample Postprocessing

Postprocessing runs after vector assembly. Configure its structural policies in
`postprocess.yaml`:

```yaml
columns:
  features:
    threshold: 0.8
    ids: [price, volume]       # optional; defaults to every feature
  targets:
    threshold: 0.9
    ids: [return]

samples:
  features:
    threshold: 0.5
  targets:
    threshold: 1.0
    ids: [return]
```

The pipeline has one fixed order:

1. assemble feature and target vectors;
2. select feature and target columns;
3. normalize vectors against the retained typed schema;
4. filter sample rows by feature coverage, then target coverage.

Feature and target policies are separate. Row filtering is also separate from
column selection:

- `DropSamplesTransform` and `DropTargetSamplesTransform` filter complete sample
  rows by the coverage of selected cells.
- `SelectFeaturesTransform` and `SelectTargetsTransform` remove columns using
  precomputed `ColumnCoverage` entries. The resulting `retained_ids` is the
  effective schema for downstream validation.

Selection and sample filters accept an optional `ids` subset and default to
every retained ID. Empty, duplicate, or unknown IDs are errors.

Postprocessing does not mutate vector values. Repair missing record values with
an ordered stream `fill` or `forward_fill` operation before feature extraction.
This keeps history and partition semantics at the stage where they are known.

## Coverage metadata

`ColumnCoverage` makes the denominator explicit:

- scalar coverage is `(present_count - null_count) / total_opportunities`;
- list coverage is `observed_elements / (total_opportunities * sequence_length)`.

List coverage therefore includes absent sample buckets instead of measuring only
buckets where the list happened to appear. The pipeline uses the feature or
target vector count from `build/metadata.json`, matching the population used to
collect `present_count`, `null_count`, and `observed_elements`.

## Schema normalization

`NormalizeFeaturesTransform` and `NormalizeTargetsTransform` receive typed
`VectorSchemaEntry` objects directly. They fill wholly absent schema entries,
order values by the schema, and reject unexpected IDs, invalid numeric values,
and incorrect list lengths. They do not truncate values, silently discard extra
columns, or mutate the pipeline context.
