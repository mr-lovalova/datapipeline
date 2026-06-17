# Transforms

Transforms are split by the stage where they run:

- [Record transforms](record.md): one record at a time, before ordering.
- [Stream transforms](stream.md): ordered record streams, usually with
  per-partition history.
- [Feature transforms](feature.md): feature payload shaping before vector
  assembly.
- [Postprocess transforms](postprocess.md): assembled vectors before split and
  output persistence.

Register custom transforms in `pyproject.toml` under the matching entry-point
group:

- `datapipeline.transforms.record`
- `datapipeline.transforms.stream`
- `datapipeline.transforms.feature`
- `datapipeline.transforms.vector`
- `datapipeline.transforms.debug`

Transform config is passed as keyword arguments to the registered callable.
