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
- `datapipeline.transforms.vector`
- `datapipeline.transforms.debug`

The feature stage is different: `dataset.yaml` exposes the explicit `scale`
and `sequence` fields. It does not accept an arbitrary list of feature
transform entry-point names.

## Configuration Shape

Each transform clause in `record:`, `stream:`, `debug:`, or
`postprocess.yaml` must be a mapping with exactly one transform name. Its value
must be a parameter mapping or `null`:

```yaml
stream:
  - dedupe: null
  - rolling:
      field: close
      to: close_mean_20
      window: 20
      statistic: mean
```

`null` and `{}` both mean that the transform has no configured parameters.
Scalar and list parameter values are invalid, as are clauses containing more
than one transform name. Parameter names must be strings.

The configuration boundary parses each valid clause into a `TransformSpec`.
The transform engine consumes only these parsed specs; it does not infer a
calling convention from the YAML value shape.

## Callable Contract

A function entry point is called as `fn(stream, **configured_params)`: it
receives the stream followed by the configured parameters as keyword arguments.

```python
def select_field(stream, *, field):
    ...
```

Transform entry points must declare their supported keyword parameters. The
runtime rejects `**kwargs` catch-alls so misspelled or unsupported
configuration fails immediately.

A class entry point is constructed as `Class(**configured_params)`, and the
resulting instance is called with the stream:

```python
transform = TransformClass(**configured_params)
output_stream = transform(input_stream)
```

Classes that need runtime services may implement
`bind_context(context)`. Partition-aware classes may implement
`bind_partition_by(partition_by)`. The engine invokes those lifecycle hooks,
when implemented, after construction and before passing the stream to the
instance. Context and partition fields are not injected into constructor or
function keyword arguments.
