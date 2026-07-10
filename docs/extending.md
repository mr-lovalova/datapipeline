# Extending the Runtime

### Entry Points

Register custom components in your plugin’s `pyproject.toml`:

```toml
[project.entry-points."datapipeline.loaders"]
demo.csv_loader = "my_datapipeline.loaders.csv:CsvLoader"

[project.entry-points."datapipeline.parsers"]
demo.weather_parser = "my_datapipeline.parsers.weather:WeatherParser"

[project.entry-points."datapipeline.mappers"]
time.ticks = "my_datapipeline.mappers.synthetic.ticks:map"

[project.entry-points."datapipeline.transforms.stream"]
weather.fill = "my_datapipeline.transforms.weather:CustomFill"
```

Loader, parser, and mapper extensions must follow the contract of their
respective entry-point group. Refer to the built-in implementations in
`src/datapipeline/sources/`, `src/datapipeline/mappers/`, and
`src/datapipeline/parsers/`.

Transform entry points follow one explicit contract. YAML parameters must be a
mapping or `null`; mapping entries are passed as configured keyword arguments.
A transform function is called as `fn(stream, **configured_params)`. A
transform class is constructed as `Class(**configured_params)`, then its
instance is called with the stream. Entry points must declare each supported
keyword parameter; `**kwargs` catch-alls are rejected.

Class-based transforms may implement `bind_context(context)` when they need
runtime services and `bind_partition_by(partition_by)` when they maintain
partitioned state. The runtime calls implemented hooks after construction and
before the instance receives the stream. It does not inject context,
partitioning, or other undeclared arguments into functions or constructors.
See [Transforms](transforms/index.md) for the accepted clause shape and full
contract.

### Transform migration to 4.0

Earlier versions inspected transform signatures and implicitly injected
`context`, `partition_by`, and `stream_partition_by`; they also activated an
ambient pipeline context while invoking the entry point. Version 4.0 removes
those implicit paths. Move context and partition access to the class hooks
above, and replace scalar or list transform parameters with a named mapping.

When a legacy signature still declares one of the injected parameters, the
runtime raises a migration error instead of allowing a partition-aware
transform to run without its state key. Code that reads the ambient context
without declaring it cannot be detected automatically and must migrate to
`bind_context(context)`.

---
