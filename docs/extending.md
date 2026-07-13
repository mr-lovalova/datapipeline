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
```

Loader, parser, and mapper extensions must follow the contract of their
respective entry-point group. Refer to the built-in implementations in
`src/datapipeline/sources/`, `src/datapipeline/mappers/`, and
`src/datapipeline/parsers/`.

Record and stream transforms are validated built-in operations rather than
plugin entry points. Feature shaping and postprocess policies are fixed pipeline
stages. See [Transforms](transforms/index.md) for their explicit configuration.

---
