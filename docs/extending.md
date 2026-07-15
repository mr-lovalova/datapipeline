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

[project.entry-points."datapipeline.combiners"]
air_density = "my_datapipeline.combiners.air_density:combine_air_density"

[project.entry-points."datapipeline.operations.runtime"]
demo.report = "my_datapipeline.operations:run_report"
```

Each extension follows the contract of its entry-point group. A stream `map`
receives an iterator and returns an iterable. An aligned stream `combine`
receives one matching record from each input and returns one record or `None`.
Combiners belong to `datapipeline.combiners`, not the iterator-oriented
`datapipeline.mappers` group.

A custom runtime operation receives exactly three positional arguments:

```python
from datapipeline.config.tasks import OperationTask
from datapipeline.operations.persistence import (
    RuntimeOutput,
    RuntimeOutputBatch,
    SplitRuntimeOutput,
)
from datapipeline.runtime import Runtime


def run_report(
    runtime: Runtime,
    task: OperationTask,
    limit: int | None,
) -> RuntimeOutput | SplitRuntimeOutput | RuntimeOutputBatch | None:
    ...
```

`runtime` is the compiled `Runtime`, `task` is the configured `OperationTask`,
and `limit` is the CLI cap or `None`. Return `RuntimeOutput`,
`SplitRuntimeOutput`, `RuntimeOutputBatch`, or `None`. Jerry persists the result
using the profile output. Dataset split routing, preview, throttle, and
`include_splits` belong to the built-in dataset operation and are not passed to
plugins.

Preprocess and ordered transforms are validated built-in operations rather than
plugin entry points. Feature shaping and postprocess policies are fixed pipeline
stages. See [Transforms](transforms/index.md) for their explicit configuration.

---
