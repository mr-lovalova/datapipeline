import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from datapipeline.config.tasks import TicksTask
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines import build_stream_id_pipeline
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.runtime import Runtime
from datapipeline.transforms.utils import get_field
from datapipeline.utils.paths import ensure_parent


def _close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def _to_iso(ts: datetime) -> str:
    text = ts.isoformat()
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def _tick_row(record, grid_by: list[str]) -> tuple:
    values = []
    for field in grid_by:
        value = get_field(record, field)
        if value is None:
            raise ValueError(f"Tick stream row is missing grid_by field '{field}'.")
        values.append(value)
    return (record.time, *values)


def _json_tick_row(row: tuple, grid_by: list[str]) -> dict:
    payload = {"time": _to_iso(row[0])}
    for field, value in zip(grid_by, row[1:]):
        payload[field] = value
    return payload


def _tick_sort_key(row: tuple) -> tuple:
    return (*row[1:], row[0])


def _unique_ticks(rows) -> Iterator[tuple]:
    previous = None
    for row in rows:
        if row == previous:
            continue
        yield row
        previous = row


def materialize_ticks(
    runtime: Runtime,
    task_cfg: TicksTask,
) -> ArtifactOutput:
    stream = build_stream_id_pipeline(
        PipelineContext(runtime),
        task_cfg.stream,
        node=3,
    )
    batch_size = runtime.registries.sort_batch_size.get(task_cfg.stream)
    tick_rows = (_tick_row(record, task_cfg.grid_by) for record in stream)
    sorted_ticks = batch_sort(
        tick_rows,
        batch_size=batch_size,
        key=_tick_sort_key,
        progress_stage="sort ticks",
    )
    rows = 0
    try:
        relative_path = Path(task_cfg.output)
        destination = (runtime.artifacts_root / relative_path).resolve()
        ensure_parent(destination)
        with destination.open("w", encoding="utf-8") as handle:
            for tick in _unique_ticks(sorted_ticks):
                rows += 1
                handle.write(json.dumps(_json_tick_row(tick, task_cfg.grid_by)))
                handle.write("\n")
    finally:
        _close_iterator(sorted_ticks)
        _close_iterator(stream)

    return ArtifactOutput(
        relative_path=str(relative_path),
        meta={
            "rows": rows,
            "stream": task_cfg.stream,
            "grid_by": list(task_cfg.grid_by),
        },
    )
