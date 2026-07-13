import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from datapipeline.utils.time import parse_datetime


@dataclass(frozen=True)
class TickGrid:
    grid_by: tuple[str, ...]
    ticks: dict[tuple, list[datetime]]

    def ticks_for(self, key: tuple) -> list[datetime]:
        return self.ticks.get(key, [])


def read_tick_grid(path: Path, grid_by: tuple[str, ...]) -> TickGrid:
    ticks: dict[tuple, set[datetime]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            _validate_tick_grid_fields(path, row, grid_by)
            value = row.get("time")
            if value is None:
                raise ValueError(
                    f"Tick artifact '{path}' contains a row without time."
                )
            key = tuple(row[field] for field in grid_by)
            ticks.setdefault(key, set()).add(parse_datetime(str(value)))
    return TickGrid(
        grid_by=grid_by,
        ticks={key: sorted(values) for key, values in ticks.items()},
    )


def tick_grid_by_from_metadata(artifact_id: str, meta: Mapping[str, Any]) -> tuple[str, ...]:
    grid_by = meta.get("grid_by")
    if grid_by is None:
        raise RuntimeError(
            f"Tick artifact '{artifact_id}' metadata field 'grid_by' is required."
        )
    if not isinstance(grid_by, list) or any(
        not isinstance(field, str) for field in grid_by
    ):
        raise RuntimeError(
            f"Tick artifact '{artifact_id}' metadata field 'grid_by' must be "
            "a list of strings."
        )
    return tuple(grid_by)


def _validate_tick_grid_fields(
    path: Path,
    row: dict,
    grid_by: tuple[str, ...],
) -> None:
    row_fields = {key for key in row if key != "time"}
    if row_fields != set(grid_by):
        raise ValueError(
            f"Tick artifact '{path}' row grid fields {sorted(row_fields)!r} "
            f"do not match expected grid_by {list(grid_by)!r}."
        )
