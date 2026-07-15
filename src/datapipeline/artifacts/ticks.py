import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from datapipeline.utils.time import parse_datetime


@dataclass(frozen=True)
class TickGrid:
    grid_by: tuple[str, ...]
    ticks: dict[tuple, list[datetime]]

    def ticks_for(self, key: tuple) -> list[datetime]:
        return self.ticks.get(key, [])


def read_tick_grid(path: Path, grid_by: tuple[str, ...]) -> TickGrid:
    ticks: dict[tuple, list[datetime]] = {}
    previous_key: tuple | None = None
    expected_fields = set(grid_by)
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            row_fields = {field for field in row if field != "time"}
            if row_fields != expected_fields:
                raise ValueError(
                    f"Tick artifact '{path}' row grid fields "
                    f"{sorted(row_fields)!r} do not match expected grid_by "
                    f"{list(grid_by)!r}."
                )
            value = row.get("time")
            if value is None:
                raise ValueError(f"Tick artifact '{path}' contains a row without time.")
            key = tuple(row[field] for field in grid_by)
            time = parse_datetime(str(value))
            values = ticks.setdefault(key, [])
            if (
                previous_key is not None and key != previous_key and key <= previous_key
            ) or (values and time <= values[-1]):
                raise ValueError(
                    f"Tick artifact '{path}' rows must be strictly ordered by "
                    f"{[*grid_by, 'time']!r}."
                )
            values.append(time)
            previous_key = key
    return TickGrid(
        grid_by=grid_by,
        ticks=ticks,
    )


def tick_grid_by_from_metadata(
    artifact_id: str, meta: Mapping[str, Any]
) -> tuple[str, ...]:
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
