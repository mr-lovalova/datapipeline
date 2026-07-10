from bisect import bisect_right
from collections.abc import Iterator
from datetime import datetime

from datapipeline.artifacts.ticks import (
    TickGrid,
    read_tick_grid,
    tick_grid_by_from_metadata,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.services.artifacts import ArtifactNotRegisteredError
from datapipeline.transforms.interfaces import PartitionedFieldStreamTransformBase
from datapipeline.transforms.utils import clone_record
from datapipeline.utils.time import parse_timecode


class EnsureCadenceTransform(PartitionedFieldStreamTransformBase):
    """Insert placeholder records so timestamps are exactly one cadence apart per partition.

    - cadence: duration string (e.g., "10m", "1h", "30s").
    - Placeholders carry field=None and inherit partition metadata.
    - Assumes input sorted by (partition_key, record.time).
    """

    def __init__(
        self,
        *,
        cadence: str,
        field: str,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, partition_by=partition_by)
        self.cadence = cadence
        self.context: PipelineContext | None = None
        self._artifact_ticks: TickGrid | None = None

    def bind_context(self, context: PipelineContext) -> None:
        self.context = context

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        try:
            step = parse_timecode(self.cadence)
        except ValueError:
            yield from self._apply_artifact_ticks(stream)
            return
        if step.total_seconds() <= 0:
            raise ValueError("ensure_cadence duration must be positive")

        last: TemporalRecord | None = None
        last_key: tuple | None = None
        for record in stream:
            key = self.partition_key(record)
            if last is None or last_key != key:
                yield record
                last = record
                last_key = key
                continue

            expect = last.time + step
            while expect < record.time:
                yield self._placeholder_record(last, expect)
                expect = expect + step
            yield record
            last = record

    def _apply_artifact_ticks(
        self,
        stream: Iterator[TemporalRecord],
    ) -> Iterator[TemporalRecord]:
        ticks = self._ticks_from_artifact()
        last: TemporalRecord | None = None
        last_key: tuple | None = None
        for record in stream:
            key = self.partition_key(record)
            if last is None:
                yield from self._placeholder_ticks_before(record, ticks.ticks_for(key))
                yield record
                last = record
                last_key = key
                continue
            if last_key != key:
                yield from self._placeholder_ticks_after(
                    last,
                    ticks.ticks_for(last_key or ()),
                )
                yield from self._placeholder_ticks_before(record, ticks.ticks_for(key))
                yield record
                last = record
                last_key = key
                continue

            yield from self._placeholder_ticks_between(
                last,
                record,
                ticks.ticks_for(key),
            )
            yield record
            last = record
        if last is not None:
            yield from self._placeholder_ticks_after(
                last, ticks.ticks_for(last_key or ())
            )

    def _ticks_from_artifact(self) -> TickGrid:
        if self._artifact_ticks is not None:
            return self._artifact_ticks
        if self.context is None:
            raise RuntimeError(
                f"Cadence '{self.cadence}' is not a duration and cannot be resolved "
                "without a pipeline context."
            )
        try:
            path = self.context.resolve_artifact_path(self.cadence)
        except ArtifactNotRegisteredError as exc:
            raise RuntimeError(
                f"Cadence '{self.cadence}' is not a duration and no tick artifact "
                "with that id is registered."
            ) from exc
        grid_by = tick_grid_by_from_metadata(
            self.cadence,
            self.context.artifact_metadata(self.cadence),
        )
        self._artifact_ticks = read_tick_grid(path, grid_by)
        partition_fields = self.partition_fields()
        if self._artifact_ticks.grid_by != partition_fields:
            raise RuntimeError(
                f"Tick artifact '{self.cadence}' grid_by "
                f"{list(self._artifact_ticks.grid_by)!r} "
                f"must match ensure_cadence partition_by {list(partition_fields)!r}."
            )
        return self._artifact_ticks

    def _placeholder_ticks_before(
        self,
        record: TemporalRecord,
        ticks: list[datetime],
    ) -> Iterator[TemporalRecord]:
        for tick in ticks:
            if tick >= record.time:
                break
            yield self._placeholder_record(record, tick)

    def _placeholder_ticks_between(
        self,
        previous: TemporalRecord,
        current: TemporalRecord,
        ticks: list[datetime],
    ) -> Iterator[TemporalRecord]:
        start_index = bisect_right(ticks, previous.time)
        for tick in ticks[start_index:]:
            if tick >= current.time:
                break
            yield self._placeholder_record(previous, tick)

    def _placeholder_ticks_after(
        self,
        record: TemporalRecord,
        ticks: list[datetime],
    ) -> Iterator[TemporalRecord]:
        start_index = bisect_right(ticks, record.time)
        for tick in ticks[start_index:]:
            yield self._placeholder_record(record, tick)

    def _placeholder_record(self, record: TemporalRecord, time) -> TemporalRecord:
        keep = {"time", self.field, *self.partition_fields()}
        updates = {
            key: None
            for key in record.__dict__
            if not key.startswith("_") and key not in keep
        }
        return clone_record(record, time=time, **updates, **{self.field: None})
