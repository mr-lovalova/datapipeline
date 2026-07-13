from collections.abc import Generator, Iterator, Sequence
from datetime import datetime
from itertools import chain, groupby

from datapipeline.execution.events import ProgressSnapshot
from datapipeline.execution.runner import report_node_progress
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.transforms.utils import partition_key


_CanonicalKey = tuple[tuple[object, ...], datetime]


def align_streams(
    inputs: Sequence[tuple[str, Iterator[TemporalRecord]]],
    partition_by: tuple[str, ...],
    buffer_bytes: int,
) -> Generator[tuple[TemporalRecord, ...], None, None]:
    if len(inputs) < 2:
        raise ValueError("Alignment requires at least two input streams")

    populated_inputs: list[tuple[str, Iterator[TemporalRecord]]] = []
    for stream_id, records in inputs:
        try:
            first = next(records)
        except StopIteration:
            return
        populated_inputs.append((stream_id, chain((first,), records)))

    indexed_records = (
        (input_index, record)
        for input_index, (_, records) in enumerate(populated_inputs)
        for record in records
    )
    ordered_records = batch_sort(
        indexed_records,
        buffer_bytes=buffer_bytes,
        key=lambda item: (_canonical_key(item[1], partition_by), item[0]),
    )

    try:
        groups = groupby(
            ordered_records,
            key=lambda item: _canonical_key(item[1], partition_by),
        )
        sorting_complete = False
        for (partition, time), group in groups:
            if not sorting_complete:
                report_node_progress(ProgressSnapshot(completed=0, phase="aligning"))
                sorting_complete = True
            records_by_input: dict[int, TemporalRecord] = {}
            for input_index, record in group:
                if input_index in records_by_input:
                    stream_id = populated_inputs[input_index][0]
                    raise ValueError(
                        f"Alignment input {stream_id!r} has duplicate canonical key "
                        f"partition={partition!r}, time={time.isoformat()}"
                    )
                records_by_input[input_index] = record

            if len(records_by_input) == len(populated_inputs):
                yield tuple(
                    records_by_input[index] for index in range(len(populated_inputs))
                )
    finally:
        ordered_records.close()


def _canonical_key(
    record: TemporalRecord,
    partition_by: tuple[str, ...],
) -> _CanonicalKey:
    return partition_key(record, partition_by), record.time
