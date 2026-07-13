from collections.abc import Iterator
from typing import Protocol, TypeVar


TRecord = TypeVar("TRecord")


def canonical_record_order(
    partition_by: str | list[str] | None,
) -> list[str]:
    if partition_by is None:
        return ["time"]
    if isinstance(partition_by, str):
        return [partition_by, "time"]
    return [*partition_by, "time"]


class RecordStream(Protocol[TRecord]):
    def stream(self) -> Iterator[TRecord]:
        pass
