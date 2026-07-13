from dataclasses import dataclass
from typing import Any, Protocol


class CsvRowProjector(Protocol):
    def __call__(self, item: Any) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class CsvProjectedRow:
    header: list[str]
    values: list[Any]


class CsvTableProjector:
    """Projects rows into a stable table schema (header locked on first row)."""

    def __init__(self, row_projector: CsvRowProjector) -> None:
        self._row_projector = row_projector
        self._header: list[str] | None = None

    def project(self, item: Any) -> CsvProjectedRow:
        row = self._row_projector(item)
        if self._header is None:
            self._header = list(row.keys())
        else:
            unexpected = [name for name in row.keys() if name not in self._header]
            if unexpected:
                raise ValueError(
                    "CSV row contains fields not present in header: "
                    + ", ".join(unexpected)
                )
        assert self._header is not None
        return CsvProjectedRow(
            header=self._header,
            values=[row.get(field, "") for field in self._header],
        )
