from typing import Any

from datapipeline.domain.record import TemporalRecord
from datapipeline.sources.parser import DataParser


class TimeRowParser(DataParser[TemporalRecord]):
    def parse(self, raw: dict[str, Any]) -> TemporalRecord:
        t = raw["time"]
        return TemporalRecord(time=t)
