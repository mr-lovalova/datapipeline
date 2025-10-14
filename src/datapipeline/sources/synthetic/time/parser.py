from typing import Optional, Dict, Any
from datapipeline.sources.models.parser import DataParser
from datapipeline.domain.record import TemporalRecord


class TimeRowParser(DataParser[TemporalRecord]):
    def parse(self, raw: Dict[str, Any]) -> Optional[TemporalRecord]:
        t = raw["time"]
        return TemporalRecord(time=t, value=t)
