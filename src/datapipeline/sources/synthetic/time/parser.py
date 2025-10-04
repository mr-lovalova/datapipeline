from typing import Optional, Dict, Any
from datapipeline.sources.models.parser import DataParser
from datapipeline.domain.record import TimeSeriesRecord


class TimeRowParser(DataParser[TimeSeriesRecord]):
    def parse(self, raw: Dict[str, Any]) -> Optional[TimeSeriesRecord]:
        t = raw["time"]
        return TimeSeriesRecord(time=t, value=t)
