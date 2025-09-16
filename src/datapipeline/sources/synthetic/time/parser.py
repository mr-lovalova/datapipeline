from typing import Optional, Dict, Any
from datapipeline.sources.models.parser import DataParser
from datapipeline.domain.record import TimeFeatureRecord


class TimeRowParser(DataParser[TimeFeatureRecord]):
    def parse(self, raw: Dict[str, Any]) -> Optional[TimeFeatureRecord]:
        t = raw["time"]
        return TimeFeatureRecord(time=t, value=t)
