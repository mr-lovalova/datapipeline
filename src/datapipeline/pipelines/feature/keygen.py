from typing import Any, List, Union

from datapipeline.transforms.vector_utils import PARTITION_SEP


class FeatureIdGenerator:
    COMPONENT_PREFIX = "@"
    COMPONENT_JOINER = "_"
    VALUE_DELIMITER = ":"

    def __init__(self, partition_by: Union[str, List[str], None]):
        self.partition_by = partition_by

    def _format_component(self, field: str, value: Any) -> str:
        value_str = "" if value is None else str(value)
        return f"{self.COMPONENT_PREFIX}{field}{self.VALUE_DELIMITER}{value_str}"

    def generate(self, base_id: str, record: Any) -> str:
        if not self.partition_by:
            return base_id
        if isinstance(self.partition_by, str):
            value = getattr(record, self.partition_by)
            suffix = self._format_component(self.partition_by, value)
        else:
            parts = [
                self._format_component(field, getattr(record, field))
                for field in self.partition_by
            ]
            suffix = self.COMPONENT_JOINER.join(parts)
        return f"{base_id}{PARTITION_SEP}{suffix}"
