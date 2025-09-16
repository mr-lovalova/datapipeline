from typing import Union, List, Any


class RecordKeyGenerator:
    """
    Generates unique feature keys by appending suffixes from expand_by fields.
    """

    def __init__(self, partition_by: Union[str, List[str], None]):
        self.partition_by = partition_by

    def generate(self, base_id: str, record: Any) -> str:
        if not self.partition_by:
            return base_id
        if isinstance(self.partition_by, str):
            suffix = getattr(record, self.partition_by)
        else:
            suffix = "__".join(str(getattr(record, f))
                               for f in self.partition_by)
        return f"{base_id}__{suffix}"
