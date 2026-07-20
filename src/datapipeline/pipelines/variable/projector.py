from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.domain.variable import VariableRecord
from datapipeline.domain.variable_id import (
    VARIABLE_ID_COMPONENT_SEPARATOR,
    encode_variable_id_component,
    make_partitioned_variable_id,
)
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.domain.value import normalize_data_value
from datapipeline.transforms.utils import get_field, partition_key


@dataclass
class VariableProjector:
    partition_by: tuple[str, ...]
    sample_keys: SampleKeyContract
    variable_id_fields: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        sample_keys = set(self.sample_keys.fields)
        self.variable_id_fields = tuple(
            partition_field
            for partition_field in self.partition_by
            if partition_field not in sample_keys
        )

    def project(
        self,
        record: Any,
        configs: Sequence[VariableConfig],
    ) -> Iterator[VariableRecord]:
        entity_key = partition_key(record, self.sample_keys.fields)
        self.sample_keys.validate(entity_key)
        suffix = None
        if self.variable_id_fields:
            suffix = VARIABLE_ID_COMPONENT_SEPARATOR.join(
                encode_variable_id_component(field, getattr(record, field))
                for field in self.variable_id_fields
            )

        for config in configs:
            variable_id = (
                config.id
                if suffix is None
                else make_partitioned_variable_id(config.id, suffix)
            )
            yield VariableRecord(
                id=variable_id,
                time=record.time,
                value=normalize_data_value(get_field(record, config.field)),
                entity_key=entity_key,
            )
