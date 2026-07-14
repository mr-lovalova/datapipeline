from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.feature_id import (
    FEATURE_ID_COMPONENT_SEP,
    encode_feature_id_component,
    make_partition_id,
)
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.transforms.utils import get_field, partition_key


@dataclass
class FeatureProjector:
    partition_by: tuple[str, ...]
    sample_keys: SampleKeyContract
    feature_id_fields: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        sample_keys = set(self.sample_keys.fields)
        self.feature_id_fields = tuple(
            partition_field
            for partition_field in self.partition_by
            if partition_field not in sample_keys
        )

    def project(
        self,
        record: Any,
        configs: Sequence[FeatureRecordConfig],
    ) -> Iterator[FeatureRecord]:
        entity_key = partition_key(record, self.sample_keys.fields)
        self.sample_keys.validate(entity_key)
        suffix = None
        if self.feature_id_fields:
            suffix = FEATURE_ID_COMPONENT_SEP.join(
                encode_feature_id_component(field, getattr(record, field))
                for field in self.feature_id_fields
            )

        for config in configs:
            feature_id = (
                config.id if suffix is None else make_partition_id(config.id, suffix)
            )
            yield FeatureRecord(
                record=record,
                id=feature_id,
                value=get_field(record, config.field),
                entity_key=entity_key,
            )
