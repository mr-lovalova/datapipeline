from collections.abc import Iterator, Sequence
from dataclasses import dataclass
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
    feature_id_by: tuple[str, ...] | None
    sample_keys: SampleKeyContract

    def project(
        self,
        record: Any,
        configs: Sequence[FeatureRecordConfig],
    ) -> Iterator[FeatureRecord]:
        entity_key = partition_key(record, self.sample_keys.fields)
        self.sample_keys.validate(entity_key)
        suffix = None
        if self.feature_id_by:
            suffix = FEATURE_ID_COMPONENT_SEP.join(
                encode_feature_id_component(field, getattr(record, field))
                for field in self.feature_id_by
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
