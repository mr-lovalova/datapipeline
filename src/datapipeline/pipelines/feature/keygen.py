from datapipeline.domain.feature_id import (
    FEATURE_ID_COMPONENT_SEP,
    encode_feature_id_component,
    make_partition_id,
)


class FeatureIdGenerator:
    def __init__(self, feature_id_by: tuple[str, ...] | None):
        self.feature_id_by = feature_id_by

    def generate(self, base_id: str, record: object) -> str:
        if not self.feature_id_by:
            return base_id
        components = [
            encode_feature_id_component(field, getattr(record, field))
            for field in self.feature_id_by
        ]
        suffix = FEATURE_ID_COMPONENT_SEP.join(components)
        return make_partition_id(base_id, suffix)
