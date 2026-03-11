from typing import Any

from datapipeline.domain.sample import Sample
from datapipeline.io.normalization import (
    NormalizedRow,
    flatten_fields,
    normalized_record_row,
    normalized_row,
)


def row_from_sample(sample: Sample) -> NormalizedRow:
    fields: dict[str, Any] = {}
    flatten_fields("features", sample.features.values, fields)
    if sample.targets is not None:
        flatten_fields("targets", sample.targets.values, fields)
    return normalized_row(
        key=sample.key,
        kind=type(sample).__name__,
        raw=sample.as_full_payload(),
        fields=fields,
    )


def row_from_record(item: Any) -> NormalizedRow:
    return normalized_record_row(item)
