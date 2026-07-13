from collections.abc import Iterator
from typing import Any

from datapipeline.config.catalog import AlignedStreamConfig
from datapipeline.plugins import MAPPERS_EP
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_combine_stage(spec: AlignedStreamConfig):
    combine = load_ep(MAPPERS_EP, spec.combine.entrypoint)
    args = normalize_args(spec.combine.args)

    def combine_records(rows: Iterator[tuple[Any, ...]]) -> Iterator[Any]:
        for records in rows:
            record = combine(*records, **args)
            if record is not None:
                yield record

    return combine_records
