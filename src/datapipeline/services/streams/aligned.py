from collections.abc import Iterator
from typing import Any

from datapipeline.config.catalog import StreamConfig
from datapipeline.plugins import MAPPERS_EP
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_aligned_mapper(spec: StreamConfig):
    mapper = spec.map
    if mapper is None:
        raise ValueError(f"Aligned stream '{spec.id}' requires map.entrypoint")
    function = load_ep(MAPPERS_EP, mapper.entrypoint)
    args = normalize_args(mapper.args)

    def map_records(rows: Iterator[tuple[Any, ...]]) -> Iterator[Any]:
        for records in rows:
            record = function(*records, **args)
            if record is not None:
                yield record

    return map_records
