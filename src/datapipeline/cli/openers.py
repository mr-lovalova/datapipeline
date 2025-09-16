from typing import Iterator, Any
from datapipeline.streams.canonical import canonical_entry
from datapipeline.streams.raw import open_raw_stream
from datapipeline.cli.visuals import wrap_with_tqdm


def open_canonical_stream_visual(alias: str, show: bool = True) -> Iterator[Any]:
    entry = canonical_entry(alias)
    raw = open_raw_stream(entry.source_alias)
    wrapped = wrap_with_tqdm(raw, stream_alias=alias, show=show)
    return entry.mapper(wrapped, **entry.mapper_args)
