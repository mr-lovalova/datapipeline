import glob
from typing import assert_never

from datapipeline.config.sources import (
    CsvReaderConfig,
    FsLoaderConfig,
    HttpLoaderConfig,
    JsonLinesReaderConfig,
    JsonReaderConfig,
    ParquetReaderConfig,
    TextReaderConfig,
)
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import (
    CsvDecoder,
    Decoder,
    JsonDecoder,
    JsonLinesDecoder,
)
from datapipeline.sources.models.loader import BaseDataLoader
from datapipeline.sources.parquet_loader import ParquetLoader
from datapipeline.sources.ports import SourceTransport


def build_builtin_loader(config: FsLoaderConfig | HttpLoaderConfig) -> BaseDataLoader:
    transport: SourceTransport
    if isinstance(config, FsLoaderConfig):
        if isinstance(config.reader, ParquetReaderConfig):
            return ParquetLoader(config.path)
        transport = (
            FsGlobTransport(config.path, compression=config.compression)
            if glob.has_magic(config.path)
            else FsFileTransport(config.path, compression=config.compression)
        )
        reader = config.reader
    else:
        transport = HttpTransport(
            config.url,
            headers=config.headers,
            params=config.params,
            timeout_seconds=config.timeout_seconds,
        )
        reader = config.reader

    return DataLoader(transport, _build_text_decoder(reader))


def _build_text_decoder(config: TextReaderConfig) -> Decoder:
    if isinstance(config, CsvReaderConfig):
        return CsvDecoder(
            delimiter=config.delimiter,
            encoding=config.encoding,
            error_prefixes=config.error_prefixes,
        )
    if isinstance(config, JsonReaderConfig):
        return JsonDecoder(
            encoding=config.encoding,
            array_field=config.array_field,
        )
    if isinstance(config, JsonLinesReaderConfig):
        return JsonLinesDecoder(encoding=config.encoding)
    assert_never(config)
