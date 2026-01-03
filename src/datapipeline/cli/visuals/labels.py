from datapipeline.sources.models.loader import SyntheticLoader, BaseDataLoader
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.transports import FsFileTransport, FsGlobTransport, HttpTransport
from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder, PickleDecoder


def unit_for_loader(loader) -> str:
    if isinstance(loader, SyntheticLoader):
        return "tick"
    if isinstance(loader, DataLoader):
        dec = getattr(loader, "decoder", None)
        if isinstance(dec, CsvDecoder):
            return "row"
        if isinstance(dec, (JsonDecoder, JsonLinesDecoder, PickleDecoder)):
            return "item"
    return "record"


def build_source_label(loader: BaseDataLoader) -> str:
    if isinstance(loader, SyntheticLoader):
        try:
            gen_name = loader.generator.__class__.__name__
        except Exception:
            gen_name = loader.__class__.__name__
        return "Generating data with " + gen_name
    if isinstance(loader, ForeachLoader):
        key = str(getattr(loader, "_key", "item"))
        values = getattr(loader, "_values", None)
        n = len(values) if isinstance(values, list) else "?"
        return f"Fan-out {key}×{n}:"
    if isinstance(loader, DataLoader):
        transport = getattr(loader, "transport", None)
        if isinstance(transport, (FsFileTransport, FsGlobTransport)):
            return "Loading"
        if isinstance(transport, HttpTransport):
            return "Downloading"
    return loader.__class__.__name__


def progress_meta_for_loader(loader: BaseDataLoader) -> tuple[str, str]:
    return build_source_label(loader), unit_for_loader(loader)
