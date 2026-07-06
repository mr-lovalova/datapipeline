from datapipeline.sources.models.loader import SyntheticLoader, BaseDataLoader
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder, PickleDecoder


def _transport_for_loader(loader):
    return getattr(loader, "transport", None)


def _decoder_for_loader(loader):
    return getattr(loader, "decoder", None)


def unit_for_loader(loader) -> str:
    if isinstance(loader, SyntheticLoader):
        return "tick"
    dec = _decoder_for_loader(loader)
    if dec is None:
        return "record"
    if isinstance(dec, CsvDecoder):
        return "row"
    if isinstance(dec, (JsonDecoder, JsonLinesDecoder, PickleDecoder)):
        return "item"
    return "record"


def build_source_label(loader: BaseDataLoader) -> str:
    custom = _custom_progress_label(loader)
    if custom is not None:
        return custom
    if isinstance(loader, SyntheticLoader):
        try:
            gen_name = loader.generator.__class__.__name__
        except Exception:
            gen_name = loader.__class__.__name__
        return "generating data with " + gen_name
    if isinstance(loader, ForeachLoader):
        key = str(getattr(loader, "_key", "item"))
        values = getattr(loader, "_values", None)
        n = len(values) if isinstance(values, list) else "?"
        return f"fan-out {key}×{n}:"
    if _decoder_for_loader(loader) is None:
        return loader.__class__.__name__
    transport = _transport_for_loader(loader)
    if isinstance(transport, (FsFileTransport, FsGlobTransport)):
        return "streaming from"
    if isinstance(transport, HttpTransport):
        return "downloading"
    return loader.__class__.__name__


def progress_meta_for_loader(loader: BaseDataLoader) -> tuple[str, str]:
    return build_source_label(loader), unit_for_loader(loader)


def _custom_progress_label(loader) -> str | None:
    producer = getattr(loader, "progress_label", None)
    if not callable(producer):
        return None
    try:
        value = producer()
    except Exception:
        return None
    text = str(value).strip() if value is not None else ""
    return text or None
