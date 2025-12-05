from pathlib import Path
from urllib.parse import urlparse

from datapipeline.sources.models.loader import SyntheticLoader, BaseDataLoader
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.transports import FsFileTransport, FsGlobTransport, HttpTransport
from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder, PickleDecoder

MAX_LABEL_LEN = 48
GLOB_SEGMENTS = 3


def _truncate_middle(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    keep = max_len - 3
    head = (keep + 1) // 2
    tail = keep - head
    suffix = text[-tail:] if tail > 0 else ""
    return f"{text[:head]}...{suffix}"


def _compact_path_label(name: str) -> str:
    if not name:
        return "fs"
    normalized = name.replace("\\", "/").strip()
    if not normalized:
        return "fs"
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return normalized
    if len(parts) > GLOB_SEGMENTS:
        parts = parts[-GLOB_SEGMENTS:]
    label = "/".join(parts)
    return _truncate_middle(label, MAX_LABEL_LEN)


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
    if isinstance(loader, DataLoader):
        transport = getattr(loader, "transport", None)
        if isinstance(transport, (FsFileTransport, FsGlobTransport)):
            name = str(getattr(transport, "pattern", getattr(transport, "path", "")))
            if isinstance(transport, FsFileTransport) and name and "*" not in name:
                label = Path(name).name or "fs"
            else:
                label = _compact_path_label(name)
            return f"Loading data from: {label}"
        if isinstance(transport, HttpTransport):
            host = urlparse(transport.url).netloc or "http"
            return f"Downloading data from: @{host}"
    return loader.__class__.__name__


def progress_meta_for_loader(loader: BaseDataLoader) -> tuple[str, str]:
    return build_source_label(loader), unit_for_loader(loader)
