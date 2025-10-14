from pathlib import Path
from urllib.parse import urlparse

from datapipeline.sources.models.loader import SyntheticLoader, RawDataLoader
from datapipeline.sources.composed_loader import ComposedRawLoader
from datapipeline.sources.transports import FsFileSource, FsGlobSource, UrlSource
from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder


def unit_for_loader(loader) -> str:
    if isinstance(loader, SyntheticLoader):
        return "tick"
    if isinstance(loader, ComposedRawLoader):
        dec = getattr(loader, "decoder", None)
        if isinstance(dec, CsvDecoder):
            return "row"
        if isinstance(dec, (JsonDecoder, JsonLinesDecoder)):
            return "item"
    return "record"


def build_source_label(loader: RawDataLoader) -> str:
    if isinstance(loader, SyntheticLoader):
        try:
            gen_name = loader.generator.__class__.__name__
        except Exception:
            gen_name = loader.__class__.__name__
        return "Generating data with " + gen_name
    if isinstance(loader, ComposedRawLoader):
        src = getattr(loader, "source", None)
        if isinstance(src, (FsFileSource, FsGlobSource)):
            name = str(getattr(src, "pattern", getattr(src, "path", "")))
            label = Path(name).name if (
                name and "*" not in name) else (name or "fs")
            return f"Loading data from: {label}"
        if isinstance(src, UrlSource):
            host = urlparse(src.url).netloc or "http"
            return f"Downloading data from: @{host}"
    return loader.__class__.__name__


def icon_for_loader(loader) -> str:
    if isinstance(loader, SyntheticLoader):
        gen = getattr(loader, "generator", None)
        try:
            if gen is not None:
                if any(hasattr(gen, attr) for attr in ("start", "end", "frequency")):
                    return "🕒"
                if hasattr(gen, "seed"):
                    return "🎲"
        except Exception:
            pass
        return "✨"
    if isinstance(loader, ComposedRawLoader):
        src = getattr(loader, "source", None)
        if isinstance(src, (FsFileSource, FsGlobSource)):
            return "📄"
        if isinstance(src, UrlSource):
            return "🌐"
        return "📦"


def progress_meta_for_loader(loader: RawDataLoader) -> tuple[str, str]:
    return f"{icon_for_loader(loader)} {build_source_label(loader)}", unit_for_loader(loader)
