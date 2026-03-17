import json
from pathlib import Path
from typing import Any, Iterator, Sequence

from datapipeline.cache.contracts import (
    MaterializationManifest,
    MaterializationRef,
)
from datapipeline.io.writers.pickle_writer import PickleFileWriter
from datapipeline.parsers.identity import IdentityParser
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.sources.factory import build_loader
from datapipeline.sources.models.loader import BaseDataLoader
from datapipeline.sources.models.source import Source


class PickleMaterializationStore:
    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()

    @property
    def root(self) -> Path:
        return self._root

    def load(self, ref: MaterializationRef) -> Source | None:
        manifest = self._load_manifest(ref)
        if manifest is None:
            return None
        if manifest.signature_hash != ref.signature_hash:
            return None
        data_path = (self._root / manifest.relative_data_path).resolve()
        if not data_path.exists():
            return None
        return self._source(ref, manifest, data_path)

    def materialize(
        self,
        ref: MaterializationRef,
        items: Iterator[Any],
    ) -> Iterator[Any]:
        data_path = self._data_path(ref)
        manifest_path = self._manifest_path(ref)
        writer = PickleFileWriter(data_path, serializer=lambda item: item)
        row_count = 0
        success = False
        try:
            for item in items:
                writer.write(item)
                row_count += 1
                yield item
            success = True
        finally:
            writer.close()
            if not success:
                data_path.unlink(missing_ok=True)
                manifest_path.unlink(missing_ok=True)
                return
            manifest = MaterializationManifest.create(
                ref,
                data_path=data_path,
                root=self._root,
                row_count=row_count,
            )
            self._save_manifest(ref, manifest)

    def _stream_dir(self, ref: MaterializationRef) -> Path:
        return (
            self._root
            / sanitize_path_segment(ref.kind)
            / sanitize_path_segment(ref.name)
            / sanitize_path_segment(ref.stage)
        )

    def _data_path(self, ref: MaterializationRef) -> Path:
        return (self._stream_dir(ref) / "data.pkl").resolve()

    def _manifest_path(self, ref: MaterializationRef) -> Path:
        return (self._stream_dir(ref) / "manifest.json").resolve()

    def _load_manifest(
        self,
        ref: MaterializationRef,
    ) -> MaterializationManifest | None:
        path = self._manifest_path(ref)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            return None
        try:
            return MaterializationManifest(**data)
        except TypeError:
            return None

    def _save_manifest(
        self,
        ref: MaterializationRef,
        manifest: MaterializationManifest,
    ) -> None:
        path = self._manifest_path(ref)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(manifest.as_json(), handle, indent=2, sort_keys=True)

    @staticmethod
    def _source(
        ref: MaterializationRef,
        manifest: MaterializationManifest,
        path: Path,
    ) -> Source:
        base_loader = build_loader(transport="fs", format="pickle", path=str(path))
        logical_data_path = Path(manifest.relative_data_path).as_posix()
        loader = _ObservedMaterializationLoader(
            base_loader,
            progress_label="Loading cache",
            info_lines=["Loaded contract output from cache"],
            debug_lines=[f"cache.file: {logical_data_path}"],
            include_transport_info=False,
            include_transport_debug=False,
        )
        return Source(
            loader=loader,
            parser=IdentityParser(),
        )


class _ObservedMaterializationLoader(BaseDataLoader):
    def __init__(
        self,
        inner: BaseDataLoader,
        *,
        progress_label: str | None = None,
        info_lines: Sequence[str] = (),
        debug_lines: Sequence[str] = (),
        include_transport_info: bool = True,
        include_transport_debug: bool = True,
    ) -> None:
        self._inner = inner
        self._progress_label = str(progress_label).strip() or None
        self._info_lines = tuple(str(line) for line in info_lines if str(line).strip())
        self._debug_lines = tuple(str(line) for line in debug_lines if str(line).strip())
        self._include_transport_info = bool(include_transport_info)
        self._include_transport_debug = bool(include_transport_debug)
        self.transport = getattr(inner, "transport", None)
        self.decoder = getattr(inner, "decoder", None)

    def load(self) -> Iterator[Any]:
        yield from self._inner.load()

    def count(self) -> int | None:
        return self._inner.count()

    def info_lines(self) -> list[str]:
        return list(self._info_lines)

    def debug_lines(self) -> list[str]:
        return list(self._debug_lines)

    def progress_label(self) -> str | None:
        return self._progress_label

    def include_transport_info(self) -> bool:
        return self._include_transport_info

    def include_transport_debug(self) -> bool:
        return self._include_transport_debug
