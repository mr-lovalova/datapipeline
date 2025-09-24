"""Vector-level pipeline transforms."""

from __future__ import annotations

import json
from pathlib import Path
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from typing import Any, Tuple

from datapipeline.domain.vector import Vector

_MANIFEST_CACHE: dict[Path, dict[str, Any]] = {}


def _load_manifest(path: Path) -> dict[str, Any]:
    """Load and cache the JSON partition manifest."""

    normalized = path.expanduser().resolve()
    cached = _MANIFEST_CACHE.get(normalized)
    if cached is not None:
        return cached

    with normalized.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if not isinstance(data, Mapping):
        raise TypeError("Partition manifest must be a mapping")

    _MANIFEST_CACHE[normalized] = data
    return data


class DropIncompleteVectorTransform:
    """Drop vectors that do not meet required coverage constraints."""

    def __init__(
        self,
        *,
        required: Sequence[str] | None = None,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
        min_coverage: float = 1.0,
    ) -> None:
        if not 0.0 <= min_coverage <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")

        # ``expected`` is retained for backwards compatibility but treated as ``required``.
        baseline = required or expected or []

        self.required: set[str] = set(baseline)
        self.manifest_path = Path(manifest) if manifest else None
        self.min_coverage = min_coverage

        self._baseline: set[str] = set(self.required)
        self._manifest_features: set[str] | None = None
        self._seen_features: set[str] = set()

    def _ensure_manifest(self) -> None:
        if not self.manifest_path or self._manifest_features is not None:
            return

        manifest = _load_manifest(self.manifest_path)
        features = manifest.get("features")
        if not isinstance(features, Sequence):
            raise TypeError("Partition manifest must expose a 'features' list")
        self._manifest_features = {str(f) for f in features}

        if not self.required:
            self.required = set(self._manifest_features)
            self._baseline = set(self._manifest_features)

    def _coverage(self, present: set[str], baseline: set[str]) -> float:
        if not baseline:
            return 1.0
        return len(present & baseline) / len(baseline)

    def apply(
        self, stream: Iterator[Tuple[Any, Vector]]
    ) -> Iterator[Tuple[Any, Vector]]:
        self._ensure_manifest()

        for group_key, vector in stream:
            present = set(vector.values.keys())
            self._seen_features |= present

            if not self._baseline and not self.required:
                # First row establishes the baseline when no manifest/requirements exist yet.
                self._baseline = set(present)
            elif not self.manifest_path and not self.required:
                # Expand the baseline with newly observed columns.
                self._baseline |= present

            baseline = self.required or self._baseline

            if self.required and not self.required.issubset(present):
                continue

            if baseline:
                coverage = self._coverage(present, baseline)
                if coverage < self.min_coverage:
                    continue

            yield group_key, vector


class FillMissingVectorTransform:
    """Fill missing vector entries using a constant strategy."""

    def __init__(
        self,
        *,
        expected: Sequence[str] | None = None,
        features: Sequence[str] | None = None,
        manifest: str | None = None,
        strategy: str = "constant",
        value: Any = 0.0,
    ) -> None:
        if strategy != "constant":
            raise ValueError("Only the 'constant' fill strategy is supported")

        manifest_path = Path(manifest) if manifest else None
        target = list(features or expected or [])

        if not target and manifest_path:
            manifest = _load_manifest(manifest_path)
            feature_list = manifest.get("features")
            if not isinstance(feature_list, Sequence):
                raise TypeError("Partition manifest must expose a 'features' list")
            target = [str(f) for f in feature_list]

        self.features = list(target)
        self.value = value

    @staticmethod
    def _clone(values: Mapping[str, Any]) -> MutableMapping[str, Any]:
        if isinstance(values, MutableMapping):
            return type(values)(values)
        return dict(values)

    def apply(
        self, stream: Iterator[Tuple[Any, Vector]]
    ) -> Iterator[Tuple[Any, Vector]]:
        if not self.features:
            yield from stream
            return

        for group_key, vector in stream:
            data = self._clone(vector.values)
            updated = False
            for feature in self.features:
                if feature not in data:
                    data[feature] = self.value
                    updated = True

            if updated:
                yield group_key, Vector(values=data)
            else:
                yield group_key, vector
