from __future__ import annotations

import re
from typing import Any, Iterator, Mapping

from datapipeline.plugins import LOADERS_EP
from datapipeline.sources.models.loader import BaseDataLoader
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args, MissingInterpolation, is_missing


_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _interpolate(obj: Any, vars_: Mapping[str, Any]) -> Any:
    if isinstance(obj, dict):
        return {k: _interpolate(v, vars_) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v, vars_) for v in obj]
    if isinstance(obj, str):
        match = _VAR_RE.fullmatch(obj)
        if match:
            key = match.group(1)
            if key in vars_:
                value = vars_[key]
                if value is None or is_missing(value):
                    return MissingInterpolation(key)
                return value
            return obj

        def repl(m):
            key = m.group(1)
            value = vars_.get(key, m.group(0))
            if value is None or is_missing(value):
                return m.group(0)
            return str(value)

        return _VAR_RE.sub(repl, obj)
    return obj


class ForeachLoader(BaseDataLoader):
    """Expand a loader spec across a foreach map and concatenate results."""

    def __init__(
        self,
        *,
        foreach: Mapping[str, list[Any]],
        loader: Mapping[str, Any],
        inject_field: str | None = None,
        inject: Mapping[str, Any] | None = None,
    ):
        self._key, self._values = self._normalize_foreach(foreach)
        self._loader_spec = self._normalize_loader_spec(loader)
        self._inject_field = inject_field
        self._inject = inject
        self._current_index: int | None = None
        self._current_value: Any | None = None
        self._current_args: dict[str, Any] | None = None
        self._current_transport: Any | None = None

        if inject_field and inject:
            raise ValueError("core.foreach supports only one of inject_field or inject")
        if inject_field and self._key is None:
            raise ValueError("inject_field requires a non-empty foreach map")
        if inject is not None and not isinstance(inject, Mapping):
            raise TypeError("inject must be a mapping when provided")

    def load(self) -> Iterator[Any]:
        for i, value in enumerate(self._values, 1):
            vars_ = {self._key: value}
            loader_args = self._make_loader_args(vars_)
            loader = self._build_loader(loader_args)
            self._current_index = i
            self._current_value = value
            self._current_args = loader_args
            self._current_transport = getattr(loader, "transport", None)
            inject_map = self._build_inject(vars_)
            for row in loader.load():
                if inject_map:
                    yield self._apply_inject(row, inject_map)
                else:
                    yield row

    def count(self):
        total = 0
        for value in self._values:
            vars_ = {self._key: value}
            loader_args = self._make_loader_args(vars_)
            loader = self._build_loader(loader_args)
            c = loader.count()
            if c is None:
                return None
            total += int(c)
        return total

    @staticmethod
    def _normalize_foreach(foreach: Mapping[str, list[Any]]):
        if not isinstance(foreach, Mapping) or not foreach:
            raise ValueError("core.foreach requires a non-empty foreach mapping")
        keys = list(foreach.keys())
        if len(keys) != 1:
            raise ValueError("core.foreach currently supports exactly one foreach key")
        key = keys[0]
        values = foreach[key]
        if not isinstance(values, list):
            raise TypeError("core.foreach foreach values must be a list")
        return str(key), list(values)

    @staticmethod
    def _normalize_loader_spec(loader: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(loader, Mapping):
            raise TypeError("core.foreach loader must be a mapping with entrypoint/args")
        entrypoint = loader.get("entrypoint")
        if not entrypoint or not isinstance(entrypoint, str):
            raise ValueError("core.foreach loader.entrypoint must be a non-empty string")
        args = loader.get("args")
        if args is not None and not isinstance(args, Mapping):
            raise TypeError("core.foreach loader.args must be a mapping when provided")
        return dict(loader)

    def _make_loader_args(self, vars_: Mapping[str, Any]) -> dict[str, Any]:
        args = self._loader_spec.get("args") or {}
        interpolated = _interpolate(args, vars_)
        return normalize_args(interpolated)

    def _build_loader(self, loader_args: dict[str, Any]) -> BaseDataLoader:
        entrypoint = self._loader_spec["entrypoint"]
        L = load_ep(LOADERS_EP, entrypoint)
        return L(**loader_args)

    def _build_inject(self, vars_: Mapping[str, Any]) -> Mapping[str, Any] | None:
        if self._inject_field:
            return {self._inject_field: vars_.get(self._key)}
        if self._inject is None:
            return None
        interpolated = _interpolate(self._inject, vars_)
        if not isinstance(interpolated, Mapping):
            raise TypeError("core.foreach inject must resolve to a mapping")
        return normalize_args(interpolated)

    @staticmethod
    def _apply_inject(row: Any, inject_map: Mapping[str, Any]) -> Any:
        if isinstance(row, dict):
            row.update(inject_map)
            return row
        if isinstance(row, Mapping):
            out = dict(row)
            out.update(inject_map)
            return out
        raise TypeError("core.foreach inject requires mapping rows")
