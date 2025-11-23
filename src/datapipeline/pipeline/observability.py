from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class TransformEvent:
    type: str
    payload: Mapping[str, object]


# Observer receives a structured event.
Observer = Callable[[TransformEvent], None]
# Factory builds an observer for a given logger (may return None if not active at current level).
ObserverFactory = Callable[[logging.Logger], Optional[Observer]]


@runtime_checkable
class SupportsObserver(Protocol):
    def set_observer(self, observer: Optional[Observer]) -> None:
        ...


class ObserverRegistry:
    def __init__(self, factories: Optional[Mapping[str, ObserverFactory]] = None) -> None:
        self._factories: dict[str, ObserverFactory] = dict(factories or {})

    def register(self, name: str, factory: ObserverFactory) -> None:
        self._factories[name] = factory

    def get(self, name: str, logger: logging.Logger) -> Optional[Observer]:
        factory = self._factories.get(name)
        if not factory:
            return None
        return factory(logger)


def _scaler_observer_factory(logger: logging.Logger) -> Optional[Observer]:
    if not logger.isEnabledFor(logging.WARNING):
        return None

    totals: dict[str, int] = {}
    warned: set[str] = set()

    def _observer(event: TransformEvent) -> None:
        if event.type != "scaler_none":
            return
        fid = event.payload.get("feature_id")
        count = event.payload.get("count")
        record = event.payload.get("record")
        if isinstance(fid, str) and isinstance(count, int):
            totals[fid] = totals.get(fid, 0) + 1
        time = getattr(record, "time", None)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Scaler encountered None: feature=%s time=%s",
                fid,
                time,
            )
        else:
            # Warn once per feature with total seen so far.
            if isinstance(fid, str) and fid not in warned:
                warned.add(fid)
                logger.warning(
                    "Scaler encountered None: feature=%s total_count=%s",
                    fid,
                    totals.get(fid, count),
                )

    return _observer


def default_observer_registry() -> ObserverRegistry:
    registry = ObserverRegistry()
    registry.register("scale", _scaler_observer_factory)
    return registry
