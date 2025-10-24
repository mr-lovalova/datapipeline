import pickle
from pathlib import Path
from typing import TypeVar, Type

T = TypeVar("T")


class PersistanceMixin:
    """Base mixin for persistence-related functionality."""
    pass


class PicklePersistanceMixin(PersistanceMixin):
    """Mixin providing save/load helpers using pickle."""

    def save(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as fh:
            pickle.dump(self, fh, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls: Type[T], path: str | Path) -> T:
        target = Path(path)
        with target.open("rb") as fh:
            obj = pickle.load(fh)
        if not isinstance(obj, cls):
            raise TypeError(
                f"Expected {cls.__name__} state, got {type(obj)!r}")
        return obj
