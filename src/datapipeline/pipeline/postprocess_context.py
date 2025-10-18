from contextvars import ContextVar
from typing import Iterable, List, Optional


_EXPECTED_IDS: ContextVar[Optional[List[str]]] = ContextVar(
    "dp_postprocess_expected_ids", default=None
)


def set_expected_ids(ids: Iterable[str]):
    return _EXPECTED_IDS.set(list(ids))


def reset_expected_ids(token) -> None:
    try:
        _EXPECTED_IDS.reset(token)
    except Exception:
        pass


def get_expected_ids() -> list[str]:
    ids = _EXPECTED_IDS.get()
    return list(ids) if ids else []
