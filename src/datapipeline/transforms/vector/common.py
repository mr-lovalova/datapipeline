from collections.abc import Mapping, Sequence, Set
from math import isfinite
from numbers import Real
from typing import Any


def select_expected_ids(
    expected_ids: Sequence[str],
    ids: Sequence[str] | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    expected = tuple(expected_ids)
    if not expected:
        raise ValueError("expected_ids must not be empty")
    if any(
        not isinstance(identifier, str) or not identifier for identifier in expected
    ):
        raise ValueError("expected_ids must contain non-empty strings")
    expected_set = set(expected)
    if len(expected_set) != len(expected):
        raise ValueError("expected_ids must not contain duplicates")

    selected = expected if ids is None else tuple(ids)
    if not selected:
        raise ValueError("ids must not be empty")
    if any(
        not isinstance(identifier, str) or not identifier for identifier in selected
    ):
        raise ValueError("ids must contain non-empty strings")
    if len(set(selected)) != len(selected):
        raise ValueError("ids must not contain duplicates")

    unknown = [identifier for identifier in selected if identifier not in expected_set]
    if unknown:
        raise ValueError(f"Unknown vector ids: {unknown!r}")
    return expected, selected


def require_scalar(value: Any, identifier: str) -> float | None:
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(
            f"Vector id {identifier!r} must contain a finite number or null; "
            f"got {type(value).__name__}."
        )
    number = float(value)
    if not isfinite(number):
        raise ValueError(
            f"Vector id {identifier!r} must contain a finite number or null."
        )
    return number


def cell_coverage(value: Any, identifier: str) -> float:
    if isinstance(value, list):
        if not value:
            return 0.0
        present = sum(require_scalar(item, identifier) is not None for item in value)
        return present / len(value)
    return 0.0 if require_scalar(value, identifier) is None else 1.0


def reject_unknown_values(
    values: Mapping[str, Any],
    expected_ids: Set[str],
) -> None:
    unknown = [identifier for identifier in values if identifier not in expected_ids]
    if unknown:
        raise ValueError(f"Vector contains unexpected ids: {unknown!r}")
