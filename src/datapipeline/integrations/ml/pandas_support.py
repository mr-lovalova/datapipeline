from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from .adapter import GroupFormat
from .rows import collect_vector_rows


def dataframe_from_vectors(
    project_yaml: str,
    *,
    limit: int | None = None,
    include_group: bool = True,
    group_format: GroupFormat = "mapping",
    group_column: str = "group",
    flatten_sequences: bool = False,
    open_stream: Callable[[str], Iterable[Any]] | None = None,
):
    """Return a Pandas DataFrame built from project vectors.

    Pandas is an optional dependency: install it before calling this helper.
    """

    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised by runtime users
        raise RuntimeError(
            "pandas is required for dataframe_from_vectors(); install pandas first.",
        ) from exc

    rows = collect_vector_rows(
        project_yaml,
        limit=limit,
        include_group=include_group,
        group_format=group_format,
        group_column=group_column,
        flatten_sequences=flatten_sequences,
        open_stream=open_stream,
    )
    return pd.DataFrame(rows)


__all__ = ["dataframe_from_vectors"]
