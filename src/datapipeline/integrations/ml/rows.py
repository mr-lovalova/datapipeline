from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from datapipeline.domain.vector import Vector

from .adapter import GroupFormat, VectorAdapter


def stream_vectors(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
) -> Iterator[tuple[Sequence[Any], Vector]]:
    """Yield ``(group_key, Vector)`` pairs for the configured project."""

    adapter = VectorAdapter.from_project(project_yaml)
    try:
        return adapter.stream(limit=limit)
    except ValueError:
        return iter(())


def iter_vector_rows(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
    include_group: bool = True,
    group_format: GroupFormat = "mapping",
    group_column: str = "group",
    flatten_sequences: bool = False,
) -> Iterator[dict[str, Any]]:
    """Return an iterator of row dictionaries derived from vectors."""

    adapter = VectorAdapter.from_project(project_yaml)
    try:
        return adapter.iter_rows(
            limit=limit,
            include_group=include_group,
            group_format=group_format,
            group_column=group_column,
            flatten_sequences=flatten_sequences,
        )
    except ValueError:
        return iter(())


def collect_vector_rows(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
    include_group: bool = True,
    group_format: GroupFormat = "mapping",
    group_column: str = "group",
    flatten_sequences: bool = False,
    open_stream=None,
) -> list[dict[str, Any]]:
    """Materialize :func:`iter_vector_rows` into a list for eager workflows."""

    iterator = iter_vector_rows(
        project_yaml,
        limit=limit,
        include_group=include_group,
        group_format=group_format,
        group_column=group_column,
        flatten_sequences=flatten_sequences,
    )
    return list(iterator)


__all__ = [
    "collect_vector_rows",
    "iter_vector_rows",
    "stream_vectors",
]
