"""Helpers for funneling pipeline vectors into ML workflows.

The datapipeline runtime deliberately exposes iterator-based builders so
downstream projects can decide how vectors should be materialized.  This
module packages a small, dependency-light adapter layer that does the
plumbing for common scenarios:

* :class:`VectorAdapter` bootstraps a project once and exposes helper
  iterators for raw ``(group_key, Vector)`` pairs or row-shaped mappings.
* :func:`stream_vectors` mirrors the CLI "serve" command but returns an
  iterator that can be consumed directly inside model code.
* :func:`iter_vector_rows` yields dictionaries that are convenient to feed
  into Pandas, NumPy, or bespoke feature store upload scripts.
* :func:`dataframe_from_vectors` and :func:`torch_dataset` show how the same
  iterator can be materialized into higher-level ML containers without
  bloating the core runtime with optional dependencies.

None of these functions change how pipelines execute—they simply provide a
documented pattern for ML practitioners who want to bridge the existing
builders with their preferred tooling.
"""

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Literal, Mapping

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process
from datapipeline.services.bootstrap import bootstrap
from datapipeline.runtime import Runtime

GroupFormat = Literal["mapping", "tuple", "list", "flat"]


def _normalize_group(
    group_key: Sequence[Any],
    group_by: str,
    fmt: GroupFormat,
) -> Any:
    if fmt == "tuple":
        return tuple(group_key)
    if fmt == "list":
        return list(group_key)
    if fmt == "flat":
        if len(group_key) != 1:
            raise ValueError(
                "group_format='flat' requires exactly one group key value",
            )
        return group_key[0]
    # Default: mapping of field names → values. With simplified GroupBy, use 'time'.
    fields = ["time"]
    return {field: value for field, value in zip(fields, group_key)}


def _ensure_features(dataset: FeatureDatasetConfig) -> list:
    features = list(dataset.features or [])
    if not features:
        raise ValueError(
            "Dataset does not define any features. Configure at least one feature "
            "stream before attempting to stream vectors.",
        )
    return features


@dataclass
class VectorAdapter:
    """Bootstrap a project once and provide ML-friendly iterators."""

    dataset: FeatureDatasetConfig
    runtime: Runtime

    @classmethod
    def from_project(
        cls,
        project_yaml: str | Path,
        *,
    ) -> "VectorAdapter":
        project_path = Path(project_yaml)
        dataset = load_dataset(project_path, "vectors")
        runtime = bootstrap(project_path)
        return cls(dataset=dataset, runtime=runtime)

    def stream(
        self,
        *,
        limit: int | None = None,
        include_targets: bool = False,
    ) -> Iterator[tuple[Sequence[Any], Vector]]:
        features = list(_ensure_features(self.dataset))
        if include_targets:
            try:
                features += list(getattr(self.dataset, "targets", []) or [])
            except Exception:
                pass
        context = PipelineContext(self.runtime)
        vectors = build_vector_pipeline(context, features, self.dataset.group_by, stage=None)
        # Apply global postprocess by default
        stream = post_process(context, vectors)
        if limit is not None:
            stream = islice(stream, limit)
        return stream

    def iter_rows(
        self,
        *,
        limit: int | None = None,
        include_group: bool = True,
        group_format: GroupFormat = "mapping",
        group_column: str = "group",
        flatten_sequences: bool = False,
        include_targets: bool = False,
    ) -> Iterator[dict[str, Any]]:
        stream = self.stream(limit=limit, include_targets=include_targets)
        group_by = self.dataset.group_by

        def _rows() -> Iterator[dict[str, Any]]:
            for group_key, vector in stream:
                row: dict[str, Any] = {}
                if include_group:
                    row[group_column] = _normalize_group(
                        group_key, group_by, group_format)
                for feature_id, value in vector.values.items():
                    if flatten_sequences and isinstance(value, list):
                        for idx, item in enumerate(value):
                            row[f"{feature_id}[{idx}]"] = item
                    else:
                        row[feature_id] = value
                yield row

        return _rows()


def stream_vectors(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
    include_targets: bool = False,
) -> Iterator[tuple[Sequence[Any], Vector]]:
    """Yield ``(group_key, Vector)`` pairs for the configured project."""

    adapter = VectorAdapter.from_project(project_yaml)
    try:
        return adapter.stream(limit=limit, include_targets=include_targets)
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
    include_targets: bool = False,
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
            include_targets=include_targets,
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
    include_targets: bool = False,
) -> list[dict[str, Any]]:
    """Materialize :func:`iter_vector_rows` into a list for eager workflows."""

    iterator = iter_vector_rows(
        project_yaml,
        limit=limit,
        include_group=include_group,
        group_format=group_format,
        group_column=group_column,
        flatten_sequences=flatten_sequences,
        include_targets=include_targets,
        
    )
    return list(iterator)


def dataframe_from_vectors(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
    include_group: bool = True,
    group_format: GroupFormat = "mapping",
    group_column: str = "group",
    flatten_sequences: bool = False,
    include_targets: bool = False,
    open_stream: Callable[[str], Iterable[Any]] = open_source_stream,
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
        include_targets=include_targets,
        open_stream=open_stream,
    )
    return pd.DataFrame(rows)


def _resolve_columns(
    rows: list[Mapping[str, Any]],
    *,
    feature_columns: Sequence[str] | None,
    target_columns: Sequence[str] | None,
) -> tuple[list[str], list[str]]:
    if not rows:
        return list(feature_columns or []), list(target_columns or [])

    keys = list(rows[0].keys())
    if feature_columns is None:
        feature_columns = [k for k in keys if k not in (target_columns or ())]
    if target_columns is None:
        target_columns = []
    return list(feature_columns), list(target_columns)


def torch_dataset(
    project_yaml: str | Path,
    *,
    limit: int | None = None,
    feature_columns: Sequence[str] | None = None,
    target_columns: Sequence[str] | None = None,
    dtype: Any | None = None,
    device: Any | None = None,
    flatten_sequences: bool = False,
    include_targets: bool = False,
):
    """Build a torch.utils.data.Dataset that yields tensors from vectors.

    Torch is optional.  Install ``torch`` in the consuming project before using
    this helper.  Sequence-valued features can be flattened by passing
    ``flatten_sequences=True``; otherwise they are left as Python lists and
    ``torch.as_tensor`` must be able to coerce them into a tensor shape.
    """

    try:
        import torch
        from torch.utils.data import Dataset
    except ImportError as exc:  # pragma: no cover - exercised by runtime users
        raise RuntimeError(
            "torch is required for torch_dataset(); install torch in your project.",
        ) from exc

    rows = collect_vector_rows(
        project_yaml,
        limit=limit,
        include_group=False,
        flatten_sequences=flatten_sequences,
        include_targets=include_targets,
    )

    # Auto-derive target columns when include_targets is set and none provided
    if include_targets and target_columns is None:
        try:
            ds = load_dataset(Path(project_yaml), "vectors")
            target_columns = [cfg.id for cfg in (getattr(ds, "targets", []) or [])]
        except Exception:
            target_columns = None

    feature_cols, target_cols = _resolve_columns(
        rows,
        feature_columns=feature_columns,
        target_columns=target_columns,
    )

    class _VectorDataset(Dataset):
        def __len__(self) -> int:
            return len(rows)

        def __getitem__(self, idx: int):
            sample = rows[idx]
            features = torch.as_tensor(
                [sample[col] for col in feature_cols],
                dtype=dtype,
                device=device,
            ) if feature_cols else torch.tensor([], dtype=dtype, device=device)
            if not target_cols:
                return features
            targets = torch.as_tensor(
                [sample[col] for col in target_cols],
                dtype=dtype,
                device=device,
            )
            return features, targets

    return _VectorDataset()


__all__ = [
    "VectorAdapter",
    "collect_vector_rows",
    "dataframe_from_vectors",
    "iter_vector_rows",
    "stream_vectors",
    "torch_dataset",
]
