from collections.abc import Sequence
from pathlib import Path
from typing import Any, Mapping

from datapipeline.artifacts.models import VectorSchemaEntry
from datapipeline.artifacts.registry import VECTOR_SCHEMA_SPEC
from datapipeline.execution.context import PipelineContext

from .adapter import VectorAdapter


def _resolve_columns(
    rows: Sequence[Mapping[str, Any]],
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


def _schema_entry_columns(
    entries: Sequence[VectorSchemaEntry],
    flatten_sequences: bool,
) -> list[str]:
    columns: list[str] = []
    for entry in entries:
        if flatten_sequences and entry.kind == "list":
            assert entry.cadence is not None
            columns.extend(
                f"{entry.id}[{index}]" for index in range(entry.cadence.target)
            )
        else:
            columns.append(entry.id)
    return columns


def _schema_columns(
    adapter: VectorAdapter,
    flatten_sequences: bool = False,
) -> tuple[list[str], list[str]]:
    context = PipelineContext(adapter.runtime)
    schema = context.require_artifact(VECTOR_SCHEMA_SPEC)
    return (
        _schema_entry_columns(schema.features, flatten_sequences),
        _schema_entry_columns(schema.targets, flatten_sequences),
    )


def torch_dataset(
    project_yaml: str | Path,
    *,
    output_id: str | None = None,
    limit: int | None = None,
    feature_columns: Sequence[str] | None = None,
    target_columns: Sequence[str] | None = None,
    dtype: Any | None = None,
    device: Any | None = None,
    flatten_sequences: bool = False,
):
    """Build a torch.utils.data.Dataset that yields tensors from vectors."""

    try:
        import torch  # type: ignore[import-not-found]
        from torch.utils.data import Dataset  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised by runtime users
        raise RuntimeError(
            "torch is required for torch_dataset(); install torch in your project.",
        ) from exc

    adapter = VectorAdapter.from_project(project_yaml, output_id=output_id)
    rows = list(
        adapter.iter_rows(
            limit=limit,
            include_group=False,
            flatten_sequences=flatten_sequences,
        )
    )

    schema_feature_columns, schema_target_columns = _schema_columns(
        adapter,
        flatten_sequences,
    )
    if feature_columns is None and schema_feature_columns:
        feature_columns = schema_feature_columns
    if target_columns is None:
        target_columns = schema_target_columns or [
            cfg.id for cfg in getattr(adapter.dataset, "targets", []) or []
        ]

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
            features = (
                torch.as_tensor(
                    [sample[col] for col in feature_cols],
                    dtype=dtype,
                    device=device,
                )
                if feature_cols
                else torch.tensor([], dtype=dtype, device=device)
            )
            if not target_cols:
                return features
            targets = torch.as_tensor(
                [sample[col] for col in target_cols],
                dtype=dtype,
                device=device,
            )
            return features, targets

    return _VectorDataset()
