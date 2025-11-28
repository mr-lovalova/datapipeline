from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Mapping

from datapipeline.config.dataset.loader import load_dataset

from .rows import collect_vector_rows


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
):
    """Build a torch.utils.data.Dataset that yields tensors from vectors."""

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
    )

    if target_columns is None:
        try:
            ds = load_dataset(Path(project_yaml), "vectors")
            target_columns = [cfg.id for cfg in getattr(ds, "targets", []) or []]
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


__all__ = ["torch_dataset"]
