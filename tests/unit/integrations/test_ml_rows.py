import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import datapipeline.integrations.ml.adapter as adapter_module
import datapipeline.integrations.ml.rows as rows_module
from datapipeline.artifacts.specs import VECTOR_SCHEMA
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.split import (
    DatasetFold,
    TimeInterval,
    TimeSplitConfig,
)
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.integrations.ml.adapter import VectorAdapter
from datapipeline.runtime import Runtime


class _BrokenAdapter:
    def stream(self, **_kwargs):
        raise ValueError("invalid vector configuration")
        yield

    def iter_rows(self, **_kwargs):
        raise ValueError("invalid row configuration")
        yield


def test_ml_row_helpers_propagate_configuration_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        rows_module,
        "VectorAdapter",
        SimpleNamespace(
            from_project=lambda _path, output_id=None: _BrokenAdapter(),
        ),
    )

    with pytest.raises(ValueError, match="invalid vector configuration"):
        list(
            rows_module.stream_vectors(
                "project.yaml",
                output_id="holdout.train",
            )
        )

    with pytest.raises(ValueError, match="invalid row configuration"):
        list(
            rows_module.iter_vector_rows(
                "project.yaml",
                output_id="holdout.validation",
            )
        )


def test_vector_adapter_closes_dataset_pipeline(monkeypatch, tmp_path: Path) -> None:
    closed = False

    def samples():
        nonlocal closed
        try:
            yield Sample(key=("first",), features=Vector({"value": 1}))
            yield Sample(key=("second",), features=Vector({"value": 2}))
        finally:
            closed = True

    monkeypatch.setattr(
        adapter_module,
        "run_dataset_pipeline",
        lambda *_args, **_kwargs: samples(),
    )
    dataset = _dataset()
    adapter = VectorAdapter(dataset=dataset, runtime=_runtime(tmp_path, dataset))

    vectors = adapter.stream()
    assert next(vectors) == (("first",), Vector({"value": 1}))
    vectors.close()

    assert closed


def test_vector_adapter_requires_output_for_split_dataset(tmp_path: Path) -> None:
    dataset = _dataset(split=_split())
    with pytest.raises(
        ValueError,
        match=(
            "pass output_id with one of: "
            "walk_0.train, walk_0.validation, "
            "walk_1.train, walk_1.validation"
        ),
    ):
        VectorAdapter(dataset=dataset, runtime=_runtime(tmp_path, dataset))


def test_vector_adapter_rejects_unknown_split_output(tmp_path: Path) -> None:
    dataset = _dataset(split=_split())
    with pytest.raises(
        ValueError,
        match=(
            "Dataset output 'walk_2.train' is not defined; choose one of: "
            "walk_0.train, walk_0.validation, "
            "walk_1.train, walk_1.validation"
        ),
    ):
        VectorAdapter(
            dataset=dataset,
            runtime=_runtime(tmp_path, dataset),
            output_id="walk_2.train",
        )


def test_vector_adapter_rejects_output_for_unsplit_dataset(tmp_path: Path) -> None:
    dataset = _dataset()
    with pytest.raises(
        ValueError,
        match="output_id is only valid when dataset.split is configured",
    ):
        VectorAdapter(
            dataset=dataset,
            runtime=_runtime(tmp_path, dataset),
            output_id="holdout.train",
        )


def test_vector_adapter_runs_selected_fold_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls = []

    def run_fold(
        _context,
        _features,
        _cadence,
        fold,
        labels,
        **_kwargs,
    ):
        calls.append((fold.id, tuple(labels)))
        yield Sample(key=("selected",), features=Vector({"value": -1.0}))

    monkeypatch.setattr(adapter_module, "run_fold_dataset_pipeline", run_fold)
    dataset = _dataset(split=_split(), scale=True)
    adapter = VectorAdapter(
        dataset=dataset,
        runtime=_runtime(tmp_path, dataset),
        output_id="walk_1.train",
    )

    assert list(adapter.stream()) == [(("selected",), Vector({"value": -1.0}))]
    assert calls == [("walk_1", ("train_0", "validation_0", "train_1"))]


def test_vector_adapter_scales_unsplit_dataset_when_configured(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls = []

    def run_scaled(*_args, **_kwargs):
        calls.append("scaled")
        yield Sample(key=("sample",), features=Vector({"value": 0.0}))

    monkeypatch.setattr(adapter_module, "run_scaled_dataset_pipeline", run_scaled)
    dataset = _dataset(scale=True)
    adapter = VectorAdapter(
        dataset=dataset,
        runtime=_runtime(tmp_path, dataset),
    )

    assert list(adapter.stream()) == [(("sample",), Vector({"value": 0.0}))]
    assert calls == ["scaled"]


def test_vector_adapter_row_columns_expand_sequences(tmp_path: Path) -> None:
    runtime = _runtime_with_schema(
        tmp_path,
        {
            "schema_version": 2,
            "features": [
                {"id": "closing_price", "kind": "scalar"},
                {
                    "id": "price_history",
                    "kind": "list",
                    "cadence": {"target": 3},
                },
            ],
            "targets": [
                {
                    "id": "future_returns",
                    "kind": "list",
                    "cadence": {"target": 2},
                }
            ],
        },
    )
    adapter = VectorAdapter(dataset=runtime.dataset, runtime=runtime)

    assert adapter.row_columns(flatten_sequences=True) == (
        [
            "closing_price",
            "price_history[0]",
            "price_history[1]",
            "price_history[2]",
        ],
        ["future_returns[0]", "future_returns[1]"],
    )


def test_vector_adapter_rejects_flattened_row_column_collisions(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_schema(
        tmp_path,
        {
            "schema_version": 2,
            "features": [
                {"id": "history[0]", "kind": "scalar"},
                {
                    "id": "history",
                    "kind": "list",
                    "cadence": {"target": 1},
                },
            ],
            "targets": [],
        },
    )
    adapter = VectorAdapter(dataset=runtime.dataset, runtime=runtime)

    with pytest.raises(ValueError, match="Flattened row column 'history\\[0\\]'"):
        list(adapter.iter_rows(flatten_sequences=True))


def test_vector_adapter_ignores_columns_removed_by_postprocess(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = _runtime_with_schema(
        tmp_path,
        {
            "schema_version": 2,
            "features": [
                {"id": "history[0]", "kind": "scalar"},
                {
                    "id": "history",
                    "kind": "list",
                    "cadence": {"target": 1},
                },
            ],
            "targets": [],
        },
    )
    monkeypatch.setattr(
        adapter_module,
        "build_postprocess_plan",
        lambda _context: SimpleNamespace(feature_ids=("history",), target_ids=()),
    )
    adapter = VectorAdapter(dataset=runtime.dataset, runtime=runtime)

    assert adapter.row_columns(flatten_sequences=True) == (["history[0]"], [])


def _dataset(
    *,
    split: TimeSplitConfig | None = None,
    scale: bool = False,
) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        sample=SampleConfig(cadence="1d"),
        features=[
            FeatureRecordConfig(
                id="value",
                stream="records",
                field="value",
                scale=scale,
            )
        ],
        split=split,
    )


def _split() -> TimeSplitConfig:
    return TimeSplitConfig(
        intervals=[
            TimeInterval(
                id="train_0",
                until="2024-01-02T00:00:00Z",
            ),
            TimeInterval(
                id="validation_0",
                until="2024-01-03T00:00:00Z",
            ),
            TimeInterval(
                id="train_1",
                until="2024-01-04T00:00:00Z",
            ),
            TimeInterval(id="validation_1"),
        ],
        folds=[
            DatasetFold(
                id="walk_0",
                train=["train_0"],
                validation=["validation_0"],
            ),
            DatasetFold(
                id="walk_1",
                train=["train_0", "validation_0", "train_1"],
                validation=["validation_1"],
            ),
        ],
    )


def _runtime(
    tmp_path: Path,
    dataset: FeatureDatasetConfig,
) -> Runtime:
    return Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=dataset,
    )


def _runtime_with_schema(
    tmp_path: Path,
    document: dict[str, object],
) -> Runtime:
    runtime = _runtime(
        tmp_path,
        FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    runtime.artifacts_root.mkdir()
    schema_path = runtime.artifacts_root / "schema.json"
    schema_path.write_text(json.dumps(document), encoding="utf-8")
    runtime.artifacts.register(VECTOR_SCHEMA, relative_path="schema.json")
    return runtime
