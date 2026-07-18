import json
import sys
from types import ModuleType, SimpleNamespace

import datapipeline.integrations.ml.torch_support as torch_support
from datapipeline.artifacts.specs import VECTOR_SCHEMA
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.integrations.ml.torch_support import _resolve_columns, _schema_columns
from datapipeline.runtime import Runtime


def _runtime_with_schema(tmp_path, document: dict[str, object]) -> Runtime:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "schema_version: 2\nartifact_revision: 1\n", encoding="utf-8"
    )
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    schema_path = artifacts_root / "schema.json"
    schema_path.write_text(json.dumps(document), encoding="utf-8")
    runtime.artifacts.register(VECTOR_SCHEMA, relative_path="schema.json")
    return runtime


def test_schema_columns_preserve_ids_and_expand_sequences(tmp_path) -> None:
    runtime = _runtime_with_schema(
        tmp_path,
        {
            "schema_version": 2,
            "features": [
                {"id": "closing_price__@ticker:AACB", "kind": "scalar"},
                {"id": "closing_price__@ticker:ZWS", "kind": "scalar"},
                {
                    "id": "price_history",
                    "kind": "list",
                    "cadence": {"target": 3},
                },
            ],
            "targets": [
                {
                    "id": "fwd_excess_return_126d_vs_spy__@ticker:AACB",
                    "kind": "scalar",
                },
                {
                    "id": "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
                    "kind": "scalar",
                },
                {
                    "id": "future_returns",
                    "kind": "list",
                    "cadence": {"target": 2},
                },
            ],
        },
    )

    feature_columns, target_columns = _schema_columns(SimpleNamespace(runtime=runtime))

    assert feature_columns == [
        "closing_price__@ticker:AACB",
        "closing_price__@ticker:ZWS",
        "price_history",
    ]
    assert target_columns == [
        "fwd_excess_return_126d_vs_spy__@ticker:AACB",
        "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
        "future_returns",
    ]
    flattened_features, flattened_targets = _schema_columns(
        SimpleNamespace(runtime=runtime),
        flatten_sequences=True,
    )

    assert flattened_features == [
        "closing_price__@ticker:AACB",
        "closing_price__@ticker:ZWS",
        "price_history[0]",
        "price_history[1]",
        "price_history[2]",
    ]
    assert flattened_targets == [
        "fwd_excess_return_126d_vs_spy__@ticker:AACB",
        "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
        "future_returns[0]",
        "future_returns[1]",
    ]


def test_torch_dataset_reads_flattened_sequence_columns(
    tmp_path,
    monkeypatch,
) -> None:
    adapter = SimpleNamespace(
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        iter_rows=lambda **_kwargs: [{"history[0]": 1.0, "history[1]": 2.0}],
    )
    monkeypatch.setattr(
        torch_support.VectorAdapter,
        "from_project",
        lambda _path, output_id=None: adapter,
    )
    flatten_options = []

    def schema_columns(_adapter, flatten_sequences=False):
        flatten_options.append(flatten_sequences)
        return ["history[0]", "history[1]"], []

    monkeypatch.setattr(torch_support, "_schema_columns", schema_columns)

    torch = ModuleType("torch")
    torch.as_tensor = lambda values, **_kwargs: tuple(values)
    torch.tensor = lambda values, **_kwargs: tuple(values)
    torch_utils = ModuleType("torch.utils")
    torch_data = ModuleType("torch.utils.data")
    torch_data.Dataset = object
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "torch.utils", torch_utils)
    monkeypatch.setitem(sys.modules, "torch.utils.data", torch_data)

    dataset = torch_support.torch_dataset(
        tmp_path / "project.yaml",
        flatten_sequences=True,
    )

    assert dataset[0] == (1.0, 2.0)
    assert flatten_options == [True]


def test_resolve_columns_excludes_partitioned_targets_from_features() -> None:
    rows = [
        {
            "closing_price__@ticker:AACB": 1.0,
            "closing_price__@ticker:ZWS": 2.0,
            "fwd_excess_return_126d_vs_spy__@ticker:AACB": 0.1,
            "fwd_excess_return_126d_vs_spy__@ticker:ZWS": 0.2,
        }
    ]

    feature_columns, target_columns = _resolve_columns(
        rows,
        feature_columns=None,
        target_columns=[
            "fwd_excess_return_126d_vs_spy__@ticker:AACB",
            "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
        ],
    )

    assert feature_columns == [
        "closing_price__@ticker:AACB",
        "closing_price__@ticker:ZWS",
    ]
    assert target_columns == [
        "fwd_excess_return_126d_vs_spy__@ticker:AACB",
        "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
    ]
