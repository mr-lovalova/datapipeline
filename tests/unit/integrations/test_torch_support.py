import json
from types import SimpleNamespace

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.integrations.ml.torch_support import _resolve_columns, _schema_columns
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_SCHEMA


def test_schema_columns_include_partitioned_target_ids(tmp_path) -> None:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    schema_path = artifacts_root / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "features": [
                    {"id": "closing_price__@ticker:AACB", "kind": "scalar"},
                    {"id": "closing_price__@ticker:ZWS", "kind": "scalar"},
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
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_SCHEMA, relative_path="schema.json")

    feature_columns, target_columns = _schema_columns(SimpleNamespace(runtime=runtime))

    assert feature_columns == [
        "closing_price__@ticker:AACB",
        "closing_price__@ticker:ZWS",
    ]
    assert target_columns == [
        "fwd_excess_return_126d_vs_spy__@ticker:AACB",
        "fwd_excess_return_126d_vs_spy__@ticker:ZWS",
    ]


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
