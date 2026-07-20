import sys
from types import ModuleType, SimpleNamespace

import datapipeline.integrations.ml.torch_support as torch_support
from datapipeline.config.dataset.dataset import DatasetConfig, SampleConfig
from datapipeline.integrations.ml.torch_support import _resolve_columns


def test_torch_dataset_reads_flattened_sequence_columns(
    tmp_path,
    monkeypatch,
) -> None:
    adapter = SimpleNamespace(
        dataset=DatasetConfig(sample=SampleConfig(cadence="1h")),
        row_columns=lambda flatten_sequences=False: (
            ["history[0]", "history[1]"],
            [],
        ),
        iter_rows=lambda **_kwargs: [{"history[0]": 1.0, "history[1]": 2.0}],
    )
    monkeypatch.setattr(
        torch_support.VectorAdapter,
        "from_project",
        lambda _path, output_id=None: adapter,
    )
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
