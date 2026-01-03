from __future__ import annotations

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.utils import load as dp_load


def test_foreach_interpolates_and_injects():
    dp_load._EP_OVERRIDES[("datapipeline.loaders", "tests.foreach.dummy")] = (
        "tests.loaders.dummy:DummyLoader"
    )

    loader = ForeachLoader(
        foreach={"symbol": ["AAPL", "MSFT"]},
        inject_field="symbol",
        loader={
            "entrypoint": "tests.foreach.dummy",
            "args": {"value": "${symbol}", "rows": 2},
        },
    )

    rows = list(loader.load())
    assert rows == [
        {"value": "AAPL", "idx": 0, "symbol": "AAPL"},
        {"value": "AAPL", "idx": 1, "symbol": "AAPL"},
        {"value": "MSFT", "idx": 0, "symbol": "MSFT"},
        {"value": "MSFT", "idx": 1, "symbol": "MSFT"},
    ]
