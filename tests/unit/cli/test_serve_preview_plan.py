from types import SimpleNamespace

from datapipeline.operations.runtime.pipeline import PreviewNode, _preview_plan


def _cfg(id_: str, record_stream: str):
    return SimpleNamespace(id=id_, record_stream=record_stream)


def test_preview_plan_dedupes_shared_streams_for_early_preview_indices() -> None:
    preview_cfgs = [
        _cfg("closing_price", "equity.ohlcv"),
        _cfg("opening_price", "equity.ohlcv"),
        _cfg("linear_time", "time.ticks.linear"),
    ]

    plan = _preview_plan(
        preview_cfgs,
        PreviewNode("open_stream", "record"),
    )

    assert plan == [
        ("equity.ohlcv", preview_cfgs[0]),
        ("time.ticks.linear", preview_cfgs[2]),
    ]


def test_preview_plan_keeps_feature_scope_for_feature_preview_indices() -> None:
    preview_cfgs = [
        _cfg("closing_price", "equity.ohlcv"),
        _cfg("opening_price", "equity.ohlcv"),
    ]

    plan = _preview_plan(
        preview_cfgs,
        PreviewNode("build_feature_stream", "feature"),
    )

    assert plan == [
        ("closing_price", preview_cfgs[0]),
        ("opening_price", preview_cfgs[1]),
    ]
