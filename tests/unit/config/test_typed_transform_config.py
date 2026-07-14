import pytest
from pydantic import ValidationError

from datapipeline.config.catalog import DerivedStreamConfig, IngestConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.config.tasks import ScalerTask
from datapipeline.config.transforms import (
    CollapseConfig,
    DedupeConfig,
    EnsureCadenceConfig,
    EnsureTicksConfig,
    FillConfig,
    ForwardFillConfig,
    RollingConfig,
    ShiftTimeConfig,
)


def _ingest(**values: object) -> IngestConfig:
    return IngestConfig.model_validate(
        {
            "id": "prices.raw",
            "from": {"source": "prices"},
            "map": {"entrypoint": "identity"},
            **values,
        }
    )


def _stream(**values: object) -> DerivedStreamConfig:
    return DerivedStreamConfig.model_validate(
        {
            "id": "prices.daily",
            "from": {"stream": "prices.raw"},
            **values,
        }
    )


def test_catalog_parses_builtins_into_typed_configs() -> None:
    ingest = _ingest(record=[{"operation": "shift_time", "by": "1d"}])
    stream = _stream(
        stream=[
            {"operation": "dedupe"},
            {
                "operation": "rolling",
                "field": "close",
                "window": 20,
                "statistic": "mean",
            },
            {
                "operation": "fill",
                "field": "close",
                "window": 5,
                "statistic": "median",
            },
            {"operation": "forward_fill", "field": "close", "to": "close_asof"},
            {"operation": "collapse", "keep": "last"},
            {"operation": "ensure_ticks", "artifact": "model_grid"},
        ]
    )

    assert ingest.record == [ShiftTimeConfig(by="1d")]
    assert stream.stream == [
        DedupeConfig(),
        RollingConfig(field="close", window=20, statistic="mean"),
        FillConfig(field="close", window=5, statistic="median"),
        ForwardFillConfig(field="close", to="close_asof"),
        CollapseConfig(keep="last"),
        EnsureTicksConfig(artifact="model_grid"),
    ]
    assert stream.model_dump()["stream"] == [
        {"operation": "dedupe"},
        {
            "operation": "rolling",
            "field": "close",
            "window": 20,
            "to": None,
            "min_samples": None,
            "statistic": "mean",
        },
        {
            "operation": "fill",
            "field": "close",
            "window": 5,
            "statistic": "median",
            "to": None,
            "min_samples": 1,
        },
        {
            "operation": "forward_fill",
            "field": "close",
            "to": "close_asof",
        },
        {"operation": "collapse", "keep": "last"},
        {"operation": "ensure_ticks", "artifact": "model_grid"},
    ]


@pytest.mark.parametrize(
    "clause",
    [
        {"operation": "rolling", "field": "close", "windwo": 20},
        {"operation": "rolling", "field": "close", "window": 2.5},
        {
            "operation": "fill",
            "field": "close",
            "window": 5,
            "statistic": "typo",
        },
        {"operation": "fill", "field": "close", "method": "forward"},
        {"operation": "collapse", "keep": "mean"},
        {"operation": "collapse", "keep": "last", "field": "close"},
        {"operation": "floor_time", "cadence": "1h"},
        {"operation": "granularity", "field": "close", "mode": "last"},
        {"operation": "unknown"},
    ],
)
def test_stream_config_rejects_invalid_builtin_parameters(clause: object) -> None:
    with pytest.raises(ValidationError):
        _stream(stream=[clause])


def test_stream_config_rejects_a_record_only_transform_model() -> None:
    with pytest.raises(ValidationError, match="shift_time"):
        _stream(stream=[ShiftTimeConfig(by="1h")])


@pytest.mark.parametrize("cadence", [None, "", "ticks", "0m", "-1h"])
def test_ensure_cadence_requires_a_positive_duration(cadence: object) -> None:
    with pytest.raises(ValidationError):
        _stream(stream=[{"operation": "ensure_cadence", "cadence": cadence}])

    assert EnsureCadenceConfig(cadence="1h").cadence == "1h"


def test_stream_config_rejects_removed_debug_stage() -> None:
    with pytest.raises(ValidationError, match="debug"):
        _stream(debug=[{"lint": {"mode": "warn"}}])


@pytest.mark.parametrize(
    "sequence",
    [
        {"size": True},
        {"size": 1.5},
        {"size": "2"},
        {"size": 2, "strdie": 1},
    ],
)
def test_sequence_config_is_strict(sequence: object) -> None:
    with pytest.raises(ValidationError):
        FeatureRecordConfig.model_validate(
            {
                "id": "close",
                "stream": "prices.daily",
                "field": "close",
                "sequence": sequence,
            }
        )


def test_feature_config_uses_explicit_scale_and_sequence_models() -> None:
    config = FeatureRecordConfig.model_validate(
        {
            "id": "close",
            "stream": "prices.daily",
            "field": "close",
            "scale": True,
            "sequence": {"size": 20, "stride": 5},
        }
    )

    assert config.scale is True
    assert config.sequence == SequenceConfig(size=20, stride=5)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("with_mean", "true"),
        ("with_std", 1),
        ("epsilon", 0),
        ("epsilon", float("inf")),
    ],
)
def test_scaler_build_options_are_strict(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        ScalerTask.model_validate({"id": "scaler", field: value})


@pytest.mark.parametrize(
    "folds",
    [
        [{"fit": "train", "apply": ["test"]}],
        [{"fit": [1], "apply": ["test"]}],
        [{"fit": [""], "apply": ["test"]}],
        [{"fit": [" train"], "apply": ["test"]}],
    ],
)
def test_scaler_fold_labels_are_strict(folds: object) -> None:
    with pytest.raises(ValidationError):
        ScalerTask.model_validate({"id": "scaler", "folds": folds})


def test_scaler_split_label_must_not_be_blank() -> None:
    with pytest.raises(ValidationError):
        ScalerTask.model_validate({"id": "scaler", "split_label": "  "})
