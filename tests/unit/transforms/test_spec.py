from dataclasses import FrozenInstanceError

import pytest

from datapipeline.transforms.spec import TransformSpec, parse_transform_spec


def test_parse_transform_spec_copies_mapping_params() -> None:
    params = {"field": "price"}

    spec = parse_transform_spec({"lag": params})
    params["field"] = "volume"

    assert spec == TransformSpec(name="lag", params={"field": "price"})


def test_parse_transform_spec_normalizes_null_params() -> None:
    assert parse_transform_spec({"dedupe": None}) == TransformSpec(
        name="dedupe",
        params={},
    )


def test_transform_spec_is_frozen() -> None:
    spec = TransformSpec(name="dedupe")

    with pytest.raises(FrozenInstanceError):
        spec.name = "where"  # type: ignore[misc]


@pytest.mark.parametrize(
    "value",
    [
        "dedupe",
        [],
        {},
        {"dedupe": {}, "where": {}},
    ],
)
def test_parse_transform_spec_rejects_non_single_mapping(value: object) -> None:
    with pytest.raises(ValueError, match="Transform must be a one-key mapping"):
        parse_transform_spec(value)


@pytest.mark.parametrize("params", ["price", 1, True, ["price"]])
def test_parse_transform_spec_rejects_non_mapping_params(params: object) -> None:
    with pytest.raises(ValueError, match="must be a mapping or null"):
        parse_transform_spec({"lag": params})


def test_parse_transform_spec_rejects_non_string_parameter_keys() -> None:
    with pytest.raises(ValueError, match="must be strings"):
        parse_transform_spec({"lag": {1: "price"}})


def test_parse_transform_spec_rejects_non_string_name() -> None:
    with pytest.raises(ValueError, match="Transform name must be a string"):
        parse_transform_spec({1: {}})


def test_parse_transform_spec_normalizes_name() -> None:
    assert parse_transform_spec({" lag ": {}}).name == "lag"


def test_parse_transform_spec_rejects_empty_name() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        parse_transform_spec({"  ": {}})
