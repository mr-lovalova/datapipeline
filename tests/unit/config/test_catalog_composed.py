import pytest

from datapipeline.config.catalog import ContractConfig


def test_composed_contract_requires_mapper_entrypoint() -> None:
    with pytest.raises(ValueError, match="requires mapper.entrypoint"):
        ContractConfig.model_validate(
            {
                "kind": "composed",
                "id": "derived.sample",
                "inputs": ["a=stream.a"],
            }
        )


def test_composed_contract_rejects_duplicate_aliases() -> None:
    with pytest.raises(ValueError, match="duplicate alias"):
        ContractConfig.model_validate(
            {
                "kind": "composed",
                "id": "derived.sample",
                "inputs": ["x=stream.a", "x=stream.b"],
                "mapper": {"entrypoint": "compose", "args": {}},
            }
        )


def test_composed_contract_rejects_unknown_driver_alias() -> None:
    with pytest.raises(ValueError, match="driver must reference one of the declared input aliases"):
        ContractConfig.model_validate(
            {
                "kind": "composed",
                "id": "derived.sample",
                "inputs": ["x=stream.a", "y=stream.b"],
                "mapper": {"entrypoint": "compose", "args": {"driver": "missing"}},
            }
        )


def test_composed_contract_accepts_trimmed_alias_and_driver() -> None:
    spec = ContractConfig.model_validate(
        {
            "kind": "composed",
            "id": "derived.sample",
            "inputs": [" a = stream.a ", " b = stream.b "],
            "mapper": {"entrypoint": "compose", "args": {"driver": "a"}},
        }
    )

    parsed = [ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])]
    assert parsed == [("a", "stream.a"), ("b", "stream.b")]
