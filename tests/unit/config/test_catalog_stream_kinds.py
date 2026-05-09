import pytest

from datapipeline.config.catalog import ContractConfig


def test_composed_contract_kind_is_not_supported() -> None:
    with pytest.raises(ValueError):
        ContractConfig.model_validate(
            {
                "kind": "composed",
                "id": "derived.sample",
                "inputs": ["a=stream.a"],
                "mapper": {"entrypoint": "compose", "args": {}},
            }
        )


def test_manual_contract_requires_mapper_entrypoint() -> None:
    with pytest.raises(ValueError, match="manual contract requires mapper.entrypoint"):
        ContractConfig.model_validate(
            {
                "kind": "manual",
                "id": "derived.sample",
                "inputs": ["a=stream.a"],
            }
        )


def test_joined_contract_requires_join_config() -> None:
    with pytest.raises(ValueError, match="joined contract requires 'join'"):
        ContractConfig.model_validate(
            {
                "kind": "joined",
                "id": "derived.sample",
                "inputs": ["a=stream.a"],
                "mapper": {"entrypoint": "join", "args": {}},
            }
        )


def test_stream_contract_rejects_duplicate_aliases() -> None:
    with pytest.raises(ValueError, match="duplicate alias"):
        ContractConfig.model_validate(
            {
                "kind": "manual",
                "id": "derived.sample",
                "inputs": ["x=stream.a", "x=stream.b"],
                "mapper": {"entrypoint": "manual", "args": {}},
            }
        )


def test_manual_contract_rejects_unknown_driver_alias() -> None:
    with pytest.raises(
        ValueError,
        match="mapper.args.driver must reference one of the declared input aliases",
    ):
        ContractConfig.model_validate(
            {
                "kind": "manual",
                "id": "derived.sample",
                "inputs": ["x=stream.a", "y=stream.b"],
                "mapper": {"entrypoint": "manual", "args": {"driver": "missing"}},
            }
        )


def test_joined_contract_accepts_trimmed_aliases_and_join() -> None:
    spec = ContractConfig.model_validate(
        {
            "kind": "joined",
            "id": "derived.sample",
            "inputs": [" a = stream.a ", " b = stream.b "],
            "join": {"primary": "a", "broadcast": ["b"]},
            "mapper": {"entrypoint": "join", "args": {}},
        }
    )

    parsed = [ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])]
    assert parsed == [("a", "stream.a"), ("b", "stream.b")]
    assert spec.join is not None
    assert spec.join.primary == "a"
    assert spec.join.broadcast == ["b"]
