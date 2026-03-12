import pytest

from datapipeline.config.catalog import ContractConfig
from datapipeline.services.bootstrap.core import _validate_composed_contracts


def _ingest(stream_id: str, partition_by=None) -> ContractConfig:
    return ContractConfig.model_validate(
        {
            "kind": "ingest",
            "id": stream_id,
            "source": "source.alias",
            "partition_by": partition_by,
        }
    )


def _composed(
    stream_id: str,
    inputs: list[str],
    partition_by=None,
    driver: str | None = None,
) -> ContractConfig:
    args = {}
    if driver is not None:
        args["driver"] = driver
    return ContractConfig.model_validate(
        {
            "kind": "composed",
            "id": stream_id,
            "inputs": inputs,
            "partition_by": partition_by,
            "mapper": {"entrypoint": "compose", "args": args},
        }
    )


def test_validate_composed_contracts_rejects_missing_refs() -> None:
    contracts = {
        "derived": _composed(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="references unknown stream"):
        _validate_composed_contracts(contracts)


def test_validate_composed_contracts_rejects_input_partition_mismatch() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _composed(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="incompatible partition_by settings"):
        _validate_composed_contracts(contracts)


def test_validate_composed_contracts_rejects_composed_partition_mismatch() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="station"),
        "derived": _composed(
            "derived",
            ["x=stream.a", "y=stream.b"],
            partition_by="ticker",
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="does not match inputs partition_by"):
        _validate_composed_contracts(contracts)


def test_validate_composed_contracts_accepts_compatible_inputs() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="station"),
        "derived": _composed(
            "derived",
            ["x=stream.a", "y=stream.b"],
            partition_by="station",
            driver="x",
        ),
    }

    _validate_composed_contracts(contracts)
