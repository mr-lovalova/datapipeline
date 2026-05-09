import pytest

from datapipeline.config.catalog import ContractConfig
from datapipeline.services.bootstrap.core import (
    _load_canonical_streams,
    _validate_stream_contracts,
)


def _ingest(stream_id: str, partition_by=None) -> ContractConfig:
    return ContractConfig.model_validate(
        {
            "kind": "ingest",
            "id": stream_id,
            "source": "source.alias",
            "partition_by": partition_by,
        }
    )


def _manual(
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
            "kind": "manual",
            "id": stream_id,
            "inputs": inputs,
            "partition_by": partition_by,
            "mapper": {"entrypoint": "manual", "args": args},
        }
    )


def _joined(
    stream_id: str,
    inputs: list[str],
    primary: str,
    broadcast: list[str] | None = None,
) -> ContractConfig:
    return ContractConfig.model_validate(
        {
            "kind": "joined",
            "id": stream_id,
            "inputs": inputs,
            "join": {
                "primary": primary,
                "on": "time",
                "mode": "inner",
                "broadcast": list(broadcast or []),
            },
            "mapper": {"entrypoint": "join", "args": {}},
        }
    )


def test_validate_stream_contracts_rejects_missing_refs() -> None:
    contracts = {
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="references unknown stream"):
        _validate_stream_contracts(contracts)


def test_validate_stream_contracts_allows_manual_partition_mismatch() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }

    _validate_stream_contracts(contracts)


def test_validate_stream_contracts_rejects_joined_unrelated_partitions() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }
    with pytest.raises(ValueError, match="incompatible partition_by"):
        _validate_stream_contracts(contracts)


def test_validate_stream_contracts_accepts_joined_matching_partitions() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="station"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }

    _validate_stream_contracts(contracts)


def test_validate_stream_contracts_accepts_joined_broadcast_subset() -> None:
    contracts = {
        "stream.a": _ingest("stream.a", partition_by=["ticker", "horizon"]),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
            broadcast=["y"],
        ),
    }

    _validate_stream_contracts(contracts)


def test_load_canonical_streams_rejects_composed_kind(tmp_path) -> None:
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  streams: ./contracts",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (contracts_dir / "old.yaml").write_text(
        "\n".join(
            [
                "kind: composed",
                "id: old",
                "inputs:",
                "  - stream.a",
                "mapper:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="composed.*no longer supported"):
        _load_canonical_streams(project_yaml, {})
