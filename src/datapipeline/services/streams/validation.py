from datapipeline.config.catalog import ContractConfig
from datapipeline.services.streams.join_plan import build_join_input_plans


def validate_stream_contracts(contracts: dict[str, ContractConfig]) -> None:
    for stream_id, spec in contracts.items():
        if spec.kind not in {"joined", "manual"}:
            continue

        refs = _input_refs(spec)
        missing = [ref for ref in refs.values() if ref not in contracts]
        if missing:
            raise ValueError(
                f"{spec.kind.capitalize()} stream '{stream_id}' references "
                f"unknown stream(s): {missing}"
            )
        if spec.kind == "manual":
            continue

        _validate_joined_stream(stream_id, spec, refs, contracts)


def contract_partition_by(
    contracts: dict[str, ContractConfig],
    spec: ContractConfig,
):
    if spec.kind != "joined":
        return spec.partition_by
    if spec.join is None:
        return None
    primary_ref = _input_refs(spec).get(spec.join.primary)
    if not primary_ref:
        return None
    return contracts[primary_ref].partition_by


def _validate_joined_stream(
    stream_id: str,
    spec: ContractConfig,
    refs: dict[str, str],
    contracts: dict[str, ContractConfig],
) -> None:
    join = spec.join
    if join is None:
        raise ValueError(f"Joined stream '{stream_id}' requires join config")

    build_join_input_plans(
        stream_id=stream_id,
        input_refs=refs,
        primary=join.primary,
        broadcast=set(join.broadcast),
        partition_by={ref: contracts[ref].partition_by for ref in refs.values()},
    )


def _input_refs(spec: ContractConfig) -> dict[str, str]:
    return {
        alias: ref
        for alias, ref in (
            ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])
        )
    }
