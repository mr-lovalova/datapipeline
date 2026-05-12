from datapipeline.config.catalog import ContractConfig


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

    primary_ref = refs[join.primary]
    primary_partition = _partition_signature(contracts[primary_ref].partition_by)
    for alias, ref in refs.items():
        if alias == join.primary:
            continue
        partition = _partition_signature(contracts[ref].partition_by)
        if alias in join.broadcast:
            if set(partition).issubset(primary_partition):
                continue
        elif partition == primary_partition:
            continue
        raise ValueError(
            "Joined stream "
            f"'{stream_id}' has incompatible partition_by: "
            f"input '{alias}' partition_by={list(partition)} "
            f"does not match primary partition_by={list(primary_partition)}"
        )


def _input_refs(spec: ContractConfig) -> dict[str, str]:
    return {
        alias: ref
        for alias, ref in (
            ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])
        )
    }


def _partition_signature(value: str | list[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)
