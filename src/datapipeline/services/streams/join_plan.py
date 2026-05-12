from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class JoinInputPlan:
    stream_id: str
    fields: tuple[str, ...]
    broadcast: bool


def build_join_input_plans(
    stream_id: str,
    input_refs: Mapping[str, str],
    primary: str,
    broadcast: set[str],
    partition_by,
) -> dict[str, JoinInputPlan]:
    if primary not in input_refs:
        raise ValueError(
            f"Joined stream '{stream_id}' join.primary '{primary}' "
            f"is not an input alias"
        )

    primary_fields = _partition_tuple(partition_by.get(input_refs[primary]))
    plans: dict[str, JoinInputPlan] = {}
    for alias, ref in input_refs.items():
        if alias == primary:
            continue
        fields = _compatible_fields(
            stream_id,
            alias,
            primary_fields,
            _partition_tuple(partition_by.get(ref)),
            broadcast=alias in broadcast,
        )
        plans[alias] = JoinInputPlan(
            stream_id=ref,
            fields=fields,
            broadcast=alias in broadcast,
        )
    return plans


def _partition_tuple(value: str | list[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _compatible_fields(
    stream_id: str,
    alias: str,
    primary_fields: tuple[str, ...],
    other_fields: tuple[str, ...],
    broadcast: bool,
) -> tuple[str, ...]:
    if broadcast and set(other_fields).issubset(primary_fields):
        return other_fields
    if not broadcast and other_fields == primary_fields:
        return primary_fields
    raise ValueError(
        "Joined stream "
        f"'{stream_id}' has incompatible partition_by: "
        f"input '{alias}' partition_by={list(other_fields)} "
        f"does not match primary partition_by={list(primary_fields)}"
    )
