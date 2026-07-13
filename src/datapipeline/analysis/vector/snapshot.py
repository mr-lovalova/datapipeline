from collections import defaultdict
from typing import Any, Hashable

from .collector import VectorStatsCollector

_SNAPSHOT_SCHEMA_VERSION = 2

_STATUS_TO_CODE = {
    "absent": 0,
    "present": 1,
    "null": 2,
}
_CODE_TO_STATUS = {code: status for status, code in _STATUS_TO_CODE.items()}


def _group_key(value: Hashable) -> str:
    return VectorStatsCollector._format_group_key(value)


def _encode_status(status: str) -> int:
    code = _STATUS_TO_CODE.get(status)
    if code is None:
        raise ValueError(f"Unknown status '{status}' in stats snapshot.")
    return code


def _decode_status(code: Any) -> str:
    status = _CODE_TO_STATUS.get(code)
    if status is None:
        raise ValueError(f"Unknown status code '{code}' in stats snapshot.")
    return status


def _serialize_status_map(
    values: dict[Hashable, dict[str, str]],
) -> dict[str, dict[str, int]]:
    return {
        _group_key(group): {
            identifier: _encode_status(status)
            for identifier, status in statuses.items()
        }
        for group, statuses in values.items()
    }


def _serialize_sub_map(
    values: dict[Hashable, dict[str, list[str]]],
) -> dict[str, dict[str, list[int]]]:
    return {
        _group_key(group): {
            identifier: [_encode_status(status) for status in statuses]
            for identifier, statuses in sub.items()
        }
        for group, sub in values.items()
    }


def _deserialize_status_map(values: dict[str, Any]) -> defaultdict[str, dict[str, str]]:
    return defaultdict(
        dict,
        {
            group: {
                str(identifier): _decode_status(code)
                for identifier, code in statuses.items()
            }
            for group, statuses in values.items()
            if isinstance(statuses, dict)
        },
    )


def _deserialize_sub_map(
    values: dict[str, Any],
) -> defaultdict[str, dict[str, list[str]]]:
    return defaultdict(
        dict,
        {
            group: {
                str(identifier): [_decode_status(code) for code in statuses]
                for identifier, statuses in sub.items()
                if isinstance(statuses, list)
            }
            for group, sub in values.items()
            if isinstance(sub, dict)
        },
    )


def snapshot_from_collector(collector: VectorStatsCollector) -> dict[str, Any]:
    return {
        "schema_version": _SNAPSHOT_SCHEMA_VERSION,
        "match_partition": collector.match_partition,
        "sample_limit": collector.sample_limit,
        "total_vectors": collector.total_vectors,
        "empty_vectors": collector.empty_vectors,
        "group_feature_status": _serialize_status_map(collector.group_feature_status),
        "group_partition_status": _serialize_status_map(
            collector.group_partition_status
        ),
        "group_feature_sub": _serialize_sub_map(collector.group_feature_sub),
        "group_partition_sub": _serialize_sub_map(collector.group_partition_sub),
    }


def _require_schema_version(snapshot: dict[str, Any]) -> None:
    version = snapshot.get("schema_version")
    if version != _SNAPSHOT_SCHEMA_VERSION:
        raise ValueError(
            "Unsupported vector stats snapshot schema version "
            f"'{version}'. Expected '{_SNAPSHOT_SCHEMA_VERSION}'. Rebuild stats."
        )


def collector_from_snapshot(
    snapshot: dict[str, Any],
    *,
    expected_feature_ids: list[str] | None,
    schema_meta: dict[str, dict[str, Any]] | None,
    matrix_output: str | None = None,
    matrix_format: str = "html",
) -> VectorStatsCollector:
    _require_schema_version(snapshot)
    collector = VectorStatsCollector(
        expected_feature_ids=expected_feature_ids,
        match_partition=snapshot.get("match_partition", "base"),
        schema_meta=schema_meta,
        sample_limit=snapshot.get("sample_limit", 5),
        matrix_output=matrix_output,
        matrix_format=matrix_format,
    )

    collector.total_vectors = snapshot.get("total_vectors", 0)
    collector.empty_vectors = snapshot.get("empty_vectors", 0)
    collector.group_feature_status = _deserialize_status_map(
        snapshot.get("group_feature_status", {})
    )
    collector.group_partition_status = _deserialize_status_map(
        snapshot.get("group_partition_status", {})
    )
    collector.group_feature_sub = _deserialize_sub_map(
        snapshot.get("group_feature_sub", {})
    )
    collector.group_partition_sub = _deserialize_sub_map(
        snapshot.get("group_partition_sub", {})
    )
    collector.rebuild_from_group_maps()
    return collector
