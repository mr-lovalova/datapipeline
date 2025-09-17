from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Optional, Tuple

from datapipeline.config.dataset.feature import BaseRecordConfig
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import Record
from datapipeline.pipeline.utils.keygen import RecordKeyGenerator
from datapipeline.plugins import FILTERS_EP, TRANSFORMS_EP
from datapipeline.utils.load import load_ep


def _extract_single_pair(clause: Mapping[str, Any], kind: str) -> Tuple[str, Any]:
    """Validate that *clause* is a one-key mapping and return that pair."""

    if not isinstance(clause, Mapping) or len(clause) != 1:
        raise TypeError(f"{kind} must be one-key mapping, got: {clause!r}")
    return next(iter(clause.items()))


def _call_with_params(fn: Callable, stream: Iterator[Any], params: Any) -> Iterator[Any]:
    """Invoke an entry-point callable with optional params semantics."""

    if params is None:
        return fn(stream)
    if isinstance(params, (list, tuple)):
        return fn(stream, *params)
    if isinstance(params, Mapping):
        return fn(stream, **params)
    return fn(stream, params)


def _instantiate_entry_point(cls: Callable[..., Any], params: Any) -> Any:
    """Instantiate a transform class with parameters from the config."""

    if params is None:
        return cls()
    if isinstance(params, Mapping):
        return cls(**params)
    if isinstance(params, (list, tuple)):
        return cls(*params)
    return cls(params)


def filter_record_stream(
    stream: Iterator[Any],
    filters: Optional[Sequence[Mapping[str, Any]]],
) -> Iterator[Any]:
    """Apply all configured filters to the record stream."""

    for clause in filters or ():
        name, mapping = _extract_single_pair(clause, "Filter")
        field, target = _extract_single_pair(mapping, f"Filter '{name}'")
        fn: Callable = load_ep(group=FILTERS_EP, name=name)
        stream = fn(stream, field, target)
    return stream


def transform_record_stream(
    stream: Iterator[Any],
    transforms: Optional[Sequence[Mapping[str, Any]]],
) -> Iterator[Any]:
    """Apply configured record-level transforms to the stream."""

    for clause in transforms or ():
        name, params = _extract_single_pair(clause, "Transform")
        fn: Callable = load_ep(group=TRANSFORMS_EP, name=name)
        stream = _call_with_params(fn, stream, params)
    return stream


def _instantiate_transforms(
    group: str, clauses: Optional[Sequence[Mapping[str, Any]]]
) -> list[Any]:
    """Instantiate configured transform classes for later application."""

    instances: list[Any] = []
    for clause in clauses or ():
        name, params = _extract_single_pair(clause, "Transform")
        cls = load_ep(group=group, name=name)
        instances.append(_instantiate_entry_point(cls, params))
    return instances


def transform_feature_stream(
    stream: Iterator[FeatureRecord],
    config: BaseRecordConfig,
) -> Iterator[FeatureRecord]:
    """Apply feature and sequence transforms declared in the config."""

    feature_tf = getattr(config, "feature_transforms", None) or []
    for transform in _instantiate_transforms("datapipeline.transforms.feature", feature_tf):
        stream = transform.apply(stream)

    seq_tf = getattr(config, "sequence_transforms", None) or []
    for transform in _instantiate_transforms("datapipeline.transforms.sequence", seq_tf):
        stream = transform.apply(stream)
    return stream


def record_to_feature(
    stream: Iterable[Record],
    config: BaseRecordConfig,
    group_by: GroupBy,
) -> Iterator[FeatureRecord]:
    """Convert raw records into ``FeatureRecord`` instances."""

    keygen = RecordKeyGenerator(config.partition_by)

    def group_key(rec: Record) -> tuple:
        return tuple(k.normalize(getattr(rec, k.field)) for k in group_by.keys)

    for rec in stream:
        yield FeatureRecord(
            record=rec,
            feature_id=keygen.generate(config.feature_id, rec),
            group_key=group_key(rec),
        )
