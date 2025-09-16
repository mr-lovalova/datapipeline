from typing import Any, Iterator, Callable
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.config.dataset.feature import BaseRecordConfig
from datapipeline.domain.record import Record
from datapipeline.domain.feature import FeatureRecord
from datapipeline.pipeline.utils.keygen import RecordKeyGenerator
from datapipeline.utils.load import load_ep

from datapipeline.utils.load import load_ep
from datapipeline.plugins import FILTERS_EP, TRANSFORMS_EP


def filter_record_stream(stream: Iterator[Any], filters: list[dict] | None) -> Iterator[Any]:
    for clause in filters or []:
        if not isinstance(clause, dict) or len(clause) != 1:
            raise TypeError(f"Filter must be one-key mapping, got: {clause!r}")
        (name, mapping), = clause.items()
        if not isinstance(mapping, dict) or len(mapping) != 1:
            raise TypeError(
                f"Filter '{name}' must map exactly one field to a target, got: {mapping!r}")
        (field, target), = mapping.items()
        fn: Callable = load_ep(group=FILTERS_EP, name=name)
        stream = fn(stream, field, target)
    return stream


def transform_record_stream(stream: Iterator[Any], transforms: list[dict] | None) -> Iterator[Any]:
    for clause in transforms or []:
        if not isinstance(clause, dict) or len(clause) != 1:
            raise TypeError(
                f"Transform must be one-key mapping, got: {clause!r}")
        (name, params), = clause.items()
        fn: Callable = load_ep(group=TRANSFORMS_EP, name=name)
        if params is None:
            stream = fn(stream)
        elif isinstance(params, (list, tuple)):
            stream = fn(stream, *params)
        elif isinstance(params, dict):
            stream = fn(stream, **params)
        else:
            stream = fn(stream, params)
    return stream


def _instantiate_transforms(group: str, clauses: list[dict] | None):
    ts = []
    for clause in clauses or []:
        if not isinstance(clause, dict) or len(clause) != 1:
            raise TypeError(f"Transform must be one-key mapping, got: {clause!r}")
        (name, params), = clause.items()
        cls = load_ep(group=group, name=name)
        if params is None:
            ts.append(cls())
        elif isinstance(params, dict):
            ts.append(cls(**params))
        elif isinstance(params, (list, tuple)):
            ts.append(cls(*params))
        else:
            ts.append(cls(params))
    return ts


def transform_feature_stream(stream: Iterator[FeatureRecord], config: BaseRecordConfig) -> Iterator[FeatureRecord]:
    # Feature transforms (non-shape-changing)
    feature_tf = getattr(config, "feature_transforms", None) or []
    for t in _instantiate_transforms("datapipeline.transforms.feature", feature_tf):
        stream = t.apply(stream)
    # Sequence transforms (shape-changing)
    seq_tf = getattr(config, "sequence_transforms", None) or []
    for t in _instantiate_transforms("datapipeline.transforms.sequence", seq_tf):
        stream = t.apply(stream)
    return stream


def record_to_feature(stream: Iterator[Record], config: BaseRecordConfig, group_by: GroupBy) -> Iterator[FeatureRecord]:
    keygen = RecordKeyGenerator(config.partition_by)

    def group_key(rec: Record) -> tuple:
        return tuple(k.normalize(getattr(rec, k.field)) for k in group_by.keys)

    for rec in stream:
        yield FeatureRecord(
            record=rec,
            feature_id=keygen.generate(config.feature_id, rec),
            group_key=group_key(rec),
        )
