from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Mapping

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.config.dataset.normalize import floor_time_to_bucket
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.feature.keygen import FeatureIdGenerator
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.transforms.engine import apply_transforms
from datapipeline.plugins import FEATURE_TRANSFORMS_EP
from datapipeline.services.constants import SCALER_STATISTICS
from datapipeline.transforms.spec import TransformSpec
from datapipeline.transforms.utils import get_field, partition_key


def build_feature_stream(
    feature_id: str,
    field: str,
    feature_id_by: str | list[str] | None,
    sample_keys: Sequence[str],
    records: Iterable[Any],
) -> Iterable[Any]:
    keygen = FeatureIdGenerator(feature_id_by)
    for rec in records:
        if not _record_has_field(rec, field):
            raise KeyError(f"Record field '{field}' not found on {type(rec).__name__}")
        yield FeatureRecord(
            record=rec,
            id=keygen.generate(feature_id, rec),
            value=get_field(rec, field),
            entity_key=partition_key(rec, list(sample_keys)),
        )


def feature_transforms(
    context: PipelineContext,
    scale: Mapping[str, Any] | bool | None,
    sequence: Mapping[str, Any] | None,
    features: Iterator[Any],
) -> Iterable[FeatureRecord | FeatureRecordSequence]:
    clauses: list[TransformSpec] = []
    if scale:
        scale_args = {} if scale is True else dict(scale)
        if "model_path" not in scale_args:
            if not context.artifacts.has(SCALER_STATISTICS):
                raise RuntimeError(
                    "Scaler artifact is missing. Run `jerry build` to generate it "
                    "or disable scale in feature config."
                )
            model_path = context.artifacts.resolve_path(SCALER_STATISTICS)
            scale_args["model_path"] = str(model_path)
        clauses.append(TransformSpec(name="scale", params=scale_args))

    if sequence:
        clauses.append(TransformSpec(name="sequence", params=dict(sequence)))

    return apply_transforms(features, FEATURE_TRANSFORMS_EP, clauses, context)


def order_feature_records(
    context: PipelineContext,
    group_by_cadence: str | None,
    features: Iterator[Any],
) -> Iterable[Any]:
    key = _time_then_id
    if context.runtime.sample_keys and group_by_cadence is not None:
        key = _sample_group_then_time_and_id(group_by_cadence)
    return batch_sort(
        features,
        batch_size=context.runtime.execution.sort_batch_records,
        key=key,
    )


def _time_then_id(item: Any) -> tuple[Any, Any]:
    return _record_time(item), getattr(item, "id", None)


def _sample_group_then_time_and_id(group_by_cadence: str):
    def key(item: Any) -> tuple[Any, ...]:
        time_value = _record_time(item)
        entity_key = getattr(item, "entity_key", ())
        return (
            floor_time_to_bucket(time_value, group_by_cadence),
            *entity_key,
            time_value,
            getattr(item, "id", None),
        )

    return key


def _record_time(item: Any) -> Any:
    rec = getattr(item, "record", None)
    if rec is not None:
        return getattr(rec, "time", None)
    else:
        records = getattr(item, "records", None)
        return getattr(records[-1], "time", None) if records else None


def _record_has_field(record: Any, field: str) -> bool:
    if isinstance(record, dict):
        return field in record
    return hasattr(record, field)
