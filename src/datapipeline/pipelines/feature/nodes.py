from collections.abc import Iterable
from typing import Any, Mapping

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.feature.keygen import FeatureIdGenerator
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.transforms.engine import apply_transforms
from datapipeline.plugins import FEATURE_TRANSFORMS_EP
from datapipeline.services.constants import SCALER_STATISTICS
from datapipeline.transforms.utils import get_field


def build_feature_stream(
    feature_id: str,
    field: str,
    partition_by: str | list[str] | None,
    records: Iterable[Any],
) -> Iterable[Any]:
    keygen = FeatureIdGenerator(partition_by)
    for rec in records:
        if not _record_has_field(rec, field):
            raise KeyError(
                f"Record field '{field}' not found on {type(rec).__name__}"
            )
        yield FeatureRecord(
            record=rec,
            id=keygen.generate(feature_id, rec),
            value=get_field(rec, field),
        )


def feature_transforms(
    context: PipelineContext,
    scale: Mapping[str, Any] | bool | None,
    sequence: Mapping[str, Any] | None,
    features: Iterable[Any] | None,
) -> Iterable[FeatureRecord | FeatureRecordSequence]:
    clauses: list[Mapping[str, Any]] = []
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
        clauses.append({"scale": scale_args})

    if sequence:
        clauses.append({"sequence": dict(sequence)})

    return apply_transforms(features, FEATURE_TRANSFORMS_EP, clauses, context)


def order_feature_records(
    context: PipelineContext,
    batch_size: int,
    features: Iterable[Any] | None,
) -> Iterable[Any]:
    return batch_sort(
        features,
        batch_size=batch_size,
        key=_time_then_id,
        spill_dir=context.runtime.sort_spill_dir,
    )


def _time_then_id(item: Any) -> tuple[Any, Any]:
    rec = getattr(item, "record", None)
    if rec is not None:
        time_value = getattr(rec, "time", None)
    else:
        records = getattr(item, "records", None)
        time_value = getattr(records[0], "time", None) if records else None
    return time_value, getattr(item, "id", None)


def _record_has_field(record: Any, field: str) -> bool:
    if isinstance(record, dict):
        return field in record
    return hasattr(record, field)
