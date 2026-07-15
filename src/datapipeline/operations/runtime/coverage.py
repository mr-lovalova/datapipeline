from typing import Any

from datapipeline.artifacts.models import (
    ListVectorColumnStats,
    VectorBaseStats,
    VectorColumnStats,
    VectorStatsSection,
)
from datapipeline.artifacts.registry import VECTOR_STATS_SPEC
from datapipeline.config.tasks import CoverageTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime


def _availability(
    present_samples: int,
    non_null_samples: int,
    total_samples: int,
) -> dict[str, int | float]:
    return {
        "present_samples": present_samples,
        "absent_samples": total_samples - present_samples,
        "null_samples": present_samples - non_null_samples,
        "covered_samples": non_null_samples,
        "sample_opportunities": total_samples,
        "coverage": non_null_samples / total_samples if total_samples else 0.0,
    }


def _base_metric(
    entry: VectorBaseStats,
    total_samples: int,
) -> dict[str, Any]:
    return {
        "id": entry.id,
        **_availability(
            entry.present_samples,
            entry.non_null_samples,
            total_samples,
        ),
    }


def _column_metric(
    entry: VectorColumnStats,
    total_samples: int,
) -> dict[str, Any]:
    metric = {
        "id": entry.id,
        "base_id": entry.base_id,
        "kind": entry.kind,
        **_availability(
            entry.present_samples,
            entry.non_null_samples,
            total_samples,
        ),
    }
    if isinstance(entry, ListVectorColumnStats):
        opportunities = total_samples * entry.length
        metric.update(
            {
                "length": entry.length,
                "observed_elements": entry.observed_elements,
                "element_opportunities": opportunities,
                "coverage": (
                    entry.observed_elements / opportunities if opportunities else 0.0
                ),
            }
        )
    return metric


def _section_report(
    section: VectorStatsSection,
    total_samples: int,
    threshold: float,
) -> dict[str, Any]:
    bases = [_base_metric(entry, total_samples) for entry in section.bases]
    columns = [_column_metric(entry, total_samples) for entry in section.columns]
    bases.sort(key=lambda metric: (metric["coverage"], metric["id"]))
    columns.sort(key=lambda metric: (metric["coverage"], metric["id"]))
    return {
        "bases": bases,
        "columns": columns,
        "below_threshold_bases": [
            metric["id"] for metric in bases if metric["coverage"] < threshold
        ],
        "below_threshold_columns": [
            metric["id"] for metric in columns if metric["coverage"] < threshold
        ],
    }


def run_coverage_operation(
    runtime: Runtime,
    task: CoverageTask,
) -> RuntimeOutput:
    options = task.options
    stats = PipelineContext(runtime).require_artifact(VECTOR_STATS_SPEC)
    return RuntimeOutput(
        payload={
            "report": "coverage",
            "stage": stats.stage,
            "threshold": options.threshold,
            "total_samples": stats.total_samples,
            "empty_samples": stats.empty_samples,
            "features": _section_report(
                stats.features,
                stats.total_samples,
                options.threshold,
            ),
            "targets": _section_report(
                stats.targets,
                stats.total_samples,
                options.threshold,
            ),
        }
    )
