from collections import Counter
from collections.abc import Iterator, Sequence
from functools import partial
from typing import Any

from datapipeline.artifacts.ticks import (
    read_tick_grid,
    tick_grid_by_from_metadata,
)
from datapipeline.config.transforms import (
    CollapseConfig,
    DedupeConfig,
    DeriveConfig,
    EnsureCadenceConfig,
    EnsureTicksConfig,
    FillConfig,
    FloorTimeConfig,
    ForwardFillConfig,
    ForwardSumConfig,
    LagConfig,
    LeadConfig,
    PreprocessConfig,
    RollingConfig,
    RollingSlopeConfig,
    ShiftTimeConfig,
    TransformConfig,
    WhereConfig,
)
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode, StageOp
from datapipeline.transforms.stream.dedupe import DedupeTransform
from datapipeline.transforms.stream.derive import DeriveTransform
from datapipeline.transforms.stream.ensure_ticks import (
    EnsureCadenceTransform,
    EnsureTicksTransform,
)
from datapipeline.transforms.stream.fill import (
    ForwardFillTransform,
    StatisticalFillTransform,
)
from datapipeline.transforms.stream.forward_sum import ForwardSumTransform
from datapipeline.transforms.stream.collapse import CollapseTransform
from datapipeline.transforms.stream.lag import LagTransform
from datapipeline.transforms.stream.lead import LeadTransform
from datapipeline.transforms.stream.rolling import RollingTransform
from datapipeline.transforms.stream.rolling_slope import RollingSlopeTransform
from datapipeline.transforms.time import FloorTimeTransform, ShiftTimeTransform
from datapipeline.transforms.where import WhereTransform


def build_preprocess_nodes(
    operations: Sequence[PreprocessConfig],
) -> tuple[PipelineNode, ...]:
    configured = tuple(operations)
    totals = Counter(operation.operation for operation in configured)
    occurrences: Counter[str] = Counter()
    nodes: list[PipelineNode] = []
    for operation in configured:
        occurrences[operation.operation] += 1
        node_name = f"preprocess_{operation.operation}"
        if totals[operation.operation] > 1:
            node_name += f"_{occurrences[operation.operation]}"

        if isinstance(operation, WhereConfig):
            node_op: StageOp = WhereTransform(
                operation.field,
                operation.operator,
                operation.comparand,
            ).apply
        elif isinstance(operation, FloorTimeConfig):
            node_op = FloorTimeTransform(operation.cadence).apply
        elif isinstance(operation, ShiftTimeConfig):
            node_op = ShiftTimeTransform(operation.by).apply
        else:
            raise TypeError(
                f"Unsupported preprocess config: {type(operation).__name__}"
            )

        nodes.append(
            PipelineNode(
                name=node_name,
                apply=node_op,
            )
        )
    return tuple(nodes)


def build_transform_nodes(
    context: PipelineContext,
    operations: Sequence[TransformConfig],
    partition_by: tuple[str, ...],
) -> tuple[PipelineNode, ...]:
    configured = tuple(operations)
    totals = Counter(operation.operation for operation in configured)
    occurrences: Counter[str] = Counter()
    nodes: list[PipelineNode] = []
    for operation in configured:
        occurrences[operation.operation] += 1
        node_name: str = operation.operation
        if totals[operation.operation] > 1:
            node_name = f"{node_name}_{occurrences[operation.operation]}"

        node_op: StageOp
        if isinstance(operation, WhereConfig):
            node_op = WhereTransform(
                operation.field,
                operation.operator,
                operation.comparand,
            ).apply
        elif isinstance(operation, DedupeConfig):
            node_op = DedupeTransform().apply
        elif isinstance(operation, LagConfig):
            node_op = LagTransform(
                operation.field,
                operation.periods,
                partition_by,
                operation.to,
            ).apply
        elif isinstance(operation, LeadConfig):
            node_op = LeadTransform(
                operation.field,
                operation.periods,
                partition_by,
                operation.to,
            ).apply
        elif isinstance(operation, ForwardSumConfig):
            node_op = ForwardSumTransform(
                operation.field,
                operation.window,
                partition_by,
                operation.to,
            ).apply
        elif isinstance(operation, EnsureCadenceConfig):
            node_op = EnsureCadenceTransform(
                operation.cadence,
                partition_by,
            ).apply
        elif isinstance(operation, EnsureTicksConfig):
            node_op = partial(
                apply_tick_grid,
                context,
                operation,
                partition_by,
            )
        elif isinstance(operation, FillConfig):
            node_op = StatisticalFillTransform(
                operation.field,
                operation.window,
                operation.statistic,
                partition_by,
                operation.to,
                operation.min_samples,
            ).apply
        elif isinstance(operation, ForwardFillConfig):
            node_op = ForwardFillTransform(
                operation.field,
                partition_by,
                operation.to,
            ).apply
        elif isinstance(operation, CollapseConfig):
            node_op = CollapseTransform(
                partition_by,
                operation.keep,
            ).apply
        elif isinstance(operation, RollingConfig):
            node_op = RollingTransform(
                operation.field,
                operation.window,
                partition_by,
                operation.to,
                operation.min_samples,
                operation.statistic,
            ).apply
        elif isinstance(operation, RollingSlopeConfig):
            node_op = RollingSlopeTransform(
                operation.x,
                operation.y,
                operation.window,
                partition_by,
                operation.to,
            ).apply
        elif isinstance(operation, DeriveConfig):
            if operation.right_field is not None:
                transform = DeriveTransform(
                    operation.left,
                    operation.operator,
                    operation.to,
                    right_field=operation.right_field,
                )
            else:
                transform = DeriveTransform(
                    operation.left,
                    operation.operator,
                    operation.to,
                    right_value=operation.right_value,
                )
            node_op = transform.apply
        else:
            raise TypeError(f"Unsupported transform config: {type(operation).__name__}")

        nodes.append(
            PipelineNode(
                name=node_name,
                apply=node_op,
            )
        )
    return tuple(nodes)


def apply_tick_grid(
    context: PipelineContext,
    operation: EnsureTicksConfig,
    partition_fields: tuple[str, ...],
    records: Iterator[Any],
) -> Iterator[Any]:
    path = context.resolve_artifact_path(operation.artifact)
    grid_by = tick_grid_by_from_metadata(
        operation.artifact,
        context.artifact_metadata(operation.artifact),
    )
    ticks = read_tick_grid(path, grid_by)
    return EnsureTicksTransform(
        ticks,
        partition_fields,
    ).apply(records)
