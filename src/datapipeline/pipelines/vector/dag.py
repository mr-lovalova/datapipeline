import heapq
import logging
from collections.abc import Iterator, Sequence
from itertools import chain, tee
from typing import Any

from datapipeline.artifacts.models import VectorMetadata
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.vector import Vector
from datapipeline.dag.dag import Dag
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.feature.dag import build_feature_dag, build_feature_pipeline
from datapipeline.pipelines.vector.nodes import (
    align_stream,
    sample_assemble_stage,
    sample_domain_window_keys,
    vector_assemble_stage,
    window_keys,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.vector.keygen import group_key_for
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
)

logger = logging.getLogger(__name__)
VECTOR_ASSEMBLE_DAG_NAME = "vector:assemble"


def _close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except Exception:
        logger.debug("Failed to close vector iterator during teardown", exc_info=True)


def build_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Any]:
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    if not feature_cfgs and not target_cfgs:
        return iter(())
    context.runtime.sample_keys = list(sample_keys)

    return run_dag(
        context,
        build_vector_dag(
            context,
            feature_cfgs,
            group_by_cadence,
            target_configs=target_cfgs,
            rectangular=rectangular,
            sample_keys=sample_keys,
        ),
    )


def build_vector_dag(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Dag:
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    keys_feature = None
    keys_target = None
    if rectangular:
        start, end = context.window_bounds(rectangular_required=True)
        keys = _rectangular_keys(
            context,
            start,
            end,
            group_by_cadence,
            sample_keys,
        )
        if keys is not None:
            if target_cfgs:
                keys_feature, keys_target = tee(keys, 2)
            else:
                keys_feature = keys

    nodes = [
        PipelineNode(
            name="feature_fanout",
            op=_assemble_vectors,
            args=(
                context,
                feature_cfgs,
                group_by_cadence,
                sample_keys,
                "Assembling feature vectors",
            ),
            output="feature_vectors",
            kind="dag_fanout",
            calls_dag="feature:*",
            child_dags=tuple(
                build_feature_dag(context, cfg, sample_keys=sample_keys)
                for cfg in feature_cfgs
            ),
        ),
        PipelineNode(
            name="align_feature_vectors",
            op=align_stream,
            input="feature_vectors",
            output="aligned_feature_vectors",
            kwargs={"keys": keys_feature},
        ),
    ]
    sample_kwinputs: dict[str, str] = {}
    if target_cfgs:
        nodes.extend(
            [
                PipelineNode(
                    name="target_fanout",
                    op=_assemble_vectors,
                    args=(
                        context,
                        target_cfgs,
                        group_by_cadence,
                        sample_keys,
                        "Assembling target vectors",
                    ),
                    output="target_vectors",
                    kind="dag_fanout",
                    calls_dag="feature:*",
                    child_dags=tuple(
                        build_feature_dag(context, cfg, sample_keys=sample_keys)
                        for cfg in target_cfgs
                    ),
                ),
                PipelineNode(
                    name="align_target_vectors",
                    op=align_stream,
                    input="target_vectors",
                    output="aligned_target_vectors",
                    kwargs={"keys": keys_target},
                ),
            ]
        )
        sample_kwinputs["target_vectors"] = "aligned_target_vectors"
    return Dag(
        name=VECTOR_ASSEMBLE_DAG_NAME,
        nodes=(
            *nodes,
            PipelineNode(
                name="sample_assembly",
                op=sample_assemble_stage,
                input="aligned_feature_vectors",
                kwinputs=sample_kwinputs,
            ),
        ),
    )


def _assemble_vectors(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    sample_keys: Sequence[str] = (),
    progress_stage: str = "Assembling vectors",
) -> Iterator[tuple[tuple, Vector]]:
    if not configs:
        return iter(())

    def _merged_feature_streams() -> Iterator[Any]:
        streams = [
            build_feature_pipeline(context, cfg, sample_keys=sample_keys)
            for cfg in configs
        ]
        if not streams:
            return
        key = lambda fr: group_key_for(fr, group_by_cadence)
        if sample_keys:
            merged_stream = batch_sort(
                chain.from_iterable(streams),
                batch_size=_vector_sort_batch_size(context, configs),
                key=key,
                spill_dir=context.runtime.sort_spill_dir,
                progress_stage=f"Ordering {progress_stage.lower()} by sample key",
            )
        else:
            merged_stream = heapq.merge(*streams, key=key)
        try:
            yield from merged_stream
        finally:
            _close_iterator(merged_stream)
            for stream in streams:
                _close_iterator(stream)

    return vector_assemble_stage(
        _merged_feature_streams(),
        group_by_cadence,
        progress_stage=progress_stage,
    )


def _vector_sort_batch_size(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
) -> int:
    return max(
        context.runtime.registries.sort_batch_size.get(cfg.record_stream)
        for cfg in configs
    )


def _rectangular_keys(
    context: PipelineContext,
    start,
    end,
    cadence: str,
    sample_keys: Sequence[str],
) -> Iterator[tuple] | None:
    if not sample_keys:
        return window_keys(start, end, cadence)
    domain = _sample_domain(context, cadence, sample_keys)
    return sample_domain_window_keys(start, end, cadence, sample_keys, domain)


def _sample_domain(
    context: PipelineContext,
    cadence: str,
    sample_keys: Sequence[str],
) -> list[dict[str, Any]]:
    try:
        payload = context.require_artifact(VECTOR_METADATA_SPEC)
    except ArtifactNotRegisteredError as exc:
        raise RuntimeError(
            "Sample domain unavailable; rebuild vector metadata before rectangular "
            "output with sample.keys."
        ) from exc
    meta = VectorMetadata.model_validate(payload)
    if meta.sample is None:
        raise RuntimeError(
            "Vector metadata has no sample domain; rebuild metadata after adding sample.keys."
        )
    if meta.sample.cadence != cadence or meta.sample.keys != list(sample_keys):
        raise RuntimeError(
            "Vector metadata sample config does not match dataset sample config; rebuild metadata."
        )
    return [
        entry.model_dump(mode="python", exclude_none=True)
        for entry in meta.sample.domain
    ]
