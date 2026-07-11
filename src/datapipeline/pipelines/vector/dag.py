import heapq
import logging
from collections.abc import Iterator, Sequence
from itertools import tee
from pathlib import Path
from typing import Any

from datapipeline.artifacts.models import VectorMetadata
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.vector.keygen import group_key_for
from datapipeline.pipelines.vector.nodes import (
    align_stream,
    sample_assemble_stage,
    sample_domain_window_keys,
    vector_assemble_stage,
    window_keys,
)
from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
)
from datapipeline.services.constants import VECTOR_INPUTS
from datapipeline.vector_inputs import (
    CachedVectorInputShard,
    load_vector_inputs_manifest,
    open_vector_input_records,
)

logger = logging.getLogger(__name__)


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

    cached = _build_cached_vector_pipeline(
        context,
        feature_cfgs,
        group_by_cadence,
        target_configs=target_cfgs,
        rectangular=rectangular,
        sample_keys=sample_keys,
    )
    if cached is not None:
        return cached
    raise RuntimeError(
        "Vector inputs artifact is required before vector assembly. "
        "Run `jerry build --run vector_inputs` or use `--artifact-mode AUTO|FORCE`."
    )


def _build_cached_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Any] | None:
    artifact = context.runtime.artifacts.optional(VECTOR_INPUTS)
    if artifact is None:
        return None

    manifest_path = artifact.resolve(context.runtime.artifacts.root)
    manifest = load_vector_inputs_manifest(manifest_path)
    if manifest.format != "jsonl.gz":
        raise RuntimeError(f"Unsupported vector inputs format '{manifest.format}'.")
    if manifest.group_by != group_by_cadence:
        raise RuntimeError(
            "Vector inputs artifact group_by does not match requested pipeline cadence: "
            f"{manifest.group_by!r} != {group_by_cadence!r}."
        )
    if manifest.sample_keys != tuple(sample_keys):
        raise RuntimeError(
            "Vector inputs artifact sample keys do not match requested pipeline sample keys."
        )

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

    feature_records = _merged_cached_records(
        manifest_path=manifest_path,
        shards=_shards_for_configs(
            manifest.feature_shards,
            manifest.target_shards,
            configs,
        ),
        configs=configs,
        group_by_cadence=group_by_cadence,
    )
    feature_vectors = vector_assemble_stage(
        feature_records,
        group_by_cadence,
    )
    aligned_feature_vectors = align_stream(feature_vectors, keys_feature)

    target_vectors = None
    if target_cfgs:
        target_records = _merged_cached_records(
            manifest_path=manifest_path,
            shards=manifest.target_shards,
            configs=target_cfgs,
            group_by_cadence=group_by_cadence,
        )
        target_vectors = align_stream(
            vector_assemble_stage(
                target_records,
                group_by_cadence,
            ),
            keys_target,
        )
    return sample_assemble_stage(aligned_feature_vectors, target_vectors)


def _shards_for_configs(
    feature_shards: Sequence[CachedVectorInputShard],
    target_shards: Sequence[CachedVectorInputShard],
    configs: Sequence[FeatureRecordConfig],
) -> Sequence[CachedVectorInputShard]:
    requested = {cfg.id for cfg in configs}
    feature_ids = {shard.id for shard in feature_shards}
    if requested <= feature_ids:
        return feature_shards
    target_ids = {shard.id for shard in target_shards}
    if requested <= target_ids:
        return target_shards
    missing = sorted(requested - feature_ids - target_ids)
    raise RuntimeError(
        "Vector inputs artifact does not contain configured input ids: "
        + ", ".join(missing)
    )


def _merged_cached_records(
    *,
    manifest_path: Path,
    shards: Sequence[CachedVectorInputShard],
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
) -> Iterator[Any]:
    root = manifest_path.parent
    shards_by_id = {shard.id: shard for shard in shards}
    opened_streams: list[Iterator[Any]] = []
    for cfg in configs:
        shard = shards_by_id.get(cfg.id)
        if shard is None:
            raise RuntimeError(
                f"Vector inputs artifact does not contain configured input '{cfg.id}'."
            )
        opened_streams.append(open_vector_input_records(root / shard.path))

    def group_key(feature_record):
        return group_key_for(feature_record, group_by_cadence)

    merged_stream = heapq.merge(*opened_streams, key=group_key)
    try:
        yield from merged_stream
    finally:
        _close_iterator(merged_stream)
        for stream in opened_streams:
            _close_iterator(stream)


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
