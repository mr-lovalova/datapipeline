import heapq
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from datetime import timedelta
from itertools import tee
from pathlib import Path

from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.sample import Sample
from datapipeline.domain.sample_key import SampleKeyContract
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
from datapipeline.vector_inputs.store import (
    CachedVectorInputShard,
    load_vector_inputs_manifest,
    open_vector_input_records,
)
from datapipeline.utils.time import parse_cadence


def build_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Sample]:
    feature_cfgs = configs
    target_cfgs = () if target_configs is None else target_configs
    if not feature_cfgs and not target_cfgs:
        return iter(())
    artifact = context.runtime.artifacts.optional(VECTOR_INPUTS)
    if artifact is None:
        raise RuntimeError(
            "Vector inputs artifact is required before vector assembly. "
            "Run `jerry build --profile vector_inputs` or use "
            "`--artifact-mode AUTO|FORCE`."
        )

    manifest_path = artifact.resolve(context.runtime.artifacts.root)
    manifest = load_vector_inputs_manifest(manifest_path)
    if manifest.cadence != group_by_cadence:
        raise RuntimeError(
            "Vector inputs artifact cadence does not match requested pipeline cadence: "
            f"{manifest.cadence!r} != {group_by_cadence!r}."
        )
    if manifest.sample_keys != tuple(sample_keys):
        raise RuntimeError(
            "Vector inputs artifact sample keys do not match requested pipeline sample keys."
        )
    sample_key_contract = SampleKeyContract(
        sample_keys,
        manifest.sample_key_types,
    )

    cadence = parse_cadence(group_by_cadence)
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
            manifest.features,
            manifest.targets,
            feature_cfgs,
        ),
        configs=feature_cfgs,
        group_by_cadence=cadence,
        sample_key_contract=sample_key_contract,
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
            shards=manifest.targets,
            configs=target_cfgs,
            group_by_cadence=cadence,
            sample_key_contract=sample_key_contract,
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
    group_by_cadence: timedelta,
    sample_key_contract: SampleKeyContract,
) -> Iterator[FeatureRecord | FeatureSequence]:
    root = manifest_path.parent
    shards_by_id = {shard.id: shard for shard in shards}

    def validated_stream(
        stream: Iterator[FeatureRecord | FeatureSequence],
    ) -> Iterator[FeatureRecord | FeatureSequence]:
        for record in stream:
            sample_key_contract.validate(record.entity_key)
            yield record

    with ExitStack() as opened:
        opened_streams: list[Iterator[FeatureRecord | FeatureSequence]] = []
        for cfg in configs:
            shard = shards_by_id.get(cfg.id)
            if shard is None:
                raise RuntimeError(
                    f"Vector inputs artifact does not contain configured input '{cfg.id}'."
                )
            opened_stream = open_vector_input_records(root / shard.path)
            closer = getattr(opened_stream, "close", None)
            if callable(closer):
                opened.callback(closer)

            opened_streams.append(validated_stream(opened_stream))

        def group_key(
            feature_record: FeatureRecord | FeatureSequence,
        ) -> tuple:
            return group_key_for(feature_record, group_by_cadence)

        merged_stream = heapq.merge(*opened_streams, key=group_key)
        opened.callback(merged_stream.close)
        yield from merged_stream


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
) -> list[SampleDomainEntry]:
    try:
        metadata = context.require_artifact(VECTOR_METADATA_SPEC)
    except ArtifactNotRegisteredError as exc:
        raise RuntimeError(
            "Sample domain unavailable; rebuild vector metadata before rectangular "
            "output with sample.keys."
        ) from exc
    if metadata.sample is None:
        raise RuntimeError(
            "Vector metadata has no sample domain; rebuild metadata after adding sample.keys."
        )
    if metadata.sample.cadence != cadence or metadata.sample.keys != list(sample_keys):
        raise RuntimeError(
            "Vector metadata sample config does not match dataset sample config; rebuild metadata."
        )
    return metadata.sample.domain
