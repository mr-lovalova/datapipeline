import heapq
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from datetime import timedelta
from functools import partial
from itertools import tee
from pathlib import Path

from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.artifacts.registry import (
    VECTOR_METADATA_SPEC,
    ArtifactNotRegisteredError,
)
from datapipeline.artifacts.series import (
    SeriesManifest,
    SeriesShard,
    load_series_manifest,
    open_series,
)
from datapipeline.artifacts.specs import SERIES
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.domain.series import SeriesRecord, SeriesSequence
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.events import ProgressSnapshot
from datapipeline.execution.pipeline import Input
from datapipeline.pipelines.sample.assembly import (
    align_vectors_to_keys,
    assemble_samples,
    assemble_vectors,
)
from datapipeline.pipelines.sample.keys import (
    RectangularKeyPlan,
    sample_domain_key_plan,
    sample_key_for,
    window_key_plan,
)
from datapipeline.utils.time import parse_cadence


def open_samples(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Sample]:
    feature_cfgs = tuple(feature_configs)
    target_cfgs = () if target_configs is None else tuple(target_configs)
    sample_key_fields = tuple(sample_keys)
    if not feature_cfgs and not target_cfgs:
        return iter(())

    manifest_path, manifest, sample_key_contract = _require_series(
        context, group_by_cadence, sample_key_fields
    )
    cadence = parse_cadence(group_by_cadence)
    key_plan = (
        rectangular_key_plan(context, group_by_cadence, sample_key_fields)
        if rectangular
        else None
    )
    return _assemble_samples_from_series(
        manifest_path=manifest_path,
        manifest=manifest,
        feature_configs=feature_cfgs,
        target_configs=target_cfgs,
        cadence=cadence,
        sample_key_contract=sample_key_contract,
        key_plan=key_plan,
    )


def build_sample_input(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Input:
    feature_cfgs = tuple(feature_configs)
    target_cfgs = () if target_configs is None else tuple(target_configs)
    sample_key_fields = tuple(sample_keys)
    has_inputs = bool(feature_cfgs or target_cfgs)
    key_plan = (
        rectangular_key_plan(context, group_by_cadence, sample_key_fields)
        if rectangular and has_inputs
        else None
    )
    progress = None
    if key_plan is not None:
        progress = partial(
            ProgressSnapshot,
            total=key_plan.total,
            unit="samples",
        )
    return Input(
        name="assemble_samples",
        open=partial(
            _open_samples,
            context,
            feature_cfgs,
            group_by_cadence,
            target_cfgs,
            sample_key_fields,
            key_plan,
        ),
        progress=progress,
    )


def _open_samples(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig],
    sample_keys: Sequence[str],
    key_plan: RectangularKeyPlan | None,
) -> Iterator[Sample]:
    if not feature_configs and not target_configs:
        return iter(())

    manifest_path, manifest, sample_key_contract = _require_series(
        context, group_by_cadence, sample_keys
    )
    return _assemble_samples_from_series(
        manifest_path=manifest_path,
        manifest=manifest,
        feature_configs=feature_configs,
        target_configs=target_configs,
        cadence=parse_cadence(group_by_cadence),
        sample_key_contract=sample_key_contract,
        key_plan=key_plan,
    )


def _require_series(
    context: PipelineContext,
    group_by_cadence: str,
    sample_keys: Sequence[str],
) -> tuple[Path, SeriesManifest, SampleKeyContract]:
    artifact = context.runtime.artifacts.optional(SERIES)
    if artifact is None:
        raise RuntimeError(
            "Series artifact is required before sample assembly. "
            "Run `jerry build --profile series` or use "
            "`--artifact-mode AUTO|FORCE`."
        )

    manifest_path = artifact.resolve(context.runtime.artifacts.root)
    manifest = load_series_manifest(manifest_path)
    if manifest.cadence != group_by_cadence:
        raise RuntimeError(
            "Series artifact cadence does not match requested pipeline cadence: "
            f"{manifest.cadence!r} != {group_by_cadence!r}."
        )
    if manifest.sample_keys != tuple(sample_keys):
        raise RuntimeError(
            "Series artifact sample keys do not match requested pipeline sample keys."
        )
    sample_key_contract = SampleKeyContract(
        sample_keys,
        manifest.sample_key_types,
    )
    return manifest_path, manifest, sample_key_contract


def _assemble_samples_from_series(
    manifest_path: Path,
    manifest: SeriesManifest,
    feature_configs: Sequence[SeriesConfig],
    target_configs: Sequence[SeriesConfig],
    cadence: timedelta,
    sample_key_contract: SampleKeyContract,
    key_plan: RectangularKeyPlan | None,
) -> Iterator[Sample]:
    feature_keys = None if key_plan is None else key_plan.keys()
    target_keys = None
    if feature_keys is not None and target_configs:
        feature_keys, target_keys = tee(feature_keys)

    keyed_feature_records = _merged_keyed_records(
        manifest_path=manifest_path,
        shards=_shards_for_configs(
            manifest.features,
            manifest.targets,
            feature_configs,
        ),
        configs=feature_configs,
        group_by_cadence=cadence,
        sample_key_contract=sample_key_contract,
    )
    feature_vectors = assemble_vectors(keyed_feature_records)
    aligned_feature_vectors = align_vectors_to_keys(feature_vectors, feature_keys)

    target_vectors = None
    if target_configs:
        keyed_target_records = _merged_keyed_records(
            manifest_path=manifest_path,
            shards=manifest.targets,
            configs=target_configs,
            group_by_cadence=cadence,
            sample_key_contract=sample_key_contract,
        )
        target_vectors = align_vectors_to_keys(
            assemble_vectors(keyed_target_records),
            target_keys,
        )
    return assemble_samples(aligned_feature_vectors, target_vectors)


def _shards_for_configs(
    feature_shards: Sequence[SeriesShard],
    target_shards: Sequence[SeriesShard],
    configs: Sequence[SeriesConfig],
) -> Sequence[SeriesShard]:
    requested = {cfg.id for cfg in configs}
    feature_ids = {shard.id for shard in feature_shards}
    if requested <= feature_ids:
        return feature_shards
    target_ids = {shard.id for shard in target_shards}
    if requested <= target_ids:
        return target_shards
    missing = sorted(requested - feature_ids - target_ids)
    raise RuntimeError(
        "Series artifact does not contain configured series ids: " + ", ".join(missing)
    )


def _merged_keyed_records(
    *,
    manifest_path: Path,
    shards: Sequence[SeriesShard],
    configs: Sequence[SeriesConfig],
    group_by_cadence: timedelta,
    sample_key_contract: SampleKeyContract,
) -> Iterator[tuple[tuple, SeriesRecord | SeriesSequence]]:
    root = manifest_path.parent
    shards_by_id = {shard.id: shard for shard in shards}

    def keyed_stream(
        stream: Iterator[SeriesRecord | SeriesSequence],
    ) -> Iterator[tuple[tuple, SeriesRecord | SeriesSequence]]:
        for record in stream:
            sample_key_contract.validate(record.entity_key)
            yield sample_key_for(record, group_by_cadence), record

    with ExitStack() as opened:
        opened_streams: list[Iterator[tuple[tuple, SeriesRecord | SeriesSequence]]] = []
        for cfg in configs:
            shard = shards_by_id.get(cfg.id)
            if shard is None:
                raise RuntimeError(
                    f"Series artifact does not contain series '{cfg.id}'."
                )
            opened_stream = open_series(
                root / shard.path,
                expected_rows=shard.rows,
            )
            closer = getattr(opened_stream, "close", None)
            if callable(closer):
                opened.callback(closer)

            opened_streams.append(keyed_stream(opened_stream))

        merged_stream = heapq.merge(*opened_streams, key=lambda item: item[0])
        opened.callback(merged_stream.close)
        yield from merged_stream


def rectangular_key_plan(
    context: PipelineContext,
    cadence: str,
    sample_keys: Sequence[str],
) -> RectangularKeyPlan | None:
    start, end = context.window_bounds(rectangular_required=True)
    if not sample_keys:
        return window_key_plan(start, end, cadence)
    domain = _sample_domain(context, cadence, sample_keys)
    return sample_domain_key_plan(start, end, cadence, sample_keys, domain)


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
