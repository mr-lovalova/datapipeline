from collections.abc import Iterator, Mapping, Sequence
from functools import partial
from pathlib import Path
from typing import Any

from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.artifacts.registry import (
    VECTOR_METADATA_SPEC,
    ArtifactNotRegisteredError,
)
from datapipeline.artifacts.series import (
    SeriesManifest,
    SeriesRow,
    load_series_manifest,
    open_series,
)
from datapipeline.artifacts.specs import SERIES
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.series_id import base_id
from datapipeline.domain.vector import Vector
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.events import ProgressSnapshot
from datapipeline.execution.pipeline import Input
from datapipeline.pipelines.sample.keys import (
    RectangularKeyPlan,
    sample_domain_key_plan,
    window_key_plan,
)


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

    manifest_path, manifest = _require_series(
        context,
        group_by_cadence,
        sample_key_fields,
    )
    key_plan = (
        rectangular_key_plan(context, group_by_cadence, sample_key_fields)
        if rectangular
        else None
    )
    return _samples_from_series(
        manifest_path,
        manifest,
        feature_cfgs,
        target_cfgs,
        key_plan,
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

    manifest_path, manifest = _require_series(
        context,
        group_by_cadence,
        sample_keys,
    )
    return _samples_from_series(
        manifest_path,
        manifest,
        feature_configs,
        target_configs,
        key_plan,
    )


def _require_series(
    context: PipelineContext,
    group_by_cadence: str,
    sample_keys: Sequence[str],
) -> tuple[Path, SeriesManifest]:
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
    return manifest_path, manifest


def _samples_from_series(
    manifest_path: Path,
    manifest: SeriesManifest,
    feature_configs: Sequence[SeriesConfig],
    target_configs: Sequence[SeriesConfig],
    key_plan: RectangularKeyPlan | None,
) -> Iterator[Sample]:
    feature_ids = {config.id for config in feature_configs}
    available_feature_ids = {entry.id for entry in manifest.features}
    missing_features = sorted(feature_ids - available_feature_ids)
    if missing_features:
        raise RuntimeError(
            "Series artifact does not contain configured feature ids: "
            + ", ".join(missing_features)
        )

    target_ids = {config.id for config in target_configs}
    available_target_ids = {entry.id for entry in manifest.targets}
    missing_targets = sorted(target_ids - available_target_ids)
    if missing_targets:
        raise RuntimeError(
            "Series artifact does not contain configured target ids: "
            + ", ".join(missing_targets)
        )

    rows = open_series(manifest_path, manifest)
    selected = _select_rows(
        rows,
        feature_ids,
        target_ids,
        all_features_selected=feature_ids == available_feature_ids,
        all_targets_selected=target_ids == available_target_ids,
    )
    if key_plan is None:
        return _sparse_samples(selected)
    return _rectangular_samples(
        selected,
        key_plan.keys(),
        include_targets=bool(target_configs),
    )


def _select_rows(
    rows: Iterator[SeriesRow],
    feature_ids: set[str],
    target_ids: set[str],
    all_features_selected: bool,
    all_targets_selected: bool,
) -> Iterator[tuple[tuple, dict[str, Any], dict[str, Any]]]:
    try:
        for row in rows:
            yield (
                row.key,
                (
                    row.features
                    if all_features_selected
                    else _select_values(row.features, feature_ids)
                ),
                (
                    row.targets
                    if all_targets_selected
                    else _select_values(row.targets, target_ids)
                ),
            )
    finally:
        _close_iterator(rows)


def _select_values(
    values: Mapping[str, Any],
    selected_ids: set[str],
) -> dict[str, Any]:
    return {
        series_id: value
        for series_id, value in values.items()
        if base_id(series_id) in selected_ids
    }


def _sparse_samples(
    rows: Iterator[tuple[tuple, dict[str, Any], dict[str, Any]]],
) -> Iterator[Sample]:
    try:
        for key, features, targets in rows:
            if not features:
                continue
            yield Sample(
                key=key,
                features=Vector(features),
                targets=Vector(targets) if targets else None,
            )
    finally:
        _close_iterator(rows)


def _rectangular_samples(
    rows: Iterator[tuple[tuple, dict[str, Any], dict[str, Any]]],
    keys: Iterator[tuple],
    include_targets: bool,
) -> Iterator[Sample]:
    try:
        current = next(rows, None)
        for key in keys:
            while current is not None and current[0] < key:
                current = next(rows, None)
            if current is not None and current[0] == key:
                _, features, targets = current
                current = next(rows, None)
            else:
                features = {}
                targets = {}
            yield Sample(
                key=key,
                features=Vector(features),
                targets=Vector(targets) if include_targets else None,
            )
    finally:
        _close_iterator(rows)
        _close_iterator(keys)


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()


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
