from pathlib import Path
from typing import Any

from datapipeline.utils.load import load_yaml
from datapipeline.config.catalog import ContractConfig, StreamsConfig
from datapipeline.services.project_paths import streams_dir, sources_dir
from datapipeline.services.path_policy import resolve_relative_fs_loader_path
from datapipeline.build.state import load_build_state
from datapipeline.services.constants import (
    PARSER_KEY,
    LOADER_KEY,
    SOURCE_ID_KEY,
    MAPPER_KEY,
    ENTRYPOINT_KEY,
    STREAM_ID_KEY,
    POSTPROCESS_TRANSFORMS,
)
from datapipeline.services.streams.ingest import (
    build_mapper_from_spec,
    build_source_from_spec,
)
from datapipeline.services.streams.joined import build_joined_stream
from datapipeline.services.streams.manual import build_manual_stream
from datapipeline.services.streams.validation import (
    contract_partition_by,
    validate_stream_contracts,
)

from datapipeline.runtime import Runtime
from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.services.config_refs import resolve_config_refs
from .config import (
    artifacts_root,
    build_state_path,
    _globals,
    _interpolate,
    _load_by_key,
    _project,
)


SRC_PARSER_KEY = PARSER_KEY
SRC_LOADER_KEY = LOADER_KEY


def _load_sources_from_dir(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate per-source YAML files into a raw-sources mapping.

    Scans for YAML files under the sources directory (recursing through
    subfolders). Expects each file to define a single source with top-level
    'parser' and 'loader' keys. The top-level 'id' inside the file becomes the
    runtime alias.
    """
    src_dir = sources_dir(project_yaml)
    if not src_dir.exists() or not src_dir.is_dir():
        return {}
    out: dict[str, dict] = {}
    candidates = sorted(
        (p for p in src_dir.rglob("*.y*ml") if p.is_file()),
        key=lambda p: p.relative_to(src_dir).as_posix(),
    )
    for path in candidates:
        data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
        if not isinstance(data, dict):
            continue
        if isinstance(data.get(SRC_PARSER_KEY), dict) and isinstance(data.get(SRC_LOADER_KEY), dict):
            alias = data.get(SOURCE_ID_KEY)
            if not alias:
                raise ValueError(
                    f"Missing 'id' in source file: {path.relative_to(src_dir)}")
            source_doc = _interpolate(data, vars_)
            _normalize_source_loader_paths(
                source_doc,
                project_yaml=project_yaml,
                source_yaml=path,
            )
            out[alias] = source_doc
            continue
    return out


def _normalize_source_loader_paths(
    source_doc: dict[str, Any],
    project_yaml: Path,
    source_yaml: Path,
) -> None:
    loader = source_doc.get(SRC_LOADER_KEY)
    if not isinstance(loader, dict):
        return
    args = loader.get("args")
    if not isinstance(args, dict):
        return
    if str(args.get("transport", "")).lower() != "fs":
        return
    raw_path = args.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        return
    if Path(raw_path).is_absolute():
        return
    args["path"] = resolve_relative_fs_loader_path(
        raw_path,
        project_yaml.parent.resolve(),
    )


def _load_canonical_streams(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate canonical stream specs from streams_dir (supports subfolders).

    Recursively scans for *.yml|*.yaml under the configured streams dir.
    Stream alias is derived from the relative path with '/' replaced by '.'
    and extension removed, e.g. 'metobs/precip.yaml' -> 'metobs.precip'.
    """
    out: dict[str, dict] = {}
    sdir = streams_dir(project_yaml)
    if not sdir.exists() or not sdir.is_dir():
        return {}
    for p in sorted(sdir.rglob("*.y*ml")):
        if not p.is_file():
            continue
        data = resolve_config_refs(load_yaml(p), project_yaml=project_yaml)
        # Contracts must declare kind: 'ingest' | 'joined' | 'manual'
        if not isinstance(data, dict):
            continue
        kind = data.get("kind")
        if kind == "composed":
            raise ValueError(
                "Contract kind 'composed' is no longer supported; "
                "use 'joined' or 'manual'."
            )
        if kind not in {"ingest", "joined", "manual"}:
            continue
        if (STREAM_ID_KEY not in data):
            continue
        if kind == "ingest" and ("source" not in data):
            continue
        if kind in {"joined", "manual"} and ("inputs" not in data):
            continue
        m = data.get(MAPPER_KEY)
        if (not isinstance(m, dict)) or (ENTRYPOINT_KEY not in (m or {})):
            data[MAPPER_KEY] = None
        # Support simple per-contract variables like 'cadence' while keeping
        # project-level globals as the single source of truth for shared values.
        local_vars = dict(vars_)
        cadence_expr = data.get("cadence")
        if cadence_expr is not None:
            # Allow cadence to reference globals (e.g. ${group_by}) while also
            # making ${cadence} usable elsewhere in the same contract.
            resolved_cadence = _interpolate(cadence_expr, vars_)
            local_vars["cadence"] = resolved_cadence
        alias = data.get(STREAM_ID_KEY)
        out[alias] = _interpolate(data, local_vars)
    return out


def load_streams(project_yaml: Path) -> StreamsConfig:
    vars_ = _globals(project_yaml)
    raw = _load_sources_from_dir(project_yaml, vars_)
    contracts = _load_canonical_streams(project_yaml, vars_)
    return StreamsConfig(raw=raw, contracts=contracts)


def init_streams(cfg: StreamsConfig, runtime: Runtime) -> None:
    """Compile typed streams config into runtime registries."""
    regs = runtime.registries
    regs.clear_all()
    contracts = cfg.contracts or {}
    validate_stream_contracts(contracts)

    for alias, spec in contracts.items():
        _register_stream_policy(runtime, alias, spec, contracts)

    for alias, spec in (cfg.raw or {}).items():
        regs.sources.register(alias, build_source_from_spec(spec))

    for alias, spec in contracts.items():
        _register_contract_stream(runtime, alias, spec)


def _register_stream_policy(
    runtime: Runtime,
    alias: str,
    spec: ContractConfig,
    contracts: dict[str, ContractConfig],
) -> None:
    regs = runtime.registries
    regs.stream_operations.register(alias, spec.stream)
    regs.debug_operations.register(alias, spec.debug)
    regs.partition_by.register(alias, contract_partition_by(contracts, spec))
    regs.sort_batch_size.register(alias, spec.sort_batch_size)
    regs.record_operations.register(alias, spec.record)


def _register_contract_stream(
    runtime: Runtime,
    alias: str,
    spec: ContractConfig,
) -> None:
    regs = runtime.registries
    if spec.kind == "manual":
        regs.stream_sources.register(alias, build_manual_stream(alias, spec, runtime))
        regs.mappers.register(alias, build_mapper_from_spec(None))
        return
    if spec.kind == "joined":
        regs.stream_sources.register(alias, build_joined_stream(alias, spec, runtime))
        regs.mappers.register(
            alias,
            build_mapper_from_spec(spec.mapper, runtime=runtime, row_mapper=True),
        )
        return

    regs.mappers.register(alias, build_mapper_from_spec(spec.mapper))
    regs.stream_sources.register(alias, regs.sources.get(spec.source))


def bootstrap(project_yaml: Path) -> Runtime:
    """One-call init returning a scoped Runtime.

    Loads streams and postprocess config, fills registries, and wires artifacts
    under a per-project runtime instance.
    """
    art_root = artifacts_root(project_yaml)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=art_root)

    # Attach project-level split config once to runtime (avoid reloading later)
    try:
        proj = _project(project_yaml)
        runtime.split = getattr(proj.globals, "split", None)
    except Exception:
        runtime.split = None

    runtime.run = None
    runtime.split_keep = getattr(runtime.split, "keep", None)

    streams = load_streams(project_yaml)
    init_streams(streams, runtime)

    post_doc = _load_by_key(project_yaml, "postprocess", require_mapping=False)
    # Allow interpolation of ${var} using project.globals in postprocess.yaml
    try:
        vars_ = _globals(project_yaml)
        post_doc = _interpolate(post_doc, vars_)
    except Exception:
        pass
    if post_doc is None:
        transforms = None
    else:
        postprocess = PostprocessConfig.model_validate(post_doc)
        transforms = postprocess.root
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS, transforms)

    state_path = build_state_path(project_yaml)
    state = load_build_state(state_path)
    if state:
        for key, info in state.artifacts.items():
            runtime.artifacts.register(
                key,
                relative_path=info.relative_path,
                meta=info.meta,
            )
    return runtime
