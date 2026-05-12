from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext
from datapipeline.cli.workspace_utils import resolve_default_project_yaml
from datapipeline.services.paths import pkg_root
from datapipeline.services.project_paths import resolve_project_yaml_path
from datapipeline.services.scaffold.stream_yaml import (
    write_ingest_stream,
    write_joined_stream,
    write_manual_stream,
    format_inputs,
)
from datapipeline.services.scaffold.discovery import (
    list_domains,
    list_mappers,
    list_sources,
    list_streams,
)
from datapipeline.services.scaffold.utils import (
    info,
    status,
    error_exit,
    pick_from_menu,
    pick_from_list,
    pick_multiple_from_list,
    choose_name,
    choose_existing_or_create_name,
)
from datapipeline.services.scaffold.layout import (
    default_stream_id_for_source,
    source_id_parts,
)
from datapipeline.cli.commands.mapper import handle as handle_mapper
from datapipeline.services.scaffold.domain import create_domain
from datapipeline.services.scaffold.mapper import (
    create_joined_mapper,
    create_manual_mapper,
)


def _select_mapper(*, allow_identity: bool, allow_create: bool, root: Path | None) -> str:
    mappers = list_mappers(root=root)
    options: list[tuple[str, str]] = []
    if allow_create:
        options.append(("create", "Create new mapper (default)"))
    if mappers:
        options.append(("existing", "Select existing mapper"))
    if allow_identity:
        options.append(("identity", "Identity mapper"))
    options.append(("custom", "Custom mapper"))

    if not options:
        error_exit("No mapper options available")

    choice = pick_from_menu("Mapper:", options)
    if choice == "existing":
        return pick_from_menu(
            "Select mapper entrypoint:",
            [(k, k) for k in sorted(mappers.keys())],
        )
    if choice == "create":
        return handle_mapper(name=None, plugin_root=root)
    if choice == "identity":
        return "identity"
    ep = input("Mapper entrypoint: ").strip()
    if not ep:
        error_exit("Mapper entrypoint is required")
    return ep


def handle(
    *,
    plugin_root: Path | None = None,
    use_identity: bool = False,
    workspace: WorkspaceContext | None = None,
) -> None:
    root_dir, _name, _pyproject = pkg_root(plugin_root)
    default_project = resolve_default_project_yaml(workspace)
    info("Stream type:")
    info("  [1] Ingest (source → stream)")
    info("  [2] Joined (aligned streams → stream)")
    info("  [3] Manual (raw streams → stream)")
    sel = input("> ").strip()
    if sel in {"2", "3"}:
        if use_identity:
            error_exit("--identity is only supported for source-backed streams.")
        scaffold_multistream_stream(
            stream_type="joined" if sel == "2" else "manual",
            stream_id=None,
            inputs=None,
            mapper_path=None,
            with_mapper_stub=False,
            plugin_root=plugin_root,
            project_yaml=default_project,
        )
        return

    # Discover sources by scanning sources_dir YAMLs
    # Default to dataset-scoped project config
    proj_path = default_project or resolve_project_yaml_path(root_dir)
    source_options = list_sources(proj_path)
    if not source_options:
        error_exit("No sources found. Create one first (jerry source create ...)")

    src_key = pick_from_list("Select source:", source_options)
    _provider, dataset, source_variant = source_id_parts(src_key)
    if not dataset:
        error_exit(
            "Source alias must be 'provider.dataset[.variant]' (from source file's id)"
        )

    dom_name, should_create_domain = choose_existing_or_create_name(
        label="Domain",
        existing=list_domains(root=plugin_root),
        create_label="Create new domain",
        prompt_new="Domain name",
        default_new=dataset,
    )
    if should_create_domain:
        create_domain(domain=dom_name, root=plugin_root)

    if use_identity:
        mapper_ep = "identity"
        status("ok", "Using built-in mapper entry point 'identity'.")
    else:
        mapper_ep = _select_mapper(
            allow_identity=True,
            allow_create=True,
            root=plugin_root,
        )

    # Derive canonical stream id as domain.dataset[.variant]
    variant_default = f" (default: {source_variant})" if source_variant else ""
    info(f"Optional variant suffix{variant_default} (press Enter to skip):")
    variant = input("> ").strip()
    stream_id = choose_name(
        "Stream id",
        default=default_stream_id_for_source(dom_name, src_key, variant or None),
    )

    write_ingest_stream(
        project_yaml=proj_path,
        stream_id=stream_id,
        source=src_key,
        mapper_entrypoint=mapper_ep,
    )


def scaffold_multistream_stream(
    *,
    stream_type: str,
    stream_id: str | None,
    inputs: str | None,
    mapper_path: str | None,
    with_mapper_stub: bool,
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> None:
    if stream_type == "joined":
        _scaffold_joined_stream(
            stream_id=stream_id,
            inputs=inputs,
            mapper_path=mapper_path,
            with_mapper_stub=with_mapper_stub,
            plugin_root=plugin_root,
            project_yaml=project_yaml,
        )
        return
    if stream_type == "manual":
        _scaffold_manual_stream(
            stream_id=stream_id,
            inputs=inputs,
            mapper_path=mapper_path,
            with_mapper_stub=with_mapper_stub,
            plugin_root=plugin_root,
            project_yaml=project_yaml,
        )
        return
    error_exit(f"Unsupported stream type '{stream_type}'")


def _collect_multistream_base(
    *,
    stream_id: str | None,
    inputs: str | None,
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> tuple[Path, str, str, str, str]:
    root_dir, _name, _ = pkg_root(plugin_root)
    proj_path = project_yaml or resolve_project_yaml_path(root_dir)
    if not inputs:
        streams = list_streams(proj_path)
        if not streams:
            error_exit(
                "No input streams found. Create a source-backed stream first."
            )
        picked = pick_multiple_from_list(
            "Select one or more input streams (comma-separated numbers):",
            streams,
        )
        inputs_list, driver_key = format_inputs(picked)
    else:
        pairs = [s.strip() for s in inputs.split(",") if s.strip()]
        inputs_list = "\n    ".join(_input_pair_to_yaml(pair) for pair in pairs)
        driver_key = inputs.split(",")[0].split("=")[0].strip()

    if not stream_id:
        domain, should_create_domain = choose_existing_or_create_name(
            label="Domain",
            existing=list_domains(root=plugin_root),
            create_label="Create new domain",
            prompt_new="Domain name",
        )
        if should_create_domain:
            create_domain(domain=domain, root=plugin_root)
        stream_id = choose_name("Stream id")
    else:
        domain = stream_id.split(".")[0]
    return proj_path, domain, stream_id, inputs_list, driver_key


def _input_pair_to_yaml(pair: str) -> str:
    if "=" not in pair:
        return f"{pair}: {pair}"
    alias, ref = pair.split("=", 1)
    return f"{alias.strip()}: {ref.strip()}"


def _select_multistream_mapper(
    *,
    label: str,
    plugin_root: Path | None,
    mapper_path: str | None,
) -> tuple[str | None, bool]:
    if not mapper_path:
        mappers = list_mappers(root=plugin_root)
        if mappers:
            choice = pick_from_menu(
                "Mapper:",
                [
                    ("create", f"Create new {label} mapper (default)"),
                    ("existing", "Select existing mapper"),
                    ("custom", "Custom mapper"),
                ],
            )
        else:
            choice = pick_from_menu(
                "Mapper:",
                [
                    ("create", f"Create new {label} mapper (default)"),
                    ("custom", "Custom mapper"),
                ],
            )
        if choice == "existing":
            mapper_path = pick_from_menu(
                "Select mapper entrypoint:",
                [(k, k) for k in sorted(mappers.keys())],
            )
            with_mapper_stub = False
        elif choice == "create":
            with_mapper_stub = True
        else:
            mapper_path = input("Mapper entrypoint: ").strip()
            if not mapper_path:
                error_exit("Mapper entrypoint is required")
            with_mapper_stub = False
    else:
        with_mapper_stub = False
    return mapper_path, with_mapper_stub


def _scaffold_manual_stream(
    *,
    stream_id: str | None,
    inputs: str | None,
    mapper_path: str | None,
    with_mapper_stub: bool,
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> None:
    proj_path, domain, stream_id, inputs_list, driver_key = _collect_multistream_base(
        stream_id=stream_id,
        inputs=inputs,
        plugin_root=plugin_root,
        project_yaml=project_yaml,
    )
    mapper_path, selected_stub = _select_multistream_mapper(
        label="manual",
        plugin_root=plugin_root,
        mapper_path=mapper_path,
    )
    with_mapper_stub = with_mapper_stub or selected_stub
    if with_mapper_stub:
        mapper_path = create_manual_mapper(
            domain=domain,
            stream_id=stream_id,
            root=plugin_root,
            mapper_path=mapper_path,
        )
    write_manual_stream(
        project_yaml=proj_path,
        stream_id=stream_id,
        inputs_list=inputs_list,
        mapper_entrypoint=mapper_path,
        driver_key=driver_key,
    )


def _scaffold_joined_stream(
    *,
    stream_id: str | None,
    inputs: str | None,
    mapper_path: str | None,
    with_mapper_stub: bool,
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> None:
    proj_path, domain, stream_id, inputs_list, primary_key = _collect_multistream_base(
        stream_id=stream_id,
        inputs=inputs,
        plugin_root=plugin_root,
        project_yaml=project_yaml,
    )
    mapper_path, selected_stub = _select_multistream_mapper(
        label="joined",
        plugin_root=plugin_root,
        mapper_path=mapper_path,
    )
    with_mapper_stub = with_mapper_stub or selected_stub
    if with_mapper_stub:
        mapper_path = create_joined_mapper(
            domain=domain,
            stream_id=stream_id,
            root=plugin_root,
            mapper_path=mapper_path,
        )
    write_joined_stream(
        project_yaml=proj_path,
        stream_id=stream_id,
        inputs_list=inputs_list,
        mapper_entrypoint=mapper_path,
        primary_key=primary_key,
    )
