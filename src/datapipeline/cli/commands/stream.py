from pathlib import Path

from datapipeline.cli.workspace import WorkspaceContext, resolve_default_project_yaml
from datapipeline.services.paths import pkg_root
from datapipeline.services.project_paths import resolve_project_yaml_path
from datapipeline.services.scaffold.stream_yaml import (
    write_aligned_stream,
    write_ingest_stream,
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


def _select_ingest_mapper(root: Path | None) -> str:
    mappers = list_mappers(root=root)
    options = [("create", "Create new mapper (default)")]
    if mappers:
        options.append(("existing", "Select existing mapper"))
    options.append(("identity", "Identity mapper"))
    options.append(("custom", "Custom mapper"))

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
    stream_type = pick_from_menu(
        "Stream type:",
        [
            ("ingest", "Ingest (source → ordered stream)"),
            ("aligned", "Aligned (streams → ordered stream)"),
        ],
    )
    if stream_type == "aligned":
        if use_identity:
            error_exit("--identity is only supported for ingests.")
        _scaffold_aligned_stream(
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
        mapper_ep = _select_ingest_mapper(plugin_root)

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


def _scaffold_aligned_stream(
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> None:
    root_dir, _name, _ = pkg_root(plugin_root)
    proj_path = project_yaml or resolve_project_yaml_path(root_dir)
    streams = list_streams(proj_path)
    if len(streams) < 2:
        error_exit("Aligned streams require at least two input streams.")
    input_streams = pick_multiple_from_list(
        "Select at least two input streams (comma-separated numbers):",
        streams,
    )
    if len(set(input_streams)) < 2:
        error_exit("Aligned streams require at least two distinct input streams.")

    stream_id = choose_name("Stream id")
    combine_entrypoint = input("Combine entrypoint: ").strip()
    if not combine_entrypoint:
        error_exit("Combine entrypoint is required")

    write_aligned_stream(
        project_yaml=proj_path,
        stream_id=stream_id,
        input_streams=input_streams,
        combine_entrypoint=combine_entrypoint,
    )
