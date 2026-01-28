import sys
from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext
from datapipeline.cli.workspace_utils import resolve_default_project_yaml
from datapipeline.services.paths import pkg_root
from datapipeline.services.entrypoints import read_group_entries
from datapipeline.services.constants import FILTERS_GROUP
from datapipeline.services.project_paths import resolve_project_yaml_path
from datapipeline.services.scaffold.contract_yaml import (
    write_ingest_contract,
    write_composed_contract,
    compose_inputs,
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
)
from datapipeline.services.scaffold.layout import default_stream_id
from datapipeline.cli.commands.mapper import handle as handle_mapper
from datapipeline.services.scaffold.mapper import create_composed_mapper


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
    root_dir, name, pyproject = pkg_root(plugin_root)
    default_project = resolve_default_project_yaml(workspace)
    # Select contract type: Ingest (source->stream) or Composed (streams->stream)
    info("Contract type:")
    info("  [1] Ingest (source → stream)")
    info("  [2] Composed (streams → stream)")
    sel = input("> ").strip()
    if sel == "2":
        if use_identity:
            error_exit("--identity is only supported for ingest contracts.")
        # Defer to composed scaffolder (fully interactive)
        scaffold_conflux(
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
    # Expect aliases as 'provider.dataset' (from source file's id)
    parts = src_key.split(".", 1)
    if len(parts) != 2:
        error_exit("Source alias must be 'provider.dataset' (from source file's id)")
    provider, dataset = parts[0], parts[1]

    domain_options = list_domains(root=plugin_root)
    if not domain_options:
        domain_options = sorted(
            read_group_entries(pyproject, FILTERS_GROUP).keys())
    if not domain_options:
        error_exit("No domains found. Create one first (jerry domain create ...)")

    dom_name = pick_from_list("Select domain:", domain_options)

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
    info("Optional variant suffix (press Enter to skip):")
    variant = input("> ").strip()
    stream_id = choose_name("Stream id", default=default_stream_id(dom_name, dataset, variant or None))

    write_ingest_contract(
        project_yaml=proj_path,
        stream_id=stream_id,
        source=src_key,
        mapper_entrypoint=mapper_ep,
    )


def scaffold_conflux(
    *,
    stream_id: str | None,
    inputs: str | None,
    mapper_path: str | None,
    with_mapper_stub: bool,
    plugin_root: Path | None,
    project_yaml: Path | None,
) -> None:
    """Scaffold a composed (multi-input) contract and optional mapper stub.

    inputs: comma-separated list of "[alias=]ref[@stage]" strings.
    mapper_path default: <pkg>.domains.<domain>:mapper where domain = stream_id.split('.')[0]
    """
    root_dir, name, _ = pkg_root(plugin_root)
    proj_path = project_yaml or resolve_project_yaml_path(root_dir)
    if not inputs:
        streams = list_streams(proj_path)
        if not streams:
            error_exit("No canonical streams found. Create them first via 'jerry contract' (ingest).")
        picked = pick_multiple_from_list(
            "Select one or more input streams (comma-separated numbers):",
            streams,
        )
        inputs_list, driver_key = compose_inputs(picked)
    else:
        inputs_list = "\n  - ".join(s.strip() for s in inputs.split(",") if s.strip())
        driver_key = inputs.split(",")[0].split("=")[0].strip()

    # If no stream_id, select target domain now and derive stream id (mirror ingest flow)
    if not stream_id:
        domain_options = list_domains(root=plugin_root)
        if not domain_options:
            error_exit("No domains found. Create one first (jerry domain create ...)")
        info("Select domain:")
        for i, opt in enumerate(domain_options, 1):
            info(f"  [{i}] {opt}")
        sel = input("> ").strip()
        try:
            idx = int(sel)
            if idx < 1 or idx > len(domain_options):
                raise ValueError
        except Exception:
            error_exit("Invalid selection.")
        domain = domain_options[idx - 1]
        stream_id = f"{domain}.processed"
    else:
        domain = stream_id.split(".")[0]

    # Mapper selection for composed contracts (no identity)
    if not mapper_path:
        mappers = list_mappers(root=plugin_root)
        if mappers:
            choice = pick_from_menu(
                "Mapper:",
                [
                    ("create", "Create new composed mapper (default)"),
                    ("existing", "Select existing mapper"),
                    ("custom", "Custom mapper"),
                ],
            )
        else:
            choice = pick_from_menu(
                "Mapper:",
                [
                    ("create", "Create new composed mapper (default)"),
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

    # Optional mapper stub under mappers/ (composed signature)
    if with_mapper_stub:
        mapper_path = create_composed_mapper(
            domain=domain,
            stream_id=stream_id,
            root=plugin_root,
            mapper_path=mapper_path,
        )
    write_composed_contract(
        project_yaml=proj_path,
        stream_id=stream_id,
        inputs_list=inputs_list,
        mapper_entrypoint=mapper_path,
        driver_key=driver_key,
    )
