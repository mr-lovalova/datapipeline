import logging
import sys
from pathlib import Path

from datapipeline.cli.prompts import pick_from_menu, prompt_required
from datapipeline.cli.source_options import SOURCE_TRANSPORTS, source_formats_for
from datapipeline.cli.workspace import WorkspaceContext, resolve_default_project_yaml
from datapipeline.services.scaffold.discovery import list_loaders, list_parsers
from datapipeline.services.scaffold.source_yaml import (
    DEFAULT_TEMPORAL_RECORD_PARSER_EP,
    create_source_yaml,
    default_loader_config,
    validate_source_id,
)

logger = logging.getLogger(__name__)


def _resolve_source_parts(
    provider: str | None,
    dataset: str | None,
    alias: str | None,
) -> tuple[str, str]:
    if provider and dataset:
        return provider, dataset

    if alias:
        parts = alias.split(".", 1)
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
        logger.error("Alias must be 'provider.dataset'")
        raise SystemExit(2)

    if provider and "." in provider and not dataset:
        parts = provider.split(".", 1)
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
        logger.error(
            "Source must be specified as '<provider> <dataset>' or '<provider>.<dataset>'"
        )
        raise SystemExit(2)

    source_id = prompt_required("Source id (provider.dataset)")
    parts = source_id.split(".", 1)
    if len(parts) == 2 and all(parts):
        return parts[0], parts[1]
    logger.error("Source id must be in the form 'provider.dataset'")
    raise SystemExit(2)


def _choose_loader_transport_or_entrypoint(
    plugin_root: Path | None,
) -> tuple[str | None, str | None]:
    known_loaders = list_loaders(root=plugin_root)
    options = [
        ("fs", "Built-in fs"),
        ("http", "Built-in http"),
        ("synthetic", "Built-in synthetic"),
    ]
    if known_loaders:
        options.append(("existing", "Select existing loader"))
    options.append(("custom", "Custom loader"))

    choice = pick_from_menu("Loader:", options)
    if choice in SOURCE_TRANSPORTS:
        return choice, None
    if choice == "existing":
        loader_ep = pick_from_menu(
            "Select loader entrypoint:",
            [(key, key) for key in sorted(known_loaders.keys())],
        )
        return None, loader_ep
    if choice == "custom":
        return None, prompt_required("Loader entrypoint")
    return None, None


def _resolve_loader_config(
    transport: str | None,
    source_format: str | None,
    loader: str | None,
    plugin_root: Path | None,
) -> tuple[str, dict]:
    if loader:
        return loader, {}

    loader_ep = None
    selected_transport = transport
    if not selected_transport:
        selected_transport, loader_ep = _choose_loader_transport_or_entrypoint(
            plugin_root
        )

    if loader_ep:
        return loader_ep, {}

    if selected_transport in {"fs", "http"} and not source_format:
        source_format = pick_from_menu(
            "Format:",
            [(name, name) for name in source_formats_for(selected_transport)],
        )
    if not selected_transport:
        logger.error("--transport is required when no --loader is provided")
        raise SystemExit(2)
    return default_loader_config(selected_transport, source_format)


def _select_parser_from_menu(plugin_root: Path | None) -> str:
    parsers = list_parsers(root=plugin_root)
    if parsers:
        choice = pick_from_menu(
            "Parser:",
            [
                ("existing", "Select existing parser (default)"),
                ("temporal_record", "Temporal record rehydration"),
                ("identity", "Identity parser"),
                ("custom", "Custom parser"),
            ],
        )
        if choice == "existing":
            return pick_from_menu(
                "Select parser entrypoint:",
                [(key, key) for key in sorted(parsers.keys())],
            )
        if choice == "temporal_record":
            return DEFAULT_TEMPORAL_RECORD_PARSER_EP
        if choice == "identity":
            return "identity"
        return prompt_required("Parser entrypoint")

    choice = pick_from_menu(
        "Parser:",
        [
            ("identity", "Identity parser (default)"),
            ("temporal_record", "Temporal record rehydration"),
            ("custom", "Custom parser"),
        ],
    )
    if choice == "temporal_record":
        return DEFAULT_TEMPORAL_RECORD_PARSER_EP
    if choice == "identity":
        return "identity"
    return prompt_required("Parser entrypoint")


def _resolve_parser_entrypoint(
    identity: bool,
    parser: str | None,
    plugin_root: Path | None,
) -> str:
    if identity:
        return "identity"
    if parser:
        return parser
    if not sys.stdin.isatty():
        return "identity"
    return _select_parser_from_menu(plugin_root)


def handle(
    subcmd: str,
    provider: str | None,
    dataset: str | None,
    transport: str | None = None,
    format: str | None = None,
    *,
    identity: bool = False,
    loader: str | None = None,
    parser: str | None = None,
    alias: str | None = None,
    plugin_root: Path | None = None,
    workspace: WorkspaceContext | None = None,
) -> None:
    if subcmd != "create":
        raise SystemExit(f"Unknown source subcommand: {subcmd}")

    provider, dataset = _resolve_source_parts(provider, dataset, alias)
    source_id = f"{provider}.{dataset}"
    try:
        validate_source_id(source_id)
        loader_ep, loader_args = _resolve_loader_config(
            transport,
            format,
            loader,
            plugin_root,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from None
    parser_ep = _resolve_parser_entrypoint(identity, parser, plugin_root)

    project_yaml = resolve_default_project_yaml(workspace)
    try:
        path = create_source_yaml(
            source_id=source_id,
            loader_ep=loader_ep,
            loader_args=loader_args,
            parser_ep=parser_ep,
            root=plugin_root,
            project_yaml=project_yaml,
        )
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    logger.info("Source: %s", path)
