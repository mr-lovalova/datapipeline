from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext
from datapipeline.cli.workspace_utils import resolve_default_project_yaml
from datapipeline.services.scaffold.source_yaml import (
    create_source_yaml,
    default_loader_config,
)
from datapipeline.services.scaffold.discovery import list_loaders, list_parsers
from datapipeline.services.scaffold.utils import (
    error_exit,
    info,
    choose_name,
    pick_from_menu,
    prompt_required,
)
import sys


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
    if subcmd == "create":
        # Allow: positional provider dataset, --provider/--dataset, --alias, or provider as 'prov.ds'
        if (not provider or not dataset):
            # Try alias flag first
            if alias:
                parts = alias.split(".", 1)
                if len(parts) == 2 and all(parts):
                    provider, dataset = parts[0], parts[1]
                else:
                    error_exit("Alias must be 'provider.dataset'")
            # Try provider passed as 'prov.ds' positional/flag
            elif provider and ("." in provider) and not dataset:
                parts = provider.split(".", 1)
                if len(parts) == 2 and all(parts):
                    provider, dataset = parts[0], parts[1]
                else:
                    error_exit("Source must be specified as '<provider> <dataset>' or '<provider>.<dataset>'")

        if not provider or not dataset:
            source_id = prompt_required("Source id (provider.dataset)")
            parts = source_id.split(".", 1)
            if len(parts) == 2 and all(parts):
                provider, dataset = parts[0], parts[1]
            else:
                error_exit("Source id must be in the form 'provider.dataset'")

        # Loader selection: either explicit loader EP or built-in transport defaults
        loader_ep: str | None = loader
        loader_args: dict = {}
        if not loader_ep:
            if not transport:
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
                if choice in {"fs", "http", "synthetic"}:
                    transport = choice
                elif choice == "existing":
                    loader_ep = pick_from_menu(
                        "Select loader entrypoint:",
                        [(k, k) for k in sorted(known_loaders.keys())],
                    )
                elif choice == "custom":
                    loader_ep = prompt_required("Loader entrypoint")
            if not loader_ep:
                if transport in {"fs", "http"} and not format:
                    format_options = [
                        ("csv", "csv"),
                        ("json", "json"),
                        ("json-lines", "json-lines"),
                    ]
                    if transport == "fs":
                        format_options.append(("pickle", "pickle"))
                    format = pick_from_menu("Format:", format_options)
                if not transport:
                    error_exit("--transport is required when no --loader is provided")
                loader_ep, loader_args = default_loader_config(transport, format)

        # Parser selection (no code generation)
        if identity:
            parser_ep = "identity"
        elif parser:
            parser_ep = parser
        else:
            interactive = sys.stdin.isatty()
            if not interactive:
                parser_ep = "identity"
            else:
                parsers = list_parsers(root=plugin_root)
                if parsers:
                    choice = pick_from_menu(
                        "Parser:",
                        [
                            ("existing", "Select existing parser (default)"),
                            ("identity", "Identity parser"),
                            ("custom", "Custom parser"),
                        ],
                    )
                    if choice == "existing":
                        parser_ep = pick_from_menu(
                            "Select parser entrypoint:",
                            [(k, k) for k in sorted(parsers.keys())],
                        )
                    elif choice == "identity":
                        parser_ep = "identity"
                    else:
                        parser_ep = prompt_required("Parser entrypoint")
                else:
                    choice = pick_from_menu(
                        "Parser:",
                        [
                            ("identity", "Identity parser (default)"),
                            ("custom", "Custom parser"),
                        ],
                    )
                    parser_ep = "identity" if choice == "identity" else prompt_required("Parser entrypoint")

        project_yaml = resolve_default_project_yaml(workspace)
        create_source_yaml(
            provider=provider,
            dataset=dataset,
            loader_ep=loader_ep,
            loader_args=loader_args,
            parser_ep=parser_ep,
            root=plugin_root,
            **({"project_yaml": project_yaml} if project_yaml is not None else {}),
        )
