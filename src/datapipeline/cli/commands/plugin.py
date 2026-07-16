import logging

from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.scaffold.plugin import scaffold_plugin

logger = logging.getLogger(__name__)


def handle(
    subcmd: str,
    name: str | None,
    out: str,
    *,
    workspace: WorkspaceContext | None = None,
) -> None:
    if subcmd != "init":
        raise SystemExit(f"Unknown plugin subcommand: {subcmd}")
    if not name:
        logger.error(
            "Plugin name is required. Use 'jerry plugin init <name>' or pass -n/--name."
        )
        raise SystemExit(2)
    outdir = resolve_workspace_path(
        out,
        workspace.root if workspace is not None else None,
    )
    try:
        target = scaffold_plugin(name, outdir)
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    logger.info("Plugin: %s", target)
