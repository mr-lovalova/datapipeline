import logging
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.scaffold.plugin import scaffold_plugin


logger = logging.getLogger(__name__)


def bar(
    subcmd: str,
    name: str | None,
    out: str,
    *,
    workspace: WorkspaceContext | None = None,
) -> None:
    if subcmd == "init":
        if not name:
            logger.error("Plugin name is required. Use 'jerry plugin init <name>' or pass -n/--name.")
            raise SystemExit(2)
        outdir = resolve_workspace_path(
            out,
            workspace.root if workspace is not None else None,
        )
        scaffold_plugin(name, outdir)
