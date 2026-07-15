from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.scaffold.demo import scaffold_demo


def handle(
    subcmd: str,
    *,
    out: str | None = None,
    workspace: WorkspaceContext | None = None,
) -> None:
    if subcmd != "init":
        raise SystemExit(f"Unknown demo subcommand: {subcmd}")
    outdir = resolve_workspace_path(
        out or ".",
        workspace.root if workspace is not None else None,
    )
    try:
        scaffold_demo(outdir)
    except FileExistsError as exc:
        raise SystemExit(str(exc)) from None
