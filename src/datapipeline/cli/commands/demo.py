from datapipeline.config.workspace import WorkspaceContext
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.scaffold.demo import scaffold_demo
from datapipeline.services.scaffold.plugin import scaffold_plugin


def handle(
    subcmd: str,
    *,
    out: str | None = None,
    workspace: WorkspaceContext | None = None,
) -> None:
    if subcmd != "init":
        raise SystemExit(f"Unknown demo subcommand: {subcmd}")
    demo_name = "demo"
    target_root = resolve_workspace_path(
        out or ".",
        workspace.root if workspace is not None else None,
    )
    scaffold_plugin(demo_name, target_root)
    scaffold_demo(target_root / demo_name)
