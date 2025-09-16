from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.project_paths import sources_dir as sources_dir_from_project


def handle(subcmd: str) -> None:
    root_dir, name, pyproject = pkg_root(None)
    if subcmd == "sources":
        # Discover sources by scanning sources_dir for YAML files
        proj_path = root_dir / "config" / "project.yaml"
        sources_dir = sources_dir_from_project(proj_path)
        if sources_dir.exists():
            aliases = sorted(p.stem for p in sources_dir.glob("*.y*ml"))
            for a in aliases:
                print(a)
    elif subcmd == "domains":
        base = resolve_base_pkg_dir(root_dir, name)
        dom_dir = base / "domains"
        if dom_dir.exists():
            names = sorted(p.name for p in dom_dir.iterdir()
                           if p.is_dir() and (p / "model.py").exists())
            for k in names:
                print(k)
