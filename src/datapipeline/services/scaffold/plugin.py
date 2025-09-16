from pathlib import Path
import pkg_resources

SKELETON_DIR = Path(pkg_resources.resource_filename(
    "datapipeline", "templates/plugin_skeleton"))


def scaffold_plugin(name: str, outdir: Path) -> None:
    target = (outdir / name).absolute()
    if target.exists():
        print(f"❗ `{target}` already exists")
        raise SystemExit(1)
    import shutil
    shutil.copytree(SKELETON_DIR, target)
    pkg_dir = target / "src" / "{{PACKAGE_NAME}}"
    pkg_dir.rename(target / "src" / name)
    for p in (target / "pyproject.toml", target / "README.md"):
        p.write_text(p.read_text().replace("{{PACKAGE_NAME}}", name))
    print(f"✨ Created plugin skeleton at {target}")
