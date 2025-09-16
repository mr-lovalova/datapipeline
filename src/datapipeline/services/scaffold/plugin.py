from pathlib import Path
import pkg_resources

from ..constants import COMPOSED_LOADER_EP

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
        text = p.read_text().replace("{{PACKAGE_NAME}}", name)
        text = text.replace("{{COMPOSED_LOADER_EP}}", COMPOSED_LOADER_EP)
        p.write_text(text)
    print(f"✨ Created plugin skeleton at {target}")
