from importlib.resources import as_file, files
from pathlib import Path

from ..constants import COMPOSED_LOADER_EP


def scaffold_plugin(name: str, outdir: Path) -> None:
    target = (outdir / name).absolute()
    if target.exists():
        print(f"❗ `{target}` already exists")
        raise SystemExit(1)
    import shutil

    skeleton_ref = files("datapipeline") / "templates" / "plugin_skeleton"
    with as_file(skeleton_ref) as skeleton_dir:
        shutil.copytree(skeleton_dir, target)
    pkg_dir = target / "src" / "{{PACKAGE_NAME}}"
    pkg_dir.rename(target / "src" / name)
    for p in (target / "pyproject.toml", target / "README.md"):
        text = p.read_text().replace("{{PACKAGE_NAME}}", name)
        text = text.replace("{{COMPOSED_LOADER_EP}}", COMPOSED_LOADER_EP)
        p.write_text(text)
    print(f"✨ Created plugin skeleton at {target}")
