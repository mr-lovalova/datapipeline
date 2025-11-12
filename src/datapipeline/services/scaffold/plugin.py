from importlib.resources import as_file, files
from pathlib import Path

from ..constants import DEFAULT_IO_LOADER_EP

_RESERVED_PACKAGE_NAMES = {"datapipeline"}


def _normalized_package_name(dist_name: str) -> str:
    package_name = dist_name.replace("-", "_")
    if package_name in _RESERVED_PACKAGE_NAMES:
        print(
            "[error] `datapipeline` is reserved for the core package. "
            "Choose a different plugin name."
        )
        raise SystemExit(1)
    if not package_name.isidentifier():
        print(
            "[error] Plugin names must be valid Python identifiers once hyphens are replaced "
            "with underscores."
        )
        raise SystemExit(1)
    return package_name


def scaffold_plugin(name: str, outdir: Path) -> None:
    target = (outdir / name).absolute()
    if target.exists():
        print(f"[error] `{target}` already exists")
        raise SystemExit(1)
    import shutil

    package_name = _normalized_package_name(name)
    skeleton_ref = files("datapipeline") / "templates" / "plugin_skeleton"
    with as_file(skeleton_ref) as skeleton_dir:
        shutil.copytree(skeleton_dir, target)
    pkg_dir = target / "src" / "{{PACKAGE_NAME}}"
    pkg_dir.rename(target / "src" / package_name)
    replacements = {
        "{{PACKAGE_NAME}}": package_name,
        "{{DIST_NAME}}": name,
        "{{DEFAULT_IO_LOADER_EP}}": DEFAULT_IO_LOADER_EP,
    }
    for p in (target / "pyproject.toml", target / "README.md"):
        text = p.read_text()
        for placeholder, value in replacements.items():
            text = text.replace(placeholder, value)
        p.write_text(text)
    print(f"[new] plugin skeleton at {target}")
