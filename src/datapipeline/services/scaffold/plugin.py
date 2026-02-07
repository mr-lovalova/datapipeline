from importlib.resources import as_file, files
from pathlib import Path
import logging
import sys

import yaml

from datapipeline.utils.load import load_yaml
from datapipeline.services.path_policy import relative_to_workspace, workspace_cwd

from ..constants import DEFAULT_IO_LOADER_EP

logger = logging.getLogger(__name__)

_RESERVED_PACKAGE_NAMES = {"datapipeline", "test", "tests"}
_STDLIB_MODULE_NAMES = getattr(sys, "stdlib_module_names", set())


def _normalized_package_name(dist_name: str) -> str:
    package_name = dist_name.replace("-", "_")
    if package_name in _RESERVED_PACKAGE_NAMES:
        logger.error(
            "`datapipeline` is reserved for the core package. Choose a different plugin name."
        )
        raise SystemExit(1)
    if package_name in _STDLIB_MODULE_NAMES:
        logger.error(
            "Plugin name '%s' conflicts with a Python standard library module. Choose a different name.",
            package_name,
        )
        raise SystemExit(1)
    if not package_name.isidentifier():
        logger.error(
            "Plugin names must be valid Python identifiers once hyphens are replaced with underscores."
        )
        raise SystemExit(1)
    return package_name


def scaffold_plugin(name: str, outdir: Path) -> None:
    target = (outdir / name).absolute()
    if target.exists():
        logger.error("`%s` already exists", target)
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
    for p in target.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in {".py", ".toml", ".md", ".yaml", ".yml"}:
            continue
        text = p.read_text()
        for placeholder, value in replacements.items():
            text = text.replace(placeholder, value)
        p.write_text(text)

    # Move jerry.yaml up to the workspace root (current working directory) so
    # users can run the CLI from the workspace without cd'ing into the plugin.
    # We adjust plugin_root and dataset paths to point at the plugin directory
    # relative to the workspace. Do not overwrite an existing workspace
    # jerry.yaml.
    plugin_jerry = target / "jerry.yaml"
    workspace_root = workspace_cwd()
    workspace_jerry = workspace_root / "jerry.yaml"
    if plugin_jerry.exists() and not workspace_jerry.exists():
        plugin_root_rel = relative_to_workspace(target, workspace_root)

        data = load_yaml(plugin_jerry)
        data["plugin_root"] = plugin_root_rel.as_posix()
        datasets = data.get("datasets") or {}
        updated_datasets = {}
        for alias, path in datasets.items():
            p = Path(path)
            if p.is_absolute():
                updated_datasets[alias] = p.as_posix()
            else:
                updated_datasets[alias] = (plugin_root_rel / p).as_posix()
        data["datasets"] = updated_datasets

        workspace_jerry.write_text(
            yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
        )
        plugin_jerry.unlink()
        logger.info("workspace jerry.yaml created at %s", workspace_jerry)

    logger.info("plugin skeleton created at %s", target)
