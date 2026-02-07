from importlib.resources import as_file, files
from pathlib import Path
import logging
import shutil

import yaml

from datapipeline.services.entrypoints import inject_ep
from datapipeline.services.path_policy import relative_to_workspace, workspace_cwd
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.utils.load import load_yaml

logger = logging.getLogger(__name__)

_DEMO_DATASET_ALIAS = "demo"
_DEMO_PARSER_EP = "sandbox_ohlcv_dto_parser"
_DEMO_MAPPER_EP = "map_sandbox_ohlcv_dto_to_equity"


def _replace_placeholders(path: Path, replacements: dict[str, str]) -> None:
    if not path.is_file():
        return
    if path.suffix not in {".py", ".toml", ".md", ".yaml", ".yml"}:
        return
    text = path.read_text()
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value)
    path.write_text(text)


def _inject_demo_entrypoints(pyproject: Path, pkg_name: str) -> None:
    toml = pyproject.read_text()
    toml = inject_ep(
        toml,
        "parsers",
        _DEMO_PARSER_EP,
        f"{pkg_name}.parsers.sandbox_ohlcv_dto_parser:SandboxOhlcvDTOParser",
    )
    toml = inject_ep(
        toml,
        "mappers",
        _DEMO_MAPPER_EP,
        f"{pkg_name}.mappers.map_sandbox_ohlcv_dto_to_equity:map_sandbox_ohlcv_dto_to_equity",
    )
    pyproject.write_text(toml)


def _update_workspace_jerry(
    workspace_root: Path,
    plugin_root_rel: Path,
    dataset_path: Path,
) -> None:
    workspace_jerry = workspace_root / "jerry.yaml"
    if not workspace_jerry.exists():
        return
    data = load_yaml(workspace_jerry)
    datasets = data.get("datasets") or {}
    demo_path = (plugin_root_rel / dataset_path).as_posix()
    datasets[_DEMO_DATASET_ALIAS] = demo_path
    # Drop skeleton placeholders that point into this demo plugin.
    for key in ("your-dataset", "interim-builder"):
        path = datasets.get(key)
        if isinstance(path, str) and path.startswith(plugin_root_rel.as_posix()):
            datasets.pop(key, None)
    data["datasets"] = datasets
    data["default_dataset"] = _DEMO_DATASET_ALIAS
    workspace_jerry.write_text(
        yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
    )


def _copy_tree(src: Path, dest: Path) -> None:
    if src.is_dir():
        shutil.copytree(src, dest, dirs_exist_ok=True)
    else:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)


def scaffold_demo(root: Path | None = None) -> None:
    root_dir, pkg_name, pyproject = pkg_root(root)
    demo_ref = files("datapipeline") / "templates" / "demo_skeleton"
    with as_file(demo_ref) as demo_dir:
        demo_data = demo_dir / "demo"
        demo_pkg = demo_dir / "src" / "{{PACKAGE_NAME}}"

        target_demo = root_dir / "demo"
        if target_demo.exists():
            logger.error("`%s` already exists", target_demo)
            raise SystemExit(1)

        _copy_tree(demo_data, target_demo)

        dest_pkg = resolve_base_pkg_dir(root_dir, pkg_name)
        for item in demo_pkg.iterdir():
            _copy_tree(item, dest_pkg / item.name)

    for cleanup in ("your-dataset", "your-interim-data-builder"):
        extra = root_dir / cleanup
        if extra.exists():
            shutil.rmtree(extra)

    replacements = {
        "{{PACKAGE_NAME}}": pkg_name,
    }
    for p in target_demo.rglob("*"):
        _replace_placeholders(p, replacements)
    for p in dest_pkg.rglob("*"):
        _replace_placeholders(p, replacements)

    _inject_demo_entrypoints(pyproject, pkg_name)

    workspace_root = workspace_cwd()
    plugin_root_rel = relative_to_workspace(root_dir, workspace_root)

    _update_workspace_jerry(
        workspace_root,
        plugin_root_rel,
        Path("demo/project.yaml"),
    )

    logger.info("demo dataset created at %s", target_demo)
