import glob
import hashlib
import stat
from collections.abc import Iterable, Mapping
from pathlib import Path

from datapipeline.config.catalog import SourceConfig
from datapipeline.services.config_refs import (
    collect_config_ref_keys,
    interpolate_config_vars,
    merged_project_env,
    project_vars_from_data,
    resolve_config_refs,
)
from datapipeline.services.path_policy import resolve_project_path
from datapipeline.services.project_paths import read_project
from datapipeline.utils.load import load_yaml


def _normalized_label(path: Path, base_dir: Path) -> str:
    try:
        return str(path.resolve().relative_to(base_dir))
    except ValueError:
        return str(path.resolve())


def _hash_file(hasher, path: Path, base_dir: Path) -> None:
    hasher.update(_normalized_label(path, base_dir).encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(path.read_bytes())
    hasher.update(b"\0")


def _hash_env_refs(hasher, project_yaml: Path, config_files: list[Path]) -> None:
    env_ref_names: set[str] = set()
    for path in config_files:
        data = load_yaml(path, require_mapping=False)
        env_ref_names.update(
            key for _scheme, key in collect_config_ref_keys(data, scheme="env")
        )

    if not env_ref_names:
        return

    env = merged_project_env(project_yaml.resolve())
    for name in sorted(env_ref_names):
        hasher.update(f"[env]{name}".encode("utf-8"))
        hasher.update(b"\0")
        value = env.get(name)
        if value is None:
            hasher.update(b"[missing]")
        else:
            hasher.update(str(value).encode("utf-8"))
        hasher.update(b"\0")


def _source_label(path: Path, base_dir: Path) -> str:
    try:
        return str(path.relative_to(base_dir))
    except ValueError:
        return str(path)


def _hash_source_file(hasher, path: Path, base_dir: Path) -> None:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        state = "missing"
    else:
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"Source input is not a regular file: {path}")
        state = f"file:{metadata.st_size}:{metadata.st_mtime_ns}:{metadata.st_ctime_ns}"
    snapshot = (
        f"{_source_label(path, base_dir)}\0"
        f"{_normalized_label(path, base_dir)}\0{state}\0"
    )
    hasher.update(snapshot.encode("utf-8"))


def _hash_source_pattern(
    hasher,
    pattern: Path,
    *,
    expands_glob: bool,
    base_dir: Path,
) -> None:
    hasher.update(f"[source]{_source_label(pattern, base_dir)}\0".encode("utf-8"))
    if expands_glob:
        matches = [Path(match) for match in sorted(glob.glob(str(pattern)))]
        if not matches:
            hasher.update(b"[no-matches]")
            return
    else:
        matches = [pattern]
    for path in matches:
        _hash_source_file(hasher, path, base_dir)


def _project_source_path(project_yaml: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project_yaml.parent.resolve() / path


def _source_input_patterns(
    source: SourceConfig,
    project_yaml: Path,
) -> Iterable[tuple[Path, bool]]:
    loader = source.loader
    if (
        loader.entrypoint == "core.io"
        and str(loader.args.get("transport") or "").lower() == "fs"
    ):
        raw_path = loader.args.get("path")
        if isinstance(raw_path, str) and raw_path:
            yield (
                _project_source_path(project_yaml, raw_path),
                glob.has_magic(raw_path),
            )
    if source.inputs is not None:
        for raw_path in source.inputs.files:
            yield _project_source_path(project_yaml, raw_path), glob.has_magic(raw_path)


def _hash_source_inputs(
    hasher,
    *,
    project_yaml: Path,
    source_files: list[Path],
    base_dir: Path,
) -> None:
    project_data = resolve_config_refs(
        load_yaml(project_yaml), project_yaml=project_yaml
    )
    if not isinstance(project_data, Mapping):
        return
    project_vars = project_vars_from_data(project_data)

    for source_file in source_files:
        data = resolve_config_refs(load_yaml(source_file), project_yaml=project_yaml)
        if (
            not isinstance(data, Mapping)
            or not isinstance(data.get("parser"), Mapping)
            or not isinstance(data.get("loader"), Mapping)
        ):
            continue
        source = SourceConfig.model_validate(
            interpolate_config_vars(data, project_vars)
        )
        for path, expands_glob in _source_input_patterns(source, project_yaml):
            _hash_source_pattern(
                hasher,
                path,
                expands_glob=expands_glob,
                base_dir=base_dir,
            )


def _yaml_files(directory: Path) -> Iterable[Path]:
    if not directory.exists():
        return []
    return sorted(p for p in directory.rglob("*.y*ml") if p.is_file())


def compute_config_hash(project_yaml: Path, tasks_path: Path) -> str:
    """Hash project configuration and declared local source snapshots."""

    hasher = hashlib.sha256()
    base_dir = project_yaml.parent.resolve()
    cfg = read_project(project_yaml)

    required = [
        project_yaml.resolve(),
        resolve_project_path(project_yaml, cfg.paths.dataset),
        resolve_project_path(project_yaml, cfg.paths.postprocess),
    ]
    hashed_files: list[Path] = []

    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Expected config file missing: {path}")
        _hash_file(hasher, path, base_dir)
        hashed_files.append(path)

    if not tasks_path.is_dir():
        raise TypeError(
            f"project.paths.tasks must point to a directory, got: {tasks_path}"
        )
    hasher.update(f"[dir]{_normalized_label(tasks_path, base_dir)}".encode("utf-8"))
    for p in _yaml_files(tasks_path):
        _hash_file(hasher, p, base_dir)
        hashed_files.append(p)

    for dir_value in (cfg.paths.ingests, cfg.paths.sources, cfg.paths.streams):
        dir_values = dir_value if isinstance(dir_value, list) else [dir_value]
        for raw_path in dir_values:
            directory = resolve_project_path(project_yaml, raw_path)
            hasher.update(
                f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8")
            )
            if not directory.exists():
                hasher.update(b"[missing]")
                continue
            for path in _yaml_files(directory):
                _hash_file(hasher, path, base_dir)
                hashed_files.append(path)

    _hash_env_refs(hasher, project_yaml, hashed_files)

    source_files: list[Path] = []
    source_roots = (
        cfg.paths.sources
        if isinstance(cfg.paths.sources, list)
        else [cfg.paths.sources]
    )
    for raw_path in source_roots:
        directory = resolve_project_path(project_yaml, raw_path)
        source_files.extend(_yaml_files(directory))
    _hash_source_inputs(
        hasher,
        project_yaml=project_yaml,
        source_files=source_files,
        base_dir=base_dir,
    )

    return hasher.hexdigest()
