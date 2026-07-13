import os
from pathlib import Path

import pytest

from datapipeline.build.config_hash import compute_config_hash
from datapipeline.config.catalog import SourceConfig
from datapipeline.services.bootstrap.config import (
    _globals,
    _interpolate,
    _load_by_key,
    artifacts_root,
)
from datapipeline.services.bootstrap.core import _load_sources_from_dir
from datapipeline.services.project_paths import sources_dir
from datapipeline.services.streams.ingest import build_source_from_spec
from datapipeline.utils.placeholders import is_missing


def _write_project_yaml(
    project_root: Path,
    *,
    artifacts_value: str = "build",
    variant: str | None = None,
    globals_lines: list[str] | None = None,
) -> Path:
    project_yaml = project_root / "project.yaml"
    lines = [
        "version: 1",
        "name: sample",
    ]
    if variant is not None:
        lines.append(f"variant: {variant}")
    lines.extend(
        [
            "paths:",
            "  ingests: ./ingests",
            "  streams: streams",
            "  sources: sources",
            "  dataset: dataset.yaml",
            "  postprocess: postprocess.yaml",
            f"  artifacts: {artifacts_value}",
            "  tasks: tasks",
        ]
    )
    if globals_lines:
        lines.append("globals:")
        lines.extend(f"  {line}" for line in globals_lines)
    else:
        lines.append("globals: {}")

    project_yaml.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return project_yaml


def _write_project_files(project_root: Path) -> None:
    (project_root / "streams").mkdir(parents=True, exist_ok=True)
    (project_root / "sources").mkdir(parents=True, exist_ok=True)
    (project_root / "tasks").mkdir(parents=True, exist_ok=True)
    (project_root / "dataset.yaml").write_text("value: default\n", encoding="utf-8")
    (project_root / "postprocess.yaml").write_text("{}\n", encoding="utf-8")


def test_globals_resolve_env_refs_from_project_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_ROOT=../shared-data\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        artifacts_value="${raw_root}/artifacts",
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )

    assert _globals(project_yaml)["raw_root"] == "../shared-data"
    assert (
        artifacts_root(project_yaml)
        == (project_root / "../shared-data" / "artifacts").resolve()
    )


def test_process_env_overrides_project_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_ROOT=../shared-data\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )
    monkeypatch.setenv("RAW_ROOT", "/runtime/raw")

    assert _globals(project_yaml)["raw_root"] == "/runtime/raw"


def test_project_variant_can_be_used_in_project_paths(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        variant="long",
        artifacts_value="build/${project_variant}",
    )

    assert artifacts_root(project_yaml) == (project_root / "build" / "long").resolve()


def test_globals_can_reference_other_globals(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=[
            "canonical_root: data/canonical",
            "canonical_equity_interim_dir: ${canonical_root}/equity/interim",
        ],
    )

    globals_ = _globals(project_yaml)

    assert globals_["canonical_equity_interim_dir"] == ("data/canonical/equity/interim")


def test_load_sources_resolve_nested_globals_before_fs_path_normalization(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    data_dir = project_root / "data" / "canonical" / "equity" / "interim"
    data_dir.mkdir(parents=True)
    (data_dir / "prices.jsonl").write_text("{}", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=[
            "canonical_root: data/canonical",
            "canonical_equity_interim_dir: ${canonical_root}/equity/interim",
        ],
    )
    (project_root / "sources" / "prices.yaml").write_text(
        "\n".join(
            [
                "id: equity.prices",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: ${canonical_equity_interim_dir}/prices.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = _load_sources_from_dir(project_yaml, vars_=_globals(project_yaml))
    source = build_source_from_spec(
        SourceConfig.model_validate(loaded["equity.prices"]),
        project_yaml=project_yaml,
    )

    assert source.loader.transport.path == str((data_dir / "prices.jsonl").resolve())


def test_cyclic_global_reference_raises_clear_error(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=[
            "left: ${right}",
            "right: ${left}",
        ],
    )

    with pytest.raises(ValueError, match="Cyclic project global reference"):
        _globals(project_yaml)


def test_global_reference_to_null_stays_missing(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=[
            "optional_root:",
            "derived_root: ${optional_root}",
        ],
    )

    globals_ = _globals(project_yaml)

    assert globals_["optional_root"] is None
    assert is_missing(globals_["derived_root"])


def test_full_placeholder_interpolation_preserves_global_value_type(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=[
            "adv_window_days: 21",
            "annualization_factor: 15.874507866387544",
        ],
    )

    interpolated = _interpolate(
        {
            "stream": [
                {
                    "operation": "rolling",
                    "field": "dollar_volume",
                    "window": "${adv_window_days}",
                    "to": "adv_${adv_window_days}",
                },
                {
                    "operation": "derive",
                    "left": "return_pstdev_63",
                    "operator": "mul",
                    "right_value": "${annualization_factor}",
                    "to": "volatility_63",
                },
            ]
        },
        _globals(project_yaml),
    )

    rolling = interpolated["stream"][0]
    derive = interpolated["stream"][1]
    assert rolling["window"] == 21
    assert isinstance(rolling["window"], int)
    assert rolling["to"] == "adv_21"
    assert derive["right_value"] == 15.874507866387544
    assert isinstance(derive["right_value"], float)


def test_load_by_key_resolves_direct_env_refs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(project_root)
    (project_root / "dataset.yaml").write_text(
        "value: ${env:DATASET_VALUE}\n", encoding="utf-8"
    )
    monkeypatch.setenv("DATASET_VALUE", "records")

    loaded = _load_by_key(project_yaml, "dataset")

    assert loaded["value"] == "records"


def test_load_sources_resolve_env_backed_globals_before_fs_path_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_GLOB", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_GLOB=data/*.jsonl\n", encoding="utf-8")
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_glob: ${env:RAW_GLOB}"],
    )
    (project_root / "sources" / "sample.yaml").write_text(
        "\n".join(
            [
                "id: sample.fs",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: ${raw_glob}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = _load_sources_from_dir(project_yaml, vars_=_globals(project_yaml))
    source = build_source_from_spec(
        SourceConfig.model_validate(loaded["sample.fs"]),
        project_yaml=project_yaml,
    )

    assert source.loader.transport.pattern == str(
        (project_root / "data" / "*.jsonl").resolve()
    )


def test_missing_env_ref_raises_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_ROOT", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:MISSING_ROOT}"],
    )

    with pytest.raises(ValueError, match="MISSING_ROOT"):
        _globals(project_yaml)


def test_sources_dir_resolves_env_backed_project_path_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    raw_root = tmp_path / "shared"
    (raw_root / "sources").mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text(f"RAW_ROOT={raw_root}\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "  sources: sources",
            "  sources: ${raw_root}/sources",
        ),
        encoding="utf-8",
    )

    assert sources_dir(project_yaml) == (raw_root / "sources").resolve()


def test_compute_config_hash_changes_when_env_value_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    _write_project_files(project_root)
    (project_root / "sources" / "sample.yaml").write_text(
        "\n".join(
            [
                "id: sample.fs",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: ${raw_root}/rows.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )

    monkeypatch.setenv("RAW_ROOT", "/tmp/raw-one")
    hash_one = compute_config_hash(project_yaml, project_root / "tasks")

    monkeypatch.setenv("RAW_ROOT", "/tmp/raw-two")
    hash_two = compute_config_hash(project_yaml, project_root / "tasks")

    assert hash_one != hash_two


def test_compute_config_hash_includes_multiple_source_roots(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    common_root = tmp_path / "common"
    _write_project_files(project_root)
    (common_root / "sources").mkdir(parents=True)
    (common_root / "sources" / "common.yaml").write_text(
        "\n".join(
            [
                "id: common.fs",
                "parser:",
                "  entrypoint: identity",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: rows.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    project_yaml = _write_project_yaml(project_root)
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "  sources: sources",
            "  sources:\n    - sources\n    - ../common/sources",
        ),
        encoding="utf-8",
    )

    hash_one = compute_config_hash(project_yaml, project_root / "tasks")

    (common_root / "sources" / "common.yaml").write_text(
        "\n".join(
            [
                "id: common.fs",
                "parser:",
                "  entrypoint: identity",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: changed.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    hash_two = compute_config_hash(project_yaml, project_root / "tasks")

    assert hash_one != hash_two


@pytest.mark.parametrize(
    ("source_yaml", "tracks_inventory"),
    [
        pytest.param(
            """\
id: sample.fs
parser: {entrypoint: identity, args: {}}
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: data/*.jsonl
""",
            True,
            id="core-io",
        ),
        pytest.param(
            """\
id: sample.file
parser: {entrypoint: identity, args: {}}
loader:
  entrypoint: core.io
  args: {transport: fs, format: jsonl, path: data/a.jsonl}
""",
            False,
            id="core-io-file",
        ),
        pytest.param(
            """\
id: sample.custom
inputs:
  files: [data/*.jsonl]
parser: {entrypoint: identity, args: {}}
loader: {entrypoint: custom.loader, args: {}}
""",
            True,
            id="custom-loader",
        ),
    ],
)
def test_compute_config_hash_tracks_local_source_snapshot(
    tmp_path: Path,
    source_yaml: str,
    tracks_inventory: bool,
) -> None:
    project_root = tmp_path / "project"
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(project_root)
    (project_root / "sources" / "sample.yaml").write_text(
        source_yaml,
        encoding="utf-8",
    )
    data_dir = project_root / "data"
    data_dir.mkdir()
    first = data_dir / "a.jsonl"
    first.write_text("one\n", encoding="utf-8")

    baseline = compute_config_hash(project_yaml, project_root / "tasks")
    assert compute_config_hash(project_yaml, project_root / "tasks") == baseline

    (data_dir / "ignored.csv").write_text("ignored\n", encoding="utf-8")
    assert compute_config_hash(project_yaml, project_root / "tasks") == baseline

    previous = first.stat()
    first.write_text("two\n", encoding="utf-8")
    os.utime(
        first,
        ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000),
    )
    edited = compute_config_hash(project_yaml, project_root / "tasks")
    assert edited != baseline

    second = data_dir / "b.jsonl"
    second.write_text("three\n", encoding="utf-8")
    added = compute_config_hash(project_yaml, project_root / "tasks")
    assert (added != edited) is tracks_inventory

    second.unlink()
    assert compute_config_hash(project_yaml, project_root / "tasks") == edited

    first.unlink()
    assert compute_config_hash(project_yaml, project_root / "tasks") != edited


def test_compute_config_hash_rejects_source_input_directory(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(project_root)
    (project_root / "sources" / "sample.yaml").write_text(
        """\
id: sample.custom
inputs: {files: [data]}
parser: {entrypoint: identity, args: {}}
loader: {entrypoint: custom.loader, args: {}}
""",
        encoding="utf-8",
    )
    (project_root / "data").mkdir()

    with pytest.raises(ValueError, match="not a regular file"):
        compute_config_hash(project_yaml, project_root / "tasks")
