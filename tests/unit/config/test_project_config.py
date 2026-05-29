import pytest
from pydantic import ValidationError

from datapipeline.config.project import ProjectConfig
from datapipeline.services.config_refs import project_vars_from_data


def _project_data(**overrides):
    data = {
        "version": 1,
        "name": "momentum",
        "paths": {
            "ingests": "ingests",
            "streams": "streams",
            "sources": "sources",
            "dataset": "dataset.yaml",
            "postprocess": "postprocess.yaml",
            "artifacts": "artifacts",
        },
    }
    data.update(overrides)
    return data


def test_project_config_accepts_variant() -> None:
    cfg = ProjectConfig.model_validate(_project_data(variant="long"))

    assert cfg.variant == "long"


def test_project_config_accepts_multiple_discovery_roots() -> None:
    cfg = ProjectConfig.model_validate(
        _project_data(
            paths={
                "ingests": ["ingests", "../common/ingests"],
                "streams": ["streams", "../common/streams"],
                "sources": ["sources", "../common/sources"],
                "dataset": "dataset.yaml",
                "postprocess": "postprocess.yaml",
                "artifacts": "artifacts",
            }
        )
    )

    assert cfg.paths.ingests == ["ingests", "../common/ingests"]
    assert cfg.paths.streams == ["streams", "../common/streams"]
    assert cfg.paths.sources == ["sources", "../common/sources"]


def test_project_config_rejects_empty_discovery_roots() -> None:
    with pytest.raises(ValidationError, match="project path lists must not be empty"):
        ProjectConfig.model_validate(
            _project_data(
                paths={
                    "ingests": [],
                    "streams": "streams",
                    "sources": "sources",
                    "dataset": "dataset.yaml",
                    "postprocess": "postprocess.yaml",
                    "artifacts": "artifacts",
                }
            )
        )


def test_project_variant_is_available_for_interpolation() -> None:
    vars_ = project_vars_from_data(_project_data(variant="long"))

    assert vars_["variant"] == "long"
    assert vars_["project_variant"] == "long"


def test_project_config_rejects_unknown_top_level_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ProjectConfig.model_validate(_project_data(varaint="long"))


def test_project_version_is_schema_version_one() -> None:
    with pytest.raises(ValidationError, match="Input should be 1"):
        ProjectConfig.model_validate(_project_data(version="v1-champion"))
