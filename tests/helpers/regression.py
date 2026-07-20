import json
from pathlib import Path
from typing import Literal

from datapipeline.config.profiles import ServeOutputConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.profiles.models import RuntimeRunRequest
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.profiles.request_builder import build_runtime_run_request


def serve_dataset(
    project_root: Path,
    artifact_mode: Literal["AUTO", "FORCE"] = "FORCE",
    cli_output: ServeOutputConfig | None = None,
    preview: PreviewStage | None = None,
) -> RuntimeRunRequest:
    request = build_runtime_run_request(
        "serve",
        str(project_root / "project.yaml"),
        profile_name="dataset",
        artifact_mode=artifact_mode,
        preview=preview,
        cli_output=cli_output,
        cli_visuals="off",
        cli_log_level="CRITICAL",
    )
    assert request is not None
    assert len(request.serve_run_plans) == 1
    run_profiles(request)
    return request


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
