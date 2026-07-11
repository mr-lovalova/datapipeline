from datetime import datetime, timezone
from pathlib import Path

from datapipeline.services.bootstrap import artifacts_root


def execution_root(project_yaml: Path) -> Path:
    execution_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    return (
        artifacts_root(project_yaml) / "_system" / "executions" / execution_id
    ).resolve()
