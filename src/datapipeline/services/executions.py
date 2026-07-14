from datetime import datetime, timezone
from pathlib import Path


def execution_root(artifacts_root: Path) -> Path:
    execution_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    return (artifacts_root / "_system" / "executions" / execution_id).resolve()
