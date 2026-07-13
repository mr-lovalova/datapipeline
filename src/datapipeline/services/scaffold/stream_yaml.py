from pathlib import Path

from datapipeline.services.project_paths import (
    ingests_dir as resolve_ingests_dir,
    streams_dir as resolve_streams_dir,
    ensure_project_scaffold,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import status


def write_ingest_stream(
    *,
    project_yaml: Path,
    stream_id: str,
    source: str,
    mapper_entrypoint: str,
) -> Path:
    ensure_project_scaffold(project_yaml)
    ingests_path = resolve_ingests_dir(project_yaml)
    ingests_dir = ingests_path if ingests_path.is_dir() else ingests_path.parent
    ingests_dir.mkdir(parents=True, exist_ok=True)
    cfile = ingests_dir / f"{stream_id}.yaml"
    cfile.write_text(
        render(
            "ingests/ingest.yaml.j2",
            source=source,
            stream_id=stream_id,
            mapper_entrypoint=mapper_entrypoint,
        ),
        encoding="utf-8",
    )
    status("new", f"ingest spec: {cfile}")
    return cfile


def write_aligned_stream(
    project_yaml: Path,
    stream_id: str,
    input_streams: list[str],
    combine_entrypoint: str,
) -> Path:
    ensure_project_scaffold(project_yaml)
    streams_path = resolve_streams_dir(project_yaml)
    streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
    streams_dir.mkdir(parents=True, exist_ok=True)
    cfile = streams_dir / f"{stream_id}.yaml"
    cfile.write_text(
        render(
            "streams/aligned.yaml.j2",
            stream_id=stream_id,
            input_streams=input_streams,
            combine_entrypoint=combine_entrypoint,
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    status("new", f"aligned stream spec: {cfile}")
    return cfile
