from pathlib import Path

from datapipeline.config.streams import AlignedStreamConfig, SourceStreamConfig
from datapipeline.services.project_paths import ensure_project_scaffold
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import status


def write_source_stream(
    project_yaml: Path,
    stream_id: str,
    source: str,
    mapper_entrypoint: str,
) -> Path:
    config = SourceStreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {"source": source},
            "map": {"entrypoint": mapper_entrypoint},
        }
    )
    project = ensure_project_scaffold(project_yaml)
    streams_dir = project.stream_dirs[0]
    path = streams_dir / f"{config.id}.yaml"
    content = render(
        "streams/source.yaml.j2",
        source=config.from_.source,
        stream_id=config.id,
        mapper_entrypoint=config.map.entrypoint,
    )
    with path.open("x", encoding="utf-8") as stream_file:
        stream_file.write(content)
    status("new", f"source stream spec: {path}")
    return path


def write_aligned_stream(
    project_yaml: Path,
    stream_id: str,
    input_streams: list[str],
    combine_entrypoint: str,
) -> Path:
    config = AlignedStreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {"align": input_streams},
            "combine": {"entrypoint": combine_entrypoint},
        }
    )
    project = ensure_project_scaffold(project_yaml)
    streams_dir = project.stream_dirs[0]
    path = streams_dir / f"{config.id}.yaml"
    content = (
        render(
            "streams/aligned.yaml.j2",
            stream_id=config.id,
            input_streams=list(config.from_.align),
            combine_entrypoint=config.combine.entrypoint,
        ).strip()
        + "\n"
    )
    with path.open("x", encoding="utf-8") as stream_file:
        stream_file.write(content)
    status("new", f"aligned stream spec: {path}")
    return path
