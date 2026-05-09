from pathlib import Path

from datapipeline.services.project_paths import streams_dir as resolve_streams_dir, ensure_project_scaffold
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import status


def write_ingest_contract(
    *,
    project_yaml: Path,
    stream_id: str,
    source: str,
    mapper_entrypoint: str,
) -> Path:
    ensure_project_scaffold(project_yaml)
    streams_path = resolve_streams_dir(project_yaml)
    streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
    streams_dir.mkdir(parents=True, exist_ok=True)
    cfile = streams_dir / f"{stream_id}.yaml"
    cfile.write_text(
        render(
            "contracts/ingest.yaml.j2",
            source=source,
            stream_id=stream_id,
            mapper_entrypoint=mapper_entrypoint,
        ),
        encoding="utf-8",
    )
    status("new", f"canonical spec: {cfile}")
    return cfile


def write_manual_contract(
    *,
    project_yaml: Path,
    stream_id: str,
    inputs_list: str,
    mapper_entrypoint: str,
    driver_key: str,
) -> Path:
    ensure_project_scaffold(project_yaml)
    streams_path = resolve_streams_dir(project_yaml)
    streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
    streams_dir.mkdir(parents=True, exist_ok=True)
    cfile = streams_dir / f"{stream_id}.yaml"
    cfile.write_text(
        render(
            "contracts/manual.yaml.j2",
            stream_id=stream_id,
            inputs_list=inputs_list,
            mapper_entrypoint=mapper_entrypoint,
            driver_key=driver_key,
        ).strip() + "\n",
        encoding="utf-8",
    )
    status("new", f"manual contract: {cfile}")
    return cfile


def write_joined_contract(
    *,
    project_yaml: Path,
    stream_id: str,
    inputs_list: str,
    mapper_entrypoint: str,
    primary_key: str,
    broadcast_keys: list[str] | None = None,
) -> Path:
    ensure_project_scaffold(project_yaml)
    streams_path = resolve_streams_dir(project_yaml)
    streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
    streams_dir.mkdir(parents=True, exist_ok=True)
    cfile = streams_dir / f"{stream_id}.yaml"
    cfile.write_text(
        render(
            "contracts/joined.yaml.j2",
            stream_id=stream_id,
            inputs_list=inputs_list,
            mapper_entrypoint=mapper_entrypoint,
            primary_key=primary_key,
            broadcast_keys=broadcast_keys or [],
        ).strip() + "\n",
        encoding="utf-8",
    )
    status("new", f"joined contract: {cfile}")
    return cfile


def format_inputs(picked: list[str]) -> tuple[str, str]:
    """Build input list text and default primary/driver key."""
    built: list[str] = []
    for ref in picked:
        parts = ref.split(".")
        if len(parts) >= 3:
            domain, variant = parts[0], parts[-1]
            alias = f"{domain}_{variant}"
        elif len(parts) == 2:
            alias = parts[0]
        else:
            alias = ref
        built.append(f"{alias}={ref}")
    inputs_csv = ",".join(built)
    inputs_list = "\n  - ".join(s.strip() for s in inputs_csv.split(",") if s.strip())
    driver_key = inputs_csv.split(",")[0].split("=")[0].strip()
    return inputs_list, driver_key
