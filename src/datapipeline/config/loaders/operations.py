from pathlib import Path

from datapipeline.config.model_utils import normalize_required_text
from datapipeline.config.tasks import (
    ArtifactTask,
    MaterializeStreamTask,
    MetadataTask,
    OperationTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
    Task,
    TicksTask,
    VectorInputsTask,
)
from datapipeline.services.project_paths import tasks_dir

from .common import ensure_unique_specs, load_specs, spec_files

ARTIFACT_OPERATION_MODELS: dict[str, type[ArtifactTask]] = {
    "vector_inputs": VectorInputsTask,
    "schema": SchemaTask,
    "scaler": ScalerTask,
    "metadata": MetadataTask,
    "stats": StatsTask,
}
TICKS_ENTRYPOINT = "core.artifact.ticks"


def _load_operation_entry(entry: dict) -> Task:
    normalized_entry = dict(entry)
    operation_kind = normalize_required_text(
        normalized_entry.get("kind"),
        field_name="kind",
        lower=True,
    )
    normalized_entry["kind"] = operation_kind
    if operation_kind == "runtime":
        entrypoint = normalize_required_text(
            normalized_entry.get("entrypoint"),
            field_name="entrypoint",
        )
        normalized_entry["entrypoint"] = entrypoint
        if entrypoint == "core.runtime.materialize_stream":
            return MaterializeStreamTask.model_validate(normalized_entry)
        return OperationTask.model_validate(normalized_entry)
    if operation_kind != "artifact":
        raise ValueError(
            f"Unsupported task kind '{operation_kind}'. Use 'artifact' or 'runtime'."
        )
    operation_id = normalize_required_text(
        normalized_entry.get("id"),
        field_name="id",
        lower=True,
    )
    normalized_entry["id"] = operation_id
    raw_entrypoint = normalized_entry.get("entrypoint")
    if raw_entrypoint is not None:
        entrypoint = normalize_required_text(
            raw_entrypoint,
            field_name="entrypoint",
        )
        normalized_entry["entrypoint"] = entrypoint
        if entrypoint == TICKS_ENTRYPOINT:
            if operation_id in ARTIFACT_OPERATION_MODELS:
                raise ValueError(
                    f"Ticks artifact task id '{operation_id}' is reserved for "
                    "a built-in artifact task."
                )
            return TicksTask.model_validate(normalized_entry)
    model_cls = ARTIFACT_OPERATION_MODELS.get(operation_id, ArtifactTask)
    return model_cls.model_validate(normalized_entry)


def _validate_operation_layout(root: Path) -> None:
    if not root.exists() or root.is_file():
        return
    root_files = sorted(path for path in root.glob("*.y*ml") if path.is_file())
    if not root_files:
        return
    listed = ", ".join(path.name for path in root_files)
    raise ValueError(
        "Operation task files must live under tasks/operations (profiles live under profiles/); "
        f"found root-level task files: {listed}"
    )


def _load_operation_specs(project_yaml: Path) -> list[Task]:
    root = tasks_dir(project_yaml)
    _validate_operation_layout(root)
    operations_root = root / "operations"
    specs: list[Task] = []
    for path in spec_files(operations_root):
        specs.extend(load_specs(path, _load_operation_entry))
    return specs


def operation_specs(
    project_yaml: Path,
) -> tuple[list[ArtifactTask], list[OperationTask]]:
    specs = _load_operation_specs(project_yaml)
    artifact_specs: list[ArtifactTask] = [
        spec for spec in specs if isinstance(spec, ArtifactTask)
    ]
    operation_task_specs: list[OperationTask] = [
        spec for spec in specs if isinstance(spec, OperationTask)
    ]

    ensure_unique_specs(
        artifact_specs,
        error_template="Duplicate artifact task ids are not allowed: {details}",
        key_fn=lambda spec: spec.id,
    )
    ensure_unique_specs(
        operation_task_specs,
        error_template="Duplicate operation task ids are not allowed: {details}",
        key_fn=lambda spec: spec.id,
    )
    overlap = sorted(
        {spec.id for spec in artifact_specs}
        & {spec.id for spec in operation_task_specs}
    )
    if overlap:
        raise ValueError(
            "Task ids must be globally unique across artifact/runtime tasks: "
            + ", ".join(overlap)
        )
    return artifact_specs, operation_task_specs


__all__ = ["operation_specs"]
