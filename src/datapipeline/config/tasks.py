from dataclasses import dataclass
from pathlib import Path
import codecs
from typing import Annotated, Literal, Sequence

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.type_adapter import TypeAdapter

from datapipeline.config.options import OUTPUT_STDOUT_FORMATS, OUTPUT_VIEWS
from datapipeline.config.observability import ObservabilityConfig
from datapipeline.services.project_paths import tasks_dir
from datapipeline.utils.load import load_yaml

VALID_BUILD_MODES = ("AUTO", "FORCE", "OFF")

Transport = Literal["fs", "stdout"]
Format = Literal["csv", "jsonl", "pickle"]
View = Literal["flat", "raw", "values"]


def _normalize_string_list(
    value,
    *,
    field_name: str,
    lower: bool = False,
) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item).strip()
        if lower:
            text = text.lower()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def _normalize_required_text(
    value,
    *,
    field_name: str,
    lower: bool = False,
) -> str:
    text = str(value).strip() if value is not None else ""
    if lower:
        text = text.lower()
    if not text:
        raise ValueError(f"{field_name} must be set")
    return text


class TaskBase(BaseModel):
    version: int = Field(default=1)
    kind: str
    name: str | None = Field(default=None)
    source_path: Path | None = Field(default=None, exclude=True)

    def effective_name(self) -> str:
        return self.name or (self.source_path.stem if self.source_path else self.kind)


class OperationTask(TaskBase):
    """Concrete executable task definition."""

    entrypoint: str

    @field_validator("entrypoint", mode="before")
    @classmethod
    def _normalize_entrypoint(cls, value):
        return _normalize_required_text(value, field_name="entrypoint")


class DependencyTask(OperationTask):
    dependencies: list[str] = Field(default_factory=list)

    @field_validator("dependencies", mode="before")
    @classmethod
    def _normalize_dependencies(cls, value):
        return _normalize_string_list(
            value,
            field_name="dependencies",
            lower=True,
        )


class ServeOperationTask(DependencyTask):
    kind: Literal["serve_operation"]
    dependencies: list[str] = Field(default_factory=list)


class ArtifactTask(DependencyTask):
    output: str

    @model_validator(mode="after")
    def _validate_dependencies(self):
        if self.kind in set(self.dependencies):
            raise ValueError("dependencies must not include the task itself")
        return self


class ScalerTask(ArtifactTask):
    kind: Literal["scaler"]
    entrypoint: str = Field(default="core.build.scaler")
    output: str = Field(default="scaler.json")
    split_label: str = Field(default="train")


class SchemaTask(ArtifactTask):
    kind: Literal["schema"]
    entrypoint: str = Field(default="core.build.schema")
    output: str = Field(default="schema.json")
    cadence_strategy: Literal["max"] = Field(default="max")


class MetadataTask(ArtifactTask):
    kind: Literal["metadata"]
    entrypoint: str = Field(default="core.build.metadata")
    output: str = Field(default="metadata.json")
    dependencies: list[str] = Field(default_factory=lambda: ["schema"])
    cadence_strategy: Literal["max"] = Field(default="max")
    window_mode: Literal["union", "intersection", "strict", "relaxed"] = Field(default="intersection")


class ProfileTask(TaskBase):
    """Execution profile that selects/how-runs concrete tasks."""

    enabled: bool = Field(default=True)

class ServeOutputConfig(BaseModel):
    transport: Transport
    format: Format
    view: View | None = Field(default=None)
    directory: Path | None = Field(default=None)
    filename: str | None = Field(default=None)
    encoding: str | None = Field(default=None)

    @field_validator("filename", mode="before")
    @classmethod
    def _normalize_filename(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if any(sep in text for sep in ("/", "\\")):
            raise ValueError("filename must not contain path separators")
        if "." in Path(text).name:
            raise ValueError("filename must not include an extension")
        return text

    @field_validator("view", mode="before")
    @classmethod
    def _normalize_view(cls, value):
        if value is None:
            return None
        text = str(value).strip().lower()
        if not text:
            return None
        if text not in set(OUTPUT_VIEWS):
            raise ValueError(
                f"view must be one of {', '.join(repr(x) for x in OUTPUT_VIEWS)}"
            )
        return text

    @field_validator("encoding", mode="before")
    @classmethod
    def _normalize_encoding(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @model_validator(mode="after")
    def _validate(self):
        if self.transport == "stdout":
            if self.directory is not None:
                raise ValueError("stdout cannot define a directory")
            if self.filename is not None:
                raise ValueError("stdout outputs do not support filenames")
            if self.encoding is not None:
                raise ValueError("stdout outputs do not support encoding")
            if self.format not in set(OUTPUT_STDOUT_FORMATS):
                raise ValueError(
                    f"stdout output supports {', '.join(repr(x) for x in OUTPUT_STDOUT_FORMATS)} formats"
                )
        elif self.directory is None:
            raise ValueError("fs outputs require a directory")
        if self.format == "csv" and self.view not in {None, "flat", "values"}:
            raise ValueError("csv output supports only view='flat' or view='values'")
        if self.transport == "fs":
            if self.format == "pickle":
                if self.encoding is not None:
                    raise ValueError("pickle output does not support encoding")
            elif self.format in {"jsonl", "csv"}:
                if self.encoding is None:
                    self.encoding = "utf-8"
                try:
                    codecs.lookup(self.encoding)
                except LookupError as exc:
                    raise ValueError(
                        f"Unknown encoding '{self.encoding}'"
                    ) from exc
        return self

class ServeTask(ProfileTask):
    kind: Literal["serve"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    keep: str | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    stage: int | None = Field(default=None, ge=0)
    throttle_ms: float | None = Field(default=None, ge=0.0)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            raise ValueError("task name cannot be empty")
        return text

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return _normalize_required_text(value, field_name="target")


class BuildTask(ProfileTask):
    kind: Literal["build"]
    observability: ObservabilityConfig | None = Field(default=None)
    mode: str | None = Field(default=None)
    target: str

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).strip().upper()
        if name not in VALID_BUILD_MODES:
            raise ValueError(
                f"mode must be one of {', '.join(VALID_BUILD_MODES)}, got {value!r}"
            )
        return name

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return _normalize_required_text(value, field_name="target", lower=True)


TaskModel = Annotated[
    ScalerTask | SchemaTask | MetadataTask | ServeOperationTask | ServeTask | BuildTask,
    Field(discriminator="kind"),
]

TASK_ADAPTER = TypeAdapter(TaskModel)


def _task_files(root: Path) -> Sequence[Path]:
    if not root.exists():
        return []
    if root.is_file():
        return [root]
    return sorted(p for p in root.rglob("*.y*ml") if p.is_file())


def _load_task_docs(path: Path) -> list[TaskBase]:
    doc = load_yaml(path)
    entries = doc if isinstance(doc, list) else [doc]
    tasks: list[TaskBase] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise TypeError(f"{path} must define mapping tasks.")
        task = TASK_ADAPTER.validate_python(entry)
        task.source_path = path
        if task.name is None:
            task.name = path.stem
        tasks.append(task)
    return tasks


def _load_all_tasks(project_yaml: Path) -> list[TaskBase]:
    root = tasks_dir(project_yaml)
    tasks: list[TaskBase] = []
    for path in _task_files(root):
        tasks.extend(_load_task_docs(path))
    return tasks


@dataclass(frozen=True)
class TaskCatalog:
    artifact_tasks: tuple[ArtifactTask, ...]
    serve_operation_tasks: tuple[ServeOperationTask, ...]
    serve_profiles: tuple[ServeTask, ...]
    build_profiles: tuple[BuildTask, ...]


def _ensure_unique_tasks(
    tasks: Sequence[TaskBase],
    *,
    error_template: str,
    key_fn,
) -> None:
    by_key: dict[str, list[TaskBase]] = {}
    for task in tasks:
        by_key.setdefault(str(key_fn(task)), []).append(task)
    duplicates = {key: items for key, items in by_key.items() if len(items) > 1}
    if not duplicates:
        return
    lines: list[str] = []
    for key, items in sorted(duplicates.items()):
        paths = ", ".join(
            str(path) if path is not None else "<generated>"
            for path in (getattr(item, "source_path", None) for item in items)
        )
        lines.append(f"{key} ({paths})")
    details = "; ".join(lines)
    raise ValueError(error_template.format(details=details))


def _with_default_artifact_tasks(tasks: Sequence[ArtifactTask]) -> list[ArtifactTask]:
    result = list(tasks)
    kinds = {task.kind for task in result}
    if "schema" not in kinds:
        result.append(SchemaTask(kind="schema"))
    if "scaler" not in kinds:
        result.append(ScalerTask(kind="scaler"))
    if "metadata" not in kinds:
        result.append(MetadataTask(kind="metadata"))
    return result


def load_task_catalog(project_yaml: Path) -> TaskCatalog:
    all_tasks = _load_all_tasks(project_yaml)
    artifact = [task for task in all_tasks if isinstance(task, ArtifactTask)]
    serve_ops = [task for task in all_tasks if isinstance(task, ServeOperationTask)]
    serve_profiles = [task for task in all_tasks if isinstance(task, ServeTask)]
    build_profiles = [task for task in all_tasks if isinstance(task, BuildTask)]

    _ensure_unique_tasks(
        artifact,
        error_template="Duplicate artifact task kinds are not allowed: {details}",
        key_fn=lambda task: task.kind,
    )
    _ensure_unique_tasks(
        serve_ops,
        error_template="Duplicate serve operation names are not allowed: {details}",
        key_fn=lambda task: task.effective_name(),
    )
    _ensure_unique_tasks(
        serve_profiles,
        error_template="Duplicate serve profile names are not allowed: {details}",
        key_fn=lambda task: task.effective_name(),
    )
    _ensure_unique_tasks(
        build_profiles,
        error_template="Duplicate build profile names are not allowed: {details}",
        key_fn=lambda task: task.effective_name(),
    )

    return TaskCatalog(
        artifact_tasks=tuple(_with_default_artifact_tasks(artifact)),
        serve_operation_tasks=tuple(serve_ops),
        serve_profiles=tuple(serve_profiles),
        build_profiles=tuple(build_profiles),
    )


def artifact_tasks(project_yaml: Path) -> list[ArtifactTask]:
    return list(load_task_catalog(project_yaml).artifact_tasks)


def serve_tasks(project_yaml: Path) -> list[ServeTask]:
    """Load all serve profiles regardless of enabled state."""
    return list(load_task_catalog(project_yaml).serve_profiles)


def serve_operation_tasks(project_yaml: Path) -> list[ServeOperationTask]:
    return list(load_task_catalog(project_yaml).serve_operation_tasks)


def build_tasks(project_yaml: Path) -> list[BuildTask]:
    return list(load_task_catalog(project_yaml).build_profiles)
