from __future__ import annotations

from pathlib import Path
from typing import Annotated, Iterable, List, Literal, Sequence

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.type_adapter import TypeAdapter

from datapipeline.services.project_paths import tasks_dir
from datapipeline.utils.load import load_yaml

VALID_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
VALID_VISUAL_PROVIDERS = ("AUTO", "TQDM", "RICH", "OFF")
VALID_PROGRESS_STYLES = ("AUTO", "SPINNER", "BARS", "OFF")

Transport = Literal["fs", "stdout"]
Format = Literal["csv", "json", "json-lines", "print", "pickle"]
PayloadMode = Literal["sample", "vector"]


class TaskBase(BaseModel):
    version: int = Field(default=1)
    kind: str
    name: str | None = Field(default=None, description="Optional task identifier.")
    enabled: bool = Field(default=True, description="Disable to skip execution.")
    depends_on: list[str] = Field(default_factory=list)
    source_path: Path | None = Field(default=None, exclude=True)

    def effective_name(self) -> str:
        return self.name or (self.source_path.stem if self.source_path else self.kind)


class ArtifactTask(TaskBase):
    output: str = Field(
        ...,
        description="Artifact path relative to project.paths.artifacts.",
    )


class ScalerTask(ArtifactTask):
    kind: Literal["scaler"]
    output: str = Field(default="scaler.pkl")
    split_label: str = Field(
        default="train",
        description="Split label to use when fitting scaler statistics.",
    )


class SchemaTask(ArtifactTask):
    kind: Literal["schema"]
    output: str = Field(default="schema.json")
    cadence_strategy: Literal["max"] = Field(
        default="max",
        description="Strategy for selecting cadence targets (currently only 'max').",
    )


class MetadataTask(ArtifactTask):
    kind: Literal["metadata"]
    output: str = Field(default="metadata.json")
    enabled: bool = Field(
        default=True,
        description="Disable to skip generating the vector metadata artifact.",
    )
    cadence_strategy: Literal["max"] = Field(
        default="max",
        description="Strategy for selecting cadence targets.",
    )
    window_mode: Literal["union", "intersection", "strict", "relaxed"] = Field(
        default="intersection",
        description="Window mode: union (base union), intersection (base intersection), strict (partition intersection), relaxed (partition union).",
    )


class RuntimeTask(TaskBase):
    """Base class for runtime-oriented tasks (serve/evaluate/etc.)."""


class ServeOutputConfig(BaseModel):
    transport: Transport = Field(..., description="fs | stdout")
    format: Format = Field(..., description="csv | json | json-lines | print | pickle")
    payload: PayloadMode = Field(
        default="sample",
        description="sample (key + metadata) or vector payload (features [+targets]).",
    )
    directory: Path | None = Field(
        default=None,
        description="Directory for fs outputs.",
    )
    filename: str | None = Field(
        default=None,
        description="Filename stem (format controls extension) for fs outputs.",
    )

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

    @model_validator(mode="after")
    def _validate(self):
        if self.transport == "stdout":
            if self.directory is not None:
                raise ValueError("stdout cannot define a directory")
            if self.filename is not None:
                raise ValueError("stdout outputs do not support filenames")
            if self.format not in {"print", "json-lines", "json"}:
                raise ValueError(
                    "stdout output supports 'print', 'json-lines', or 'json' formats"
                )
            return self

        if self.format == "print":
            raise ValueError("fs transport cannot use 'print' format")
        if self.directory is None:
            raise ValueError("fs outputs require a directory")
        return self

    @field_validator("payload", mode="before")
    @classmethod
    def _normalize_payload(cls, value):
        if value is None:
            return "sample"
        name = str(value).lower()
        if name not in {"sample", "vector"}:
            raise ValueError("payload must be 'sample' or 'vector'")
        return name


class ServeTask(RuntimeTask):
    kind: Literal["serve"]
    output: ServeOutputConfig | None = None
    keep: str | None = Field(
        default=None,
        description="Active split label to serve.",
        min_length=1,
    )
    limit: int | None = Field(
        default=None,
        description="Default max number of vectors to emit.",
        ge=1,
    )
    stage: int | None = Field(
        default=None,
        description="Default pipeline stage preview (0-7).",
        ge=0,
        le=7,
    )
    throttle_ms: float | None = Field(
        default=None,
        description="Milliseconds to sleep between emitted vectors.",
        ge=0.0,
    )
    log_level: str | None = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    visuals: str | None = Field(
        default="AUTO",
        description="Visuals provider: AUTO, TQDM, RICH, or OFF.",
    )
    progress: str | None = Field(
        default="AUTO",
        description="Progress style: AUTO, SPINNER, BARS, or OFF.",
    )

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str | None) -> str | None:
        if value is None:
            return None
        name = str(value).upper()
        if name not in VALID_LOG_LEVELS:
            raise ValueError(
                f"log_level must be one of {', '.join(VALID_LOG_LEVELS)}, got {value!r}"
            )
        return name

    @field_validator("visuals", mode="before")
    @classmethod
    def _validate_visuals_run(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).upper()
        if name not in VALID_VISUAL_PROVIDERS:
            raise ValueError(
                f"visuals must be one of {', '.join(VALID_VISUAL_PROVIDERS)}, got {value!r}"
            )
        return name

    @field_validator("progress", mode="before")
    @classmethod
    def _validate_progress_run(cls, value):
        if value is None:
            return None
        name = str(value).upper()
        if name not in VALID_PROGRESS_STYLES:
            raise ValueError(
                f"progress must be one of {', '.join(VALID_PROGRESS_STYLES)}, got {value!r}"
            )
        return name

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            raise ValueError("task name cannot be empty")
        return text


TaskModel = Annotated[
    ScalerTask | SchemaTask | MetadataTask | ServeTask,
    Field(discriminator="kind"),
]

TASK_ADAPTER = TypeAdapter(TaskModel)


def _task_files(root: Path) -> Sequence[Path]:
    if not root.exists():
        return []
    if root.is_file():
        return [root]
    return sorted(
        p for p in root.rglob("*.y*ml") if p.is_file()
    )


def _load_task_docs(path: Path) -> list[TaskBase]:
    doc = load_yaml(path)
    if isinstance(doc, list):
        entries = doc
    else:
        entries = [doc]
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


def load_all_tasks(project_yaml: Path) -> list[TaskBase]:
    root = tasks_dir(project_yaml)
    tasks: list[TaskBase] = []
    for path in _task_files(root):
        tasks.extend(_load_task_docs(path))
    return tasks


def artifact_tasks(project_yaml: Path) -> list[ArtifactTask]:
    tasks = [
        task
        for task in load_all_tasks(project_yaml)
        if isinstance(task, ArtifactTask)
    ]
    kinds = {task.kind for task in tasks}
    if "schema" not in kinds:
        tasks.append(SchemaTask(kind="schema"))
    if "scaler" not in kinds:
        tasks.append(ScalerTask(kind="scaler"))
    if "metadata" not in kinds:
        tasks.append(MetadataTask(kind="metadata"))
    return tasks


def command_tasks(project_yaml: Path, kind: str | None = None) -> list[TaskBase]:
    tasks = [
        task
        for task in load_all_tasks(project_yaml)
        if not isinstance(task, ArtifactTask)
    ]
    if kind is None:
        return tasks
    return [task for task in tasks if task.kind == kind]


def serve_tasks(project_yaml: Path) -> list[ServeTask]:
    """Load all serve tasks regardless of enabled state."""
    return [
        task
        for task in command_tasks(project_yaml, kind="serve")
        if isinstance(task, ServeTask)
    ]


def default_serve_task(project_yaml: Path) -> ServeTask | None:
    for task in serve_tasks(project_yaml):
        if task.enabled:
            return task
    return None
