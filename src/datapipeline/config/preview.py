from typing import Literal


PreviewStage = Literal[
    "input",
    "canonical",
    "records",
    "variables",
    "samples",
    "postprocess",
]

PREVIEW_STAGES: tuple[PreviewStage, ...] = (
    "input",
    "canonical",
    "records",
    "variables",
    "samples",
    "postprocess",
)
