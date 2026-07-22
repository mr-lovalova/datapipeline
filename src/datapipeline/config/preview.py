from typing import Literal


PreviewStage = Literal[
    "input",
    "canonical",
    "records",
    "series",
    "samples",
    "postprocess",
]

PREVIEW_STAGES: tuple[PreviewStage, ...] = (
    "input",
    "canonical",
    "records",
    "series",
    "samples",
    "postprocess",
)
