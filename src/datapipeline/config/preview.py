from typing import Literal


PreviewStage = Literal[
    "input",
    "canonical",
    "records",
    "features",
    "samples",
    "postprocess",
]

PREVIEW_STAGES: tuple[PreviewStage, ...] = (
    "input",
    "canonical",
    "records",
    "features",
    "samples",
    "postprocess",
)
