from typing import Literal


PreviewStage = Literal[
    "source",
    "mapped",
    "records",
    "features",
    "samples",
    "postprocess",
]

PREVIEW_STAGES: tuple[PreviewStage, ...] = (
    "source",
    "mapped",
    "records",
    "features",
    "samples",
    "postprocess",
)
