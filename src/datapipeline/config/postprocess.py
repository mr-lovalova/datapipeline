from typing import Any, List
from pydantic import RootModel


class PostprocessConfig(RootModel[List[Any]]):
    """Schema for optional postprocess.yaml."""
