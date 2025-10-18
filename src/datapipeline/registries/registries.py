from typing import Any, Mapping, Optional, Sequence, Union, List
from datapipeline.sources.models.source import Source
from datapipeline.registries.registry import Registry

sources: Registry[str, Source] = Registry()
mappers: Registry[str, Any] = Registry()
stream_sources: Registry[str, Any] = Registry()
record_operations: Registry[str, Sequence[Mapping[str, object]]] = Registry()
feature_transforms: Registry[str, Sequence[Mapping[str, object]]] = Registry()
postprocesses: Registry[str, Any] = Registry()

# Per-stream policy registries
stream_operations: Registry[str, Sequence[Mapping[str, object]]] = Registry()
debug_operations: Registry[str, Sequence[Mapping[str, object]]] = Registry()
partition_by: Registry[str, Optional[Union[str, List[str]]]] = Registry()
sort_batch_size: Registry[str, int] = Registry()
