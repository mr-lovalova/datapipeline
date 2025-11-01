import os

PARSERS_EP = os.getenv("DP_PARSERS_EP",    "datapipeline.parsers")
LOADERS_EP = os.getenv("DP_LOADERS_EP",    "datapipeline.loaders")
MAPPERS_EP = os.getenv("DP_MAPPERS_EP",    "datapipeline.mappers")
FILTERS_EP = os.getenv("DP_FILTERS_EP",    "datapipeline.filters")

RECORD_TRANSFORMS_EP = os.getenv(
    "DP_RECORD_TRANSFORMS_EP", "datapipeline.transforms.record")
STREAM_TRANFORMS_EP = os.getenv(
    "DP_STREAM_TRANSFORMS_EP", "datapipeline.transforms.stream")
FEATURE_TRANSFORMS_EP = os.getenv(
    "DP_FEATURE_TRANSFORMS_EP", "datapipeline.transforms.feature")
VECTOR_TRANSFORMS_EP = os.getenv(
    "DP_VECTOR_TRANSFORMS_EP", "datapipeline.transforms.vector"
)

# Optional debug transforms applied after stream transforms.
DEBUG_TRANSFORMS_EP = os.getenv(
    "DP_DEBUG_TRANSFORMS_EP", "datapipeline.transforms.debug"
)
