import os

PARSERS_EP = os.getenv("DP_PARSERS_EP", "datapipeline.parsers")
LOADERS_EP = os.getenv("DP_LOADERS_EP", "datapipeline.loaders")
MAPPERS_EP = os.getenv("DP_MAPPERS_EP", "datapipeline.mappers")
BUILD_OPERATIONS_EP = os.getenv(
    "DP_BUILD_OPERATIONS_EP", "datapipeline.operations.build"
)
RUNTIME_OPERATIONS_EP = os.getenv(
    "DP_RUNTIME_OPERATIONS_EP", "datapipeline.operations.runtime"
)
