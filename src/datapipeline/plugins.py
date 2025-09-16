import os

PARSERS_EP = os.getenv("DP_PARSERS_EP",    "datapipeline.parsers")
LOADERS_EP = os.getenv("DP_LOADERS_EP",    "datapipeline.loaders")
MAPPERS_EP = os.getenv("DP_MAPPERS_EP",    "datapipeline.mappers")
FILTERS_EP = os.getenv("DP_FILTERS_EP",    "datapipeline.filters")
TRANSFORMS_EP = os.getenv("DP_TRANSFORMS_EP", "datapipeline.transforms")
