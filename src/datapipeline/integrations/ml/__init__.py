"""Adapters that bridge pipeline iterators into ML-friendly shapes."""

from .adapter import GroupFormat, VectorAdapter
from .pandas_support import dataframe_from_vectors
from .rows import collect_vector_rows, iter_vector_rows, stream_vectors
from .torch_support import torch_dataset

__all__ = [
    "GroupFormat",
    "VectorAdapter",
    "collect_vector_rows",
    "dataframe_from_vectors",
    "iter_vector_rows",
    "stream_vectors",
    "torch_dataset",
]
