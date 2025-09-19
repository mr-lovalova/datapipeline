"""Adapters that bridge pipeline iterators into ML-friendly shapes."""

from datapipeline.integrations.ml import (
    VectorAdapter,
    collect_vector_rows,
    dataframe_from_vectors,
    iter_vector_rows,
    stream_vectors,
    torch_dataset,
)

__all__ = [
    "VectorAdapter",
    "collect_vector_rows",
    "dataframe_from_vectors",
    "iter_vector_rows",
    "stream_vectors",
    "torch_dataset",
]
