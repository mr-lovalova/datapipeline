# Python Integrations

`datapipeline.integrations.ml` demonstrates how to reuse the runtime from
application code:

- `VectorAdapter.from_project(project_yaml)` – bootstrap once, then stream
  vectors or row dicts.
- `stream_vectors(project_yaml, limit=...)` – iterator matching `jerry serve`.
- `iter_vector_rows` / `collect_vector_rows` – handy for Pandas or custom sinks.
- `dataframe_from_vectors` – eager helper that returns a Pandas DataFrame
  (requires `pandas`).
- `torch_dataset` – builds a `torch.utils.data.Dataset` that yields tensors. After
  running `jerry demo init`, see `demo/scripts/run_torch.py` for usage (install
  `jerry-thomas[ml]` first).

---
