# from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
# from datapipeline.integrations import iter_vector_rows, stream_vectors

# from itertools import islice
# from pathlib import Path
# import sys

# import pytest
# import yaml

# ROOT = Path(__file__).resolve().parents[2]
# sys.path.insert(0, str(ROOT / "src"))


# def _write_yaml(path: Path, data: dict) -> None:
#     path.parent.mkdir(parents=True, exist_ok=True)
#     path.write_text(yaml.safe_dump(data, sort_keys=False))


# def _build_project(tmp_path: Path, *, frequency: str) -> Path:
#     project_dir = tmp_path / "config"
#     sources_dir = project_dir / "sources"
#     streams_dir = project_dir / "contracts"
#     dataset_path = project_dir / "dataset.yaml"

#     project_yaml = tmp_path / "project.yaml"
#     _write_yaml(
#         project_yaml,
#         {
#             "version": 1,
#             "paths": {
#                 "sources": str(sources_dir),
#                 "streams": str(streams_dir),
#                 "dataset": str(dataset_path),
#             },
#         },
#     )

#     _write_yaml(
#         sources_dir / "time_ticks.yaml",
#         {
#             "parser": {"entrypoint": "synthetic.time", "args": {}},
#             "loader": {
#                 "entrypoint": "synthetic.time",
#                 "args": {
#                     "start": "2024-01-01T00:00:00Z",
#                     "end": "2024-01-01T03:00:00Z",
#                     "frequency": frequency,
#                 },
#             },
#         },
#     )

#     _write_yaml(
#         streams_dir / "time" / "encode.yaml",
#         {
#             "source": "time_ticks",
#             "mapper": {
#                 "entrypoint": "encode_time",
#                 "args": {"mode": "hour_sin"},
#             },
#         },
#     )

#     _write_yaml(
#         dataset_path,
#         {
#             "group_by": {
#                 "keys": [
#                     {
#                         "type": "time",
#                         "field": "time",
#                         "resolution": "1h",
#                     }
#                 ]
#             },
#             "features": [
#                 {
#                     "stream": "time.encode",
#                     "id": "hour_sin",
#                     "partition_by": None,
#                 }
#             ],
#         },
#     )

#     return project_yaml


# @pytest.fixture(autouse=True)
# def _register_entry_points(monkeypatch):
#     from datapipeline.mappers.synthetic.time import encode
#     from datapipeline.sources.synthetic.time.loader import make_time_loader
#     from datapipeline.sources.synthetic.time.parser import TimeRowParser

#     def _fake_load_ep(group: str, name: str):
#         if group == PARSERS_EP and name == "synthetic.time":
#             return TimeRowParser
#         if group == LOADERS_EP and name == "synthetic.time":
#             return make_time_loader
#         if group == MAPPERS_EP and name in {"encode_time", "synthetic.time.encode"}:
#             return encode
#         raise ValueError(f"Unhandled entry point lookup: {group}:{name}")

#     monkeypatch.setattr("datapipeline.utils.load.load_ep", _fake_load_ep)
#     monkeypatch.setattr(
#         "datapipeline.services.factories.load_ep", _fake_load_ep)
#     monkeypatch.setattr(
#         "datapipeline.pipeline.utils.transform_utils.load_ep", _fake_load_ep)


# def test_iter_vector_rows_returns_mapping(tmp_path):
#     project_yaml = _build_project(tmp_path, frequency="1h")

#     rows = list(islice(iter_vector_rows(project_yaml, limit=None), 2))

#     assert len(rows) == 2
#     first = rows[0]
#     assert "group" in first and isinstance(first["group"], dict)
#     assert list(first["group"].keys()) == ["time"]
#     assert "hour_sin" in first
#     assert isinstance(first["hour_sin"], float)


# def test_iter_vector_rows_flatten_sequences(tmp_path):
#     project_yaml = _build_project(tmp_path, frequency="30m")

#     rows = list(iter_vector_rows(
#         project_yaml, limit=1, flatten_sequences=True))

#     assert len(rows) == 1
#     first = rows[0]
#     assert "hour_sin[0]" in first and "hour_sin[1]" in first


# def test_stream_vectors_pairs(tmp_path):
#     project_yaml = _build_project(tmp_path, frequency="1h")

#     pairs = list(islice(stream_vectors(project_yaml), 1))

#     assert len(pairs) == 1
#     group_key, vector = pairs[0]
#     assert isinstance(group_key, tuple)
#     # Vector import is avoided to keep the test focused on iterator shape
#     assert hasattr(vector, "values")
