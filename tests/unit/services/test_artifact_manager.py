from datapipeline.services.artifacts import ArtifactManager


def test_registered_metadata_is_a_stable_read_only_mapping(tmp_path) -> None:
    source = {"rows": 1}
    artifacts = ArtifactManager(tmp_path)

    artifacts.register("ticks", "ticks.jsonl", source)
    source["rows"] = 2
    metadata = artifacts.require("ticks").meta

    assert metadata == {"rows": 1}
    assert not hasattr(metadata, "__setitem__")
