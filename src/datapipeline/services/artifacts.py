from datapipeline.services.constants import PARTIONED_IDS
from datapipeline.registries.registry import Registry


from datapipeline.registries.registries import artifacts


_builders: Registry[str, callable] = Registry()


def _expected_ids(path: str) -> list[str]:
    ids: list[str] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            ids = [line.strip() for line in fh if line.strip()]
    except FileNotFoundError:
        raise RuntimeError(
            f"Missing expected feature-id list at {path}. "
            "Run: `jerry inspect expected --project <project.yaml>` or add `expected:` to transforms in postprocess.yaml. "
            "See README: Postprocess Expected IDs."
        )
    return ids


_builders.register(PARTIONED_IDS, _expected_ids)


def load_artifact(key: str):
    path = artifacts.get(key)
    return _builders.get(key)(path)
