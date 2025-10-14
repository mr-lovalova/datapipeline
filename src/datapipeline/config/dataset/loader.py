from pathlib import Path
from typing import Literal, Mapping
import json
from datapipeline.config.dataset.dataset import RecordDatasetConfig, FeatureDatasetConfig
from datapipeline.services.bootstrap import _load_by_key

Stage = Literal["records", "features", "vectors"]


def _resolve_vector_transform_paths(ds_doc: dict, base_dir: Path) -> None:
    """Normalize any relative `manifest` paths in vector transforms.

    Expects clauses shaped like: [{"transform_name": {"manifest": "path" , ...}}, ...]
    Mutates ds_doc in-place.
    """
    clauses = ds_doc.get("vector_transforms")
    if not isinstance(clauses, list):
        return
    for clause in clauses:
        if not isinstance(clause, Mapping):
            continue
        for _name, params in clause.items():
            if isinstance(params, Mapping):
                manifest = params.get("manifest")
                if isinstance(manifest, str):
                    p = Path(manifest)
                    if not p.is_absolute():
                        params_dict = dict(params)
                        params_dict["manifest"] = str((base_dir / p).resolve())
                        clause[_name] = params_dict


def _expand_vector_transforms_manifest(ds_doc: dict) -> None:
    """Expand any vector transform clauses that reference a manifest into an
    explicit 'expected' list. Removes 'manifest' and 'match_partition' keys.

    The manifest JSON is expected to contain 'features' (base ids) and
    'partitions' (full ids). Clauses may include 'match_partition': 'full' to
    use partitions; otherwise features are used.
    """
    clauses = ds_doc.get("vector_transforms")
    if not isinstance(clauses, list):
        return
    for clause in clauses:
        if not isinstance(clause, Mapping) or len(clause) != 1:
            continue
        name, params = next(iter(clause.items()))
        if not isinstance(params, Mapping):
            continue
        # Do not expand for minimal sequence guard; it ignores manifests entirely.
        if name == "require_complete_sequences":
            if "manifest" in params or "match_partition" in params:
                new_params = dict(params)
                new_params.pop("manifest", None)
                new_params.pop("match_partition", None)
                clause[name] = new_params
            continue
        if "manifest" in params and "expected" not in params:
            try:
                manifest_path = Path(params["manifest"]).expanduser().resolve()
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                scope = params.get("match_partition")
                ids = data.get(
                    "partitions") if scope == "full" else data.get("features")
                if isinstance(ids, list):
                    new_params = dict(params)
                    new_params.pop("manifest", None)
                    new_params.pop("match_partition", None)
                    new_params["expected"] = [str(x) for x in ids]
                    clause[name] = new_params
            except Exception:
                # If expansion fails, leave clause as-is; downstream may ignore it.
                continue


def load_dataset(project_yaml, stage: Stage):
    ds_doc = _load_by_key(project_yaml, "dataset")
    # Ensure any relative artifact paths in vector transforms are based on the
    # project YAML directory, not the caller's current working directory.
    try:
        base_dir = Path(project_yaml).resolve().parent
        if isinstance(ds_doc, dict):
            _resolve_vector_transform_paths(ds_doc, base_dir)
            _expand_vector_transforms_manifest(ds_doc)
    except Exception:
        # Be permissive: if anything goes wrong, fall back to raw doc.
        pass

    if stage == "records":
        return RecordDatasetConfig.model_validate(ds_doc)
    elif stage == "features":
        return FeatureDatasetConfig.model_validate(ds_doc)
    elif stage == "vectors":
        return FeatureDatasetConfig.model_validate(ds_doc)
    else:
        raise ValueError(f"Unknown stage: {stage}")


## Contract policies and feature transforms are consumed at runtime by the pipeline.
