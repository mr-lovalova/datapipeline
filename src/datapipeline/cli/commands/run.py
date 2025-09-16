from tqdm import tqdm
from datapipeline.services.bootstrap import bootstrap
from datapipeline.pipeline.pipelines import build_record_pipeline, build_feature_pipeline, build_vector_pipeline
from pathlib import Path
from datapipeline.config.dataset.dataset import RecordDatasetConfig, FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.cli.openers import open_canonical_stream_visual


def _print_head(iterable, limit: int) -> int:
    count = 0
    try:
        for x in iterable:
            tqdm.write(str(x))
            count += 1
            if count >= limit:
                break
    except KeyboardInterrupt:
        pass
    return count


def _run_records(dataset: RecordDatasetConfig, limit: int):

    for cfg in dataset.features:
        print(f"\n▶ records for {cfg.feature_id}")
        records = build_record_pipeline(cfg, open_canonical_stream_visual)
        n = _print_head(records, limit)
        print(f"(printed {n} records)")


def _run_features(dataset: FeatureDatasetConfig, limit: int):

    group_by = dataset.group_by
    for cfg in dataset.features:
        print(f"\n▶ features for {getattr(cfg, 'feature_id', '?')}")
        features = build_feature_pipeline(
            cfg, group_by, open_canonical_stream_visual)
        n = _print_head(features, limit)
        tqdm.write(f"(printed {n} feature records)")


def _run_vectors(dataset, limit: int):

    print("\n▶ vectors")
    vecs = build_vector_pipeline(
        dataset.features, dataset.group_by, open_canonical_stream_visual)
    n = _print_head(vecs, limit)
    print(f"(printed {n} vectors)")


def handle(project: str, stage: str, limit: int = 20):

    project = Path(project)
    dataset = load_dataset(project, stage)
    bootstrap(project)

    feats = list(dataset.features or [])
    if not feats:
        print("(no features to run)")
        return

    if stage == "records":
        _run_records(dataset, limit)
    elif stage == "features":
        _run_features(dataset, limit)
    else:
        _run_vectors(dataset, limit)
