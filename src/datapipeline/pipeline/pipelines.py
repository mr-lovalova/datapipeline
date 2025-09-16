import heapq
from datapipeline.pipeline.stages import record_stage, feature_stage, vector_stage


def build_record_pipeline(cfg, open_stream):
    raw = open_stream(cfg.stream)
    return record_stage(raw, cfg.filters, cfg.transforms)


def build_feature_pipeline(cfg, group_by, open_stream):
    rec = build_record_pipeline(cfg, open_stream)
    return feature_stage(rec, cfg, group_by)


def build_vector_pipeline(configs, group_by, open_stream):
    streams = [build_feature_pipeline(
        c, group_by, open_stream) for c in configs]
    merged = heapq.merge(*streams, key=lambda fr: fr.group_key)
    return vector_stage(merged)
