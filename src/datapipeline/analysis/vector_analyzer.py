from collections import defaultdict, Counter
from typing import Any, Hashable, Iterable

import numpy as np


class VectorStatsCollector:
    def __init__(self, expected_feature_ids: Iterable[str]):
        self.expected_feature_ids = set(expected_feature_ids)
        self.missing_features = Counter()
        self.empty_vectors = 0
        self.total_vectors = 0
        self.per_group_missing = defaultdict(set)

    def update(self, group_key: Hashable, feature_vector: dict[str, Any]):
        self.total_vectors += 1

        present_features = set(feature_vector.keys())

        if not present_features:
            self.empty_vectors += 1

        missing = self.expected_feature_ids - present_features
        for feature_id in missing:
            self.missing_features[feature_id] += 1
            self.per_group_missing[group_key].add(feature_id)

        # 🔍 Check for features present but with missing/invalid values
        for fid in present_features & self.expected_feature_ids:
            val = feature_vector[fid]
            if val is None or (isinstance(val, float) and np.isnan(val)):
                self.missing_features[fid] += 1
                self.per_group_missing[group_key].add(fid)

    def print_report(self):
        print("\n=== Vector Quality Report ===")
        print(f"Total vectors processed: {self.total_vectors}")
        print(f"Empty vectors: {self.empty_vectors}")
        print(
            f"Features expected: {sorted(self.expected_feature_ids)[:10] }... (total {len(self.expected_feature_ids)})")

        print("\n→ Missing feature counts:")
        for fid, count in sorted(self.missing_features.items(), key=lambda x: -x[1]):
            print(f"  - {fid}: missing in {count} vectors")

        if self.per_group_missing:
            print("\n→ Groups with missing features (sample):")
            for group_key, missing in list(self.per_group_missing.items())[:5]:
                print(f"  - Group {group_key}: {sorted(missing)}")
