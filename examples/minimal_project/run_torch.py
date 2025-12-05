from pathlib import Path

import torch
from torch.utils.data import DataLoader

from datapipeline.integrations import torch_dataset


def main() -> None:
    project = Path(__file__).resolve().parent / \
        "config/project.yaml"
    ds = torch_dataset(
        project,
        limit=256,
        dtype=torch.float32,
        flatten_sequences=True,
    )
    loader = DataLoader(ds, batch_size=32, shuffle=True)
    batch = next(iter(loader))
    print("Feature batch shape:", batch.shape)


if __name__ == "__main__":
    main()
