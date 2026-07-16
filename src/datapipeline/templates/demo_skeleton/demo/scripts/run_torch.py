from pathlib import Path

import torch
from torch.utils.data import DataLoader

from datapipeline.integrations import torch_dataset


def main() -> None:
    project = Path(__file__).resolve().parents[1] / "project.yaml"
    ds = torch_dataset(
        project,
        output_id="holdout.train",
        limit=256,
        dtype=torch.float32,
        flatten_sequences=True,
    )
    loader = DataLoader(ds, batch_size=32, shuffle=True)
    batch = next(iter(loader))
    print("Feature batch shape:", batch.shape)


if __name__ == "__main__":
    main()
