from pathlib import Path

from datapipeline.integrations import dataframe_from_vectors


def main() -> None:
    project = Path(__file__).resolve().parent / \
        "config/project.yaml"
    df = dataframe_from_vectors(
        project,
        limit=None,
        include_group=True,
        group_format="mapping",
        flatten_sequences=True,
    )
    print("DataFrame shape:", df.shape)
    print(df.head())


if __name__ == "__main__":
    main()
