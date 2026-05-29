import os
from datetime import timedelta

import pytest

from datapipeline.services.temp_cleanup import (
    clean_temp_dirs,
    find_temp_dirs,
    format_bytes,
    parse_age,
)


def test_find_temp_dirs_only_matches_jerry_prefixes(tmp_path) -> None:
    cache_dir = tmp_path / "datapipeline-cache-project-a"
    sort_dir = tmp_path / "datapipeline-sort-run-a"
    other_dir = tmp_path / "other"
    cache_dir.mkdir()
    sort_dir.mkdir()
    other_dir.mkdir()
    (cache_dir / "data.bin").write_bytes(b"abc")
    (sort_dir / "data.bin").write_bytes(b"abcd")

    found = find_temp_dirs(root=tmp_path)

    assert [item.path.name for item in found] == [
        "datapipeline-cache-project-a",
        "datapipeline-sort-run-a",
    ]
    assert sum(item.size_bytes for item in found) == 7


def test_clean_temp_dirs_is_dry_run_by_default(tmp_path) -> None:
    cache_dir = tmp_path / "datapipeline-cache-project-a"
    cache_dir.mkdir()

    result = clean_temp_dirs(yes=False, root=tmp_path)

    assert result.dry_run is True
    assert result.removed == ()
    assert cache_dir.exists()


def test_clean_temp_dirs_removes_matches_with_yes(tmp_path) -> None:
    cache_dir = tmp_path / "datapipeline-cache-project-a"
    cache_dir.mkdir()

    result = clean_temp_dirs(yes=True, root=tmp_path)

    assert result.dry_run is False
    assert result.removed == (cache_dir,)
    assert not cache_dir.exists()


def test_find_temp_dirs_respects_age_filter(tmp_path) -> None:
    old_dir = tmp_path / "datapipeline-cache-old"
    new_dir = tmp_path / "datapipeline-cache-new"
    old_dir.mkdir()
    new_dir.mkdir()
    old_time = 1_700_000_000
    os.utime(old_dir, (old_time, old_time))

    found = find_temp_dirs(root=tmp_path, older_than=timedelta(days=1))

    assert [item.path.name for item in found] == ["datapipeline-cache-old"]


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("30m", 1800),
        ("2h", 7200),
        ("1d", 86400),
        ("3", 10800),
    ],
)
def test_parse_age(value: str, seconds: int) -> None:
    assert parse_age(value).total_seconds() == seconds


def test_parse_age_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="age must"):
        parse_age("soon")


def test_format_bytes() -> None:
    assert format_bytes(0) == "0B"
    assert format_bytes(1024) == "1.0KB"
