from datetime import datetime, timezone

from datapipeline.services import runs


def test_run_ids_include_subsecond_precision(monkeypatch) -> None:
    instants = iter(
        [
            datetime(2026, 1, 1, 12, 0, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 12, 0, 0, 2, tzinfo=timezone.utc),
        ]
    )

    class _Datetime:
        @classmethod
        def now(cls, tz):
            assert tz is timezone.utc
            return next(instants)

    monkeypatch.setattr(runs, "datetime", _Datetime)

    assert runs.make_run_id() == "2026-01-01T12-00-00-000001Z"
    assert runs.make_run_id() == "2026-01-01T12-00-00-000002Z"
