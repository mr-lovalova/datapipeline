from dataclasses import dataclass, field

from datapipeline.cli.commands.serve_pipeline import serve_stream


@dataclass
class _Writer:
    writes: list[object] = field(default_factory=list)
    closed: bool = False

    def write(self, item: object) -> None:
        self.writes.append(item)

    def close(self) -> None:
        self.closed = True


def test_serve_stream_writes_all_items_and_closes_writer(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1, 2]),
        limit=None,
        writer=writer,
    )

    captured = capsys.readouterr()
    assert count == 2
    assert writer.writes == [1, 2]
    assert writer.closed is True
    assert captured.out == ""


def test_serve_stream_honors_limit_and_closes_writer(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1, 2, 3]),
        limit=1,
        writer=writer,
    )

    captured = capsys.readouterr()
    assert count == 1
    assert writer.writes == [1]
    assert writer.closed is True
    assert captured.out == ""
