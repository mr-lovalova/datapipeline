from types import SimpleNamespace

from datapipeline.config.catalog import ContractConfig, EPArgs
from datapipeline.services.factories import _ComposedLoader


class _Registry:
    def __init__(self, keys: list[str]) -> None:
        self._keys = tuple(keys)

    def keys(self):
        return self._keys


class _ClosableIterator:
    def __init__(self, values):
        self._values = iter(values)
        self.closed = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._values)

    def close(self):
        self.closed += 1


def test_composed_loader_closes_upstream_iterators_when_closed(monkeypatch) -> None:
    upstream = _ClosableIterator([1, 2, 3])

    def _cached_record_stream(_context, _ref):
        return upstream

    def _mapper(inputs, *, context, driver, **params):
        del context
        del params
        yield from inputs[driver]

    monkeypatch.setattr(
        "datapipeline.services.factories.cached_record_stream",
        _cached_record_stream,
    )
    monkeypatch.setattr(
        "datapipeline.services.factories.load_ep",
        lambda _namespace, _entrypoint: _mapper,
    )

    runtime = SimpleNamespace(
        registries=SimpleNamespace(
            stream_sources=_Registry(["equity.aapl"]),
        )
    )
    spec = ContractConfig(
        kind="composed",
        id="equity.pair.aapl",
        inputs=["aapl=equity.aapl"],
        mapper=EPArgs(entrypoint="test.mapper", args={}),
    )
    loader = _ComposedLoader(runtime=runtime, stream_id=spec.id, spec=spec)

    iterator = loader.load()
    assert next(iterator) == 1

    iterator.close()

    assert upstream.closed == 1
