from typing import Any, Iterator

from datapipeline.config.catalog import ContractConfig, EPArgs, SourceConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.mappers.noop import identity
from datapipeline.parsers.identity import IdentityParser
from datapipeline.pipelines import build_record_pipeline
from datapipeline.pipelines.record.nodes import RECORD_NODE_COUNT
from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
from datapipeline.sources.models.loader import BaseDataLoader
from datapipeline.sources.models.source import Source
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_source_from_spec(spec: SourceConfig) -> Source:
    P = load_ep(PARSERS_EP, spec.parser.entrypoint)
    L = load_ep(LOADERS_EP, spec.loader.entrypoint)
    loader_args = normalize_args(spec.loader.args)
    parser_args = normalize_args(spec.parser.args)
    return Source(loader=L(**loader_args), parser=P(**parser_args))


def build_mapper_from_spec(spec: EPArgs | None):
    """Return a callable(raw_iter) -> iter with args bound if present."""
    if not spec or not spec.entrypoint:
        return identity
    fn = load_ep(MAPPERS_EP, spec.entrypoint)
    args = normalize_args(spec.args)
    if args:
        return lambda raw: fn(raw, **args)
    return fn


class _ComposedLoader(BaseDataLoader):
    def __init__(self, runtime, stream_id: str, spec: ContractConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def load(self):
        context = PipelineContext(self._runtime)
        raw_inputs = self._spec.inputs
        input_specs = list(raw_inputs or [])
        if not input_specs:
            return

        input_iters: dict[str, Iterator[Any]] = {
            alias: (getattr(item, "record", item) for item in iterator)
            for alias, iterator in self._resolve_inputs(context, input_specs).items()
        }

        mapper = self._spec.mapper
        if not mapper or not mapper.entrypoint:
            raise ValueError(
                f"Composed stream '{self._stream_id}' requires mapper.entrypoint composer"
            )
        ep = load_ep(MAPPERS_EP, mapper.entrypoint)
        kwargs = normalize_args(mapper.args)
        if self._spec.partition_by is not None:
            kwargs.setdefault("partition_by", self._spec.partition_by)

        input_keys = list(input_iters.keys())
        if not input_keys:
            return
        driver_key = kwargs.pop("driver", None) or input_keys[0]
        if driver_key not in input_iters:
            raise ValueError(
                f"Unknown composed driver '{driver_key}' for stream "
                f"'{self._stream_id}'. Available: {input_keys}"
            )

        try:
            composed_records = ep(
                inputs=input_iters,
                context=context,
                driver=driver_key,
                **kwargs,
            )
        except TypeError as exc:
            raise TypeError(
                "Composed mapper must use signature "
                "`mapper(inputs, *, context, driver, **params)`"
            ) from exc

        for rec in composed_records:
            yield getattr(rec, "record", rec)

    def count(self):
        # Compose/join logic may change cardinality; unknown by design.
        return None

    def _resolve_inputs(self, context: PipelineContext, specs: list[str]):
        """Parse and resolve composed inputs into iterators.

        Grammar: "[alias=]stream_id" only. All inputs are built to stage 4
        with stream transforms applied.
        """
        runtime = context.runtime
        known_streams = set(runtime.registries.stream_sources.keys())

        out: dict[str, Iterator[Any]] = {}
        for spec in specs:
            alias, ref = self._parse_input(spec)
            if ref not in known_streams:
                raise ValueError(
                    f"Unknown input stream '{ref}'. Known streams: {sorted(known_streams)}"
                )
            out[alias] = build_record_pipeline(
                context,
                ref,
                stage=RECORD_NODE_COUNT - 1,
            )

        return out

    @staticmethod
    def _parse_input(text: str) -> tuple[str, str]:
        if "@" in text:
            raise ValueError(
                "composed inputs may not include '@stage'; streams align by default")
        return ContractConfig.parse_input_spec(text)


def build_composed_source(stream_id: str, spec: ContractConfig, runtime) -> Source:
    return Source(
        loader=_ComposedLoader(runtime=runtime, stream_id=stream_id, spec=spec),
        parser=IdentityParser(),
    )
