from datapipeline.utils.load import load_ep
from datapipeline.plugins import PARSERS_EP, LOADERS_EP, MAPPERS_EP
from datapipeline.sources.models.source import Source
from datapipeline.config.catalog import SourceConfig, EPArgs, ContractConfig
from datapipeline.mappers.noop import identity
from datapipeline.utils.placeholders import normalize_args
from datapipeline.sources.models.base import SourceInterface
from datapipeline.pipeline.context import PipelineContext
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.pipeline.pipelines import build_feature_pipeline
from datapipeline.pipeline.utils.transform_utils import _supports_parameter
from inspect import isclass
from typing import Iterator, Any, Optional


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


class _ComposedSource(SourceInterface):
    def __init__(self, *, runtime, stream_id: str, spec: ContractConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def stream(self):
        context = PipelineContext(self._runtime)
        raw_inputs = self._spec.inputs
        input_specs = list(raw_inputs or [])
        if not input_specs:
            return iter(())

        # Resolve inputs: "[alias=]stream_id" (streams only)
        resolved = self._resolve_inputs(context, input_specs)
        aligned = {k: v for k, v in resolved.items() if v["aligned"]}
        aux = {k: v for k, v in resolved.items() if not v["aligned"]}

        # Build aligned/aux iterators (unwrap FeatureRecord -> record for aligned)
        aligned_iters: dict[str, Iterator[Any]] = {
            k: (fr.record for fr in v["iter"])  # stage>=3 yields FeatureRecord
            for k, v in aligned.items()
        }
        aux_iters: dict[str, Iterator[Any]] = {
            k: v["iter"] for k, v in aux.items()}

        # Load mapper (composer) from contract
        mapper = self._spec.mapper
        if not mapper or not mapper.entrypoint:
            raise ValueError(
                f"Composed stream '{self._stream_id}' requires mapper.entrypoint composer"
            )
        ep = load_ep(MAPPERS_EP, mapper.entrypoint)
        kwargs = normalize_args(mapper.args)

        # Choose driver among aligned inputs
        aligned_keys = list(aligned_iters.keys())
        if not aligned_keys:
            driver_key = None
        else:
            driver_key = kwargs.pop("driver", None) or aligned_keys[0]

        # Mapper adapters: Simple vs Advanced
        if not isclass(ep) and not _supports_parameter(ep, "inputs"):
            # Simple: expect a single iterator when exactly one aligned input and no aux
            if len(aligned_iters) == 1 and not aux_iters:
                single_iter = next(iter(aligned_iters.values()))
                for rec in ep(single_iter):
                    yield getattr(rec, "record", rec)
                return
            raise TypeError(
                "Mapper must accept inputs=... for multi-input or aux-enabled contracts"
            )

        # Advanced: pass inputs / aux / driver / context when supported
        call_kwargs = dict(kwargs)
        if _supports_parameter(ep, "context") and "context" not in call_kwargs:
            call_kwargs["context"] = context
        if _supports_parameter(ep, "aux"):
            call_kwargs["aux"] = aux_iters
        if driver_key and _supports_parameter(ep, "driver"):
            call_kwargs["driver"] = driver_key

        if isclass(ep):
            inst = ep(**call_kwargs) if call_kwargs else ep()
            binder = getattr(inst, "bind_context", None)
            if callable(binder):
                binder(context)
            for rec in inst(inputs=aligned_iters):
                yield getattr(rec, "record", rec)
            return

        for rec in ep(inputs=aligned_iters, **call_kwargs):
            yield getattr(rec, "record", rec)

    def _resolve_inputs(self, context: PipelineContext, specs: list[str]):
        """Parse and resolve composed inputs into iterators.

        Grammar: "[alias=]stream_id" only. All inputs are built to stage 4
        and are alignable (FeatureRecord -> domain record unwrapped).
        """
        runtime = context.runtime
        known_streams = set(runtime.registries.stream_sources.keys())

        out: dict[str, dict] = {}
        for spec in specs:
            alias, ref = self._parse_input(spec)
            if ref not in known_streams:
                raise ValueError(
                    f"Unknown input stream '{ref}'. Known streams: {sorted(known_streams)}"
                )
            cfg = FeatureRecordConfig(record_stream=ref, id=alias)
            it = build_feature_pipeline(context, cfg, stage=4)
            out[alias] = {"iter": it, "aligned": True}

        return out

    @staticmethod
    def _parse_input(text: str) -> tuple[str, str]:
        # alias=stream_id
        if "@" in text:
            raise ValueError(
                "composed inputs may not include '@stage'; streams align by default")
        alias: Optional[str] = None
        if "=" in text:
            alias, text = text.split("=", 1)
        ref = text
        alias = alias or ref
        return alias, ref


def build_composed_source(stream_id: str, spec: ContractConfig, runtime) -> SourceInterface:
    return _ComposedSource(runtime=runtime, stream_id=stream_id, spec=spec)
