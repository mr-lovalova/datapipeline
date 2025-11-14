from __future__ import annotations

import logging
import time
from itertools import islice
from typing import Iterator, Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process
from datapipeline.pipeline.split import apply_split_stage
from datapipeline.runtime import Runtime
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.protocols import Writer

logger = logging.getLogger(__name__)


def limit_items(items: Iterator[object], limit: Optional[int]) -> Iterator[object]:
    if limit is None:
        yield from items
    else:
        yield from islice(items, limit)


def throttle_vectors(
    vectors: Iterator[Sample],
    throttle_ms: Optional[float],
) -> Iterator[Sample]:
    if not throttle_ms or throttle_ms <= 0:
        yield from vectors
        return
    delay = throttle_ms / 1000.0
    for item in vectors:
        yield item
        time.sleep(delay)


def serve_stream(
    items: Iterator[object],
    limit: Optional[int],
    writer: Writer,
) -> int:
    count = 0
    try:
        for item in limit_items(items, limit):
            writer.write(item)
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
    return count


def report_serve(target: OutputTarget, count: int) -> None:
    if target.destination:
        logger.info("Saved %d items to %s", count, target.destination)
        return
    if target.transport == "stdout" and target.format in {"json-lines", "json", "jsonl"}:
        logger.info("(streamed %d items)", count)
        return
    logger.info("(printed %d items to stdout)", count)


def _is_stdout_tty() -> bool:
    try:
        import sys
        return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    except Exception:
        return False


def serve_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int],
    target: OutputTarget,
    include_targets: bool,
    throttle_ms: Optional[float],
    stage: Optional[int],
    visuals: Optional[str] = None,
) -> None:
    context = PipelineContext(runtime)

    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or []) if include_targets else []
    preview_cfgs = feature_cfgs + target_cfgs

    if not preview_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return

    if stage is not None and stage <= 5:
        for cfg in preview_cfgs:
            stream = build_vector_pipeline(
                context,
                [cfg],
                dataset.group_by,
                stage=stage,
            )
            feature_target = target.for_feature(cfg.id)
            writer = writer_factory(feature_target)
            # Pretty-print to stdout via Rich only for human-readable 'print' format
            if (
                feature_target.transport == "stdout"
                and feature_target.format.lower() == "print"
                and (visuals or "auto").lower() == "rich"
                and _is_stdout_tty()
            ):
                try:
                    from rich.console import Console
                    from datapipeline.io.protocols import Writer as _Writer

                    class _RichStdoutPrintWriter(_Writer):
                        def __init__(self):
                            import sys as _sys
                            self.console = Console(file=_sys.stdout, markup=False, highlight=False, soft_wrap=True)

                        def write(self, item) -> None:
                            from dataclasses import is_dataclass, asdict
                            from rich.pretty import Pretty

                            try:
                                if is_dataclass(item):
                                    # Use JSON view for a clean, readable structure
                                    self.console.print_json(data=asdict(item), default=str)
                                    return
                                to_json = None
                                if hasattr(item, "model_dump"):
                                    to_json = item.model_dump()
                                elif hasattr(item, "dict") and callable(getattr(item, "dict")):
                                    to_json = item.dict()
                                if to_json is not None:
                                    self.console.print_json(data=to_json, default=str)
                                    return
                            except Exception:
                                pass
                            # Fallback: Pretty-print the object
                            self.console.print(Pretty(item))

                        def close(self) -> None:
                            pass

                    writer = _RichStdoutPrintWriter()
                except Exception:
                    pass
            count = serve_stream(stream, limit, writer=writer)
            report_serve(feature_target, count)
        return

    vector_stage = 6 if stage in (6, 7) else None
    vectors = build_vector_pipeline(
        context,
        feature_cfgs,
        dataset.group_by,
        stage=vector_stage,
        target_configs=target_cfgs,
    )

    if stage in (None, 7):
        vectors = post_process(context, vectors)
    if stage is None:
        vectors = apply_split_stage(runtime, vectors)
        vectors = throttle_vectors(vectors, throttle_ms)

    writer = writer_factory(target)
    if (
        target.transport == "stdout"
        and target.format.lower() == "print"
        and (visuals or "auto").lower() == "rich"
        and _is_stdout_tty()
    ):
        try:
            from rich.console import Console
            from datapipeline.io.protocols import Writer as _Writer

            class _RichStdoutPrintWriter(_Writer):
                def __init__(self):
                    import sys as _sys
                    self.console = Console(file=_sys.stdout, markup=False, highlight=False, soft_wrap=True)

                def write(self, item) -> None:
                    from dataclasses import is_dataclass, asdict
                    from rich.pretty import Pretty

                    try:
                        if is_dataclass(item):
                            self.console.print_json(data=asdict(item), default=str)
                            return
                        to_json = None
                        if hasattr(item, "model_dump"):
                            to_json = item.model_dump()
                        elif hasattr(item, "dict") and callable(getattr(item, "dict")):
                            to_json = item.dict()
                        if to_json is not None:
                            self.console.print_json(data=to_json, default=str)
                            return
                    except Exception:
                        pass
                    self.console.print(Pretty(item))

                def close(self) -> None:
                    pass

            writer = _RichStdoutPrintWriter()
        except Exception:
            pass

    result_count = serve_stream(vectors, limit, writer=writer)
    report_serve(target, result_count)
