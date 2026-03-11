import logging
from typing import Callable, Any, Sequence, Tuple

from datapipeline.cli.visuals.execution import make_execution_observer
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.runtime import Runtime


logger = logging.getLogger(__name__)


def _run_work(backend, runtime: Runtime, level: int, work: Callable[[], Any]):
    previous_observer = getattr(runtime, "execution_observer", None)
    installed = previous_observer is None
    if installed:
        runtime.execution_observer = make_execution_observer(
            logging.getLogger("datapipeline.dag.observer")
        )
    try:
        with backend.wrap_sources(runtime, level):
            return work()
    finally:
        if installed:
            runtime.execution_observer = previous_observer


def run_with_backend(visuals: str, runtime: Runtime, level: int, work: Callable[[], Any]):
    """Execute a unit of work inside a visuals backend context."""
    backend = get_visuals_backend(visuals)
    return _run_work(backend, runtime, level, work)


def run_job(
    sections: Sequence[str] | None,
    label: str,
    visuals: str,
    level: int,
    runtime: Runtime,
    work: Callable[[], Any],
    idx: int | None = None,
    total: int | None = None,
):
    """Run a labeled job with visuals, rendering optional hierarchical headers."""
    backend = get_visuals_backend(visuals)

    job_idx = idx or 1
    job_total = total or 1
    section_tuple: Tuple[str, ...] = tuple(section for section in (sections or []) if section)
    presented = False
    try:
        presented = backend.on_job_start(section_tuple, label, job_idx, job_total)
    except Exception:
        presented = False
    if not presented:
        prefix = " / ".join(section_tuple) if section_tuple else "Job"
        if idx is not None and total is not None:
            logger.info("%s: '%s' (%d/%d)", prefix, label, job_idx, job_total)
        else:
            logger.info("%s: '%s'", prefix, label)

    result = _run_work(backend, runtime, level, work)

    try:
        handled = backend.on_streams_complete()
    except Exception:
        handled = False
    if not handled:
        logger.info("All streams complete")

    return result
