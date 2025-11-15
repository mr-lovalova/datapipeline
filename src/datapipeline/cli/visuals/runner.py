import logging
from typing import Callable, Any, Sequence, Tuple

from tqdm.contrib.logging import logging_redirect_tqdm

from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.runtime import Runtime


logger = logging.getLogger(__name__)


def _run_work(backend, runtime: Runtime, level: int, progress_style: str, work: Callable[[], Any]):
    with backend.wrap_sources(runtime, level, progress_style):
        if backend.requires_logging_redirect():
            with logging_redirect_tqdm():
                return work()
        return work()


def run_with_backend(*, visuals: str, progress_style: str, runtime: Runtime, level: int, work: Callable[[], Any]):
    """Execute a unit of work inside a visuals backend context."""
    backend = get_visuals_backend(visuals)
    return _run_work(backend, runtime, level, progress_style, work)


def run_job(
    *,
    sections: Sequence[str] | None,
    label: str,
    visuals: str,
    progress_style: str,
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

    result = _run_work(backend, runtime, level, progress_style, work)

    try:
        handled = backend.on_streams_complete()
    except Exception:
        handled = False
    if not handled:
        logger.info("All streams complete")

    return result
