import logging
from typing import Callable, Any

from tqdm.contrib.logging import logging_redirect_tqdm

from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.runtime import Runtime


logger = logging.getLogger(__name__)


def _run_work(backend, runtime: Runtime, level: int, work: Callable[[], Any]):
    with backend.wrap_sources(runtime, level):
        if backend.requires_logging_redirect():
            with logging_redirect_tqdm():
                return work()
        return work()


def run_with_backend(*, visuals: str, runtime: Runtime, level: int, work: Callable[[], Any]):
    """Execute a unit of work inside a visuals backend context.

    - Picks backend from visuals string (auto|basic|rich|off)
    - Wraps sources so streaming feedback is consistent across commands
    - Redirects tqdm logging for non-rich backends
    """
    backend = get_visuals_backend(visuals)
    return _run_work(backend, runtime, level, work)


def run_job(*, kind: str, label: str, visuals: str, level: int, runtime: Runtime, work: Callable[[], Any], idx: int | None = None, total: int | None = None):
    """Run a labeled job (serve run or build artifact) with visuals lifecycle.

    - Emits a headline via backend (for kind=='run'), with fallback logging
    - Wraps sources so progress/spinners render consistently
    - Emits a completion footer via backend with fallback logging
    """
    backend = get_visuals_backend(visuals)

    job_idx = idx or 1
    job_total = total or 1
    presented = False
    try:
        presented = backend.on_job_start(kind, label, job_idx, job_total)
    except Exception:
        presented = False
    if not presented:
        title = kind.capitalize() if kind else "Job"
        if idx is not None and total is not None:
            logger.info("%s: '%s' (%d/%d)", title, label, job_idx, job_total)
        else:
            logger.info("%s: '%s'", title, label)

    result = _run_work(backend, runtime, level, work)

    try:
        handled = backend.on_streams_complete()
    except Exception:
        handled = False
    if not handled:
        logger.info("All streams complete")

    return result
