import contextlib
import logging
import time
from typing import Iterator

logger = logging.getLogger(__name__)

# =============================================================================
# CONTEXT MANAGERS
# =============================================================================


@contextlib.contextmanager
def timer(
    operation_name: str,  #
) -> Iterator[None]:
    """
    Context manager to time operations.

    Parameters:
        operation_name (str): A descriptive name for the timed operation.

    Yields:
        None: This context manager yields control back to the caller.
    """
    start_time = time.perf_counter()
    logger.info(f"Starting: {operation_name}")

    try:
        yield

    finally:
        end_time = time.perf_counter()
        logger.info(
            f"Completed: {operation_name} in {end_time - start_time:.2f} seconds"
        )
