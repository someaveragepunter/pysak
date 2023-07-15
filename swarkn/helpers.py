import sys
import logging
from collections.abc import Mapping
from functools import lru_cache
from time import perf_counter
from contextlib import contextmanager
logger = logging.getLogger(__name__)

@contextmanager
def timer(msg=None) -> float:
    start = perf_counter()
    yield perf_counter() - start
    cost = perf_counter() - start
    msg = msg or "Time taken: {cost}s"
    logger.info(msg.format(cost=cost))

@lru_cache()
def is_local():
    return 'win' in sys.platform

def init_logger(level=logging.INFO, format='%(asctime)s|%(name)s|%(levelname)s|%(message)s'):
    logging.basicConfig(level=level, format=format)

def swallow_exception_wrap(func, return_exception=False, **logger_kwags, ):
    def innerfunc(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f'failed func: {func} args: {args}, kwargs: {kwargs}, exception: {e}', **logger_kwags)
            if return_exception:
                return e

    return innerfunc

@contextmanager
def swallow_exception(**kwags):
    try:
        yield
    except Exception as ex:
        logger.error(ex, **kwags)



