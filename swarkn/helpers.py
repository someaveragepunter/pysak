import frozendict
import sys
import logging
from typing import Callable
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


def freeze(x):
    from frozendict import frozendict
    return frozendict(x) if isinstance(x, dict) else x

def unfreeze(x):
    return dict(x) if isinstance(x, Mapping) else x

def clean_kwargs(fn: Callable, kwargs: dict) -> dict:
    params = kwargs if 'kwargs' in fn.__code__.co_varnames else {
        k: v for k, v in kwargs.items() if k in fn.__code__.co_varnames
    }
    return params

def run_func(fn: Callable, *args, **kwargs):
    """

    removes irrelevant kwargs before calling fn
    """
    params = clean_kwargs(fn, kwargs)
    return fn(*args, **params)

