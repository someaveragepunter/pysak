import logging
from math import ceil
from functools import lru_cache
from typing import Iterable
from multiprocessing.pool import ThreadPool
from pysak.helpers import swallow_exception_wrapper

logger = logging.getLogger(__name__)

@lru_cache()
def _get_pool():
    return ThreadPool(10)


def chunk(iterable: Iterable, size: int):
    return (iterable[i:i + size] for i in range(0, len(iterable), size))

def prl_map(func: callable, iterable: Iterable, chunksize=None, pool=None, ignore_failures=False):
    pool = pool or _get_pool()
    chunksize = chunksize or ceil(len(iterable) / len(pool._pool))
    chunks = list(chunk(iterable, chunksize))
    logger.info(f'splitting {len(iterable)} elements into {len(chunks)} buckets of size {chunksize}')
    wrapped_func = swallow_exception_wrapper(func) if ignore_failures else func
    res = pool.map(wrapped_func, chunks)
    return res

def freeze(x):
    from frozendict import frozendict
    return frozendict(x) if isinstance(x, dict) else x

def unfreeze(x):
    return dict(x) if isinstance(x, Mapping) else x