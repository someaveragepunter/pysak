import logging
from math import ceil
from functools import lru_cache
from typing import Iterable
from collections.abc import Mapping, MutableSet, MutableSequence
from frozendict import frozendict
from concurrent.futures import ThreadPoolExecutor
from swarkn.helpers import swallow_exception_wrap

logger = logging.getLogger(__name__)


FREEZE_MAPPING = {
    Mapping: frozendict,
    MutableSequence: tuple,
    MutableSet: frozenset
}

THAW_MAPPING = {
    frozendict: dict,
    tuple: list,
    frozenset: set,
}

def freeze(x):
    for fr, to in FREEZE_MAPPING.items():
        if isinstance(x, fr):
            return to(x)
    return x

def unfreeze(x):
    for fr, to in THAW_MAPPING.items():
        if isinstance(x, fr):
            return to(x)
    return x


@lru_cache()
def _get_pool():
    return ThreadPoolExecutor(10)

def prl_map(func: callable, iterable: Iterable, chunksize=None, pool=None, ignore_failures=False):
    pool = pool or _get_pool()
    chunksize = chunksize or ceil(len(iterable) / len(pool._pool))
    chunks = list(chunk(iterable, chunksize))
    logger.info(f'splitting {len(iterable)} elements into {len(chunks)} buckets of size {chunksize}')
    wrapped_func = swallow_exception_wrap(func) if ignore_failures else func
    res = list(pool.map(wrapped_func, chunks))
    return res