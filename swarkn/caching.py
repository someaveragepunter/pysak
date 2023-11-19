import yaml
from functools import partial
from typing import Callable
from swarkn.collections import freeze, unfreeze
from cachetools.keys import _HashedTuple, _kwmark


def _freeze_args(freezefn: Callable, *args, **kwargs) -> _HashedTuple:
    args_ = tuple(freezefn(a) for a in args)
    if kwargs:
        kwargs_ = sum(sorted([
            (k, freezefn(v)) for k, v in kwargs.items()
        ]), _kwmark)
        return _HashedTuple(args_ + kwargs_)
    return _HashedTuple(args_)

freeze_args = partial(_freeze_args, freeze)
freeze_args_yaml = partial(_freeze_args, yaml.dump)

try:
    from freezedata import freeze_data
    freeze_args_obj = partial(_freeze_args, freeze_data)
except:
     pass


