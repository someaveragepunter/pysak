from swarkn.collections import freeze, unfreeze
from cachetools.keys import _HashedTuple, _kwmark
def freeze_args(*args, **kwargs) -> _HashedTuple:
    args_ = tuple(freeze(a) for a in args)
    if kwargs:
        kwargs_ = sum(sorted([
            (k, freeze(v)) for k, v in kwargs.items()
        ]), _kwmark)
        return _HashedTuple(args_ + kwargs_)
    return _HashedTuple(args_)
