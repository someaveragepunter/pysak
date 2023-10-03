from cachetools import cached
from swarkn.caching import freeze_args

@cached({}, key=freeze_args)
def cached_func(a, b=[1, 2, 4]):
    print(f'this only appears if function executed {a} {b}')
    return a, b


def test_freeze_args():
    cached_func([1, 2, 3])
    cached_func(1)
    cached_func(1, [2, 3, 4])
    cached_func(1, b={'a': 2, 'b': 3})

    cached_func([1, 2, 3])
    cached_func(1)
    cached_func(1, [2, 3, 4])
    res = cached_func(1, b={'b': 3, 'a': 2})

if __name__ == '__main__':
    test_freeze_args()