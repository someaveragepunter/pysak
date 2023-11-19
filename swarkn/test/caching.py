from cachetools import cached
from swarkn.caching import freeze_args, freeze_args_yaml

@cached({}, key=freeze_args) #freeze_args freeze_args_yaml
def cached_func(a, b=[1, 2, 4]):
    print(f'this only appears if function executed {a} {b}')
    return a, b


def test_freeze_args():
    kwarg = {'a': 2, 'b': {'c': [1, 2, 3], 'd': 1}, 'e': 'ee'}
    cached_func([1, 2, 3])
    cached_func(1)
    cached_func(1, [2, 3, 4])
    cached_func(1, b=kwarg)

    cached_func([1, 2, 3])
    cached_func(1)
    cached_func(1, [2, 3, 4])
    res = cached_func(1, b=kwarg)

if __name__ == '__main__':
    test_freeze_args()