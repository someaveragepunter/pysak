from swarkn.collections import freeze, unfreeze
def test_freeze():
    f1 = freeze({'a': 1})
    f2 = freeze({1})
    f3 = freeze([1,2,3])
    f4 = freeze((1, 2, 3))
    f5 = freeze('this is a string')

    u1 = unfreeze(f1)
    u2 = unfreeze(f2)
    u3 = unfreeze(f3)
    u4 = unfreeze(f4)
    u5 = unfreeze(f5)
