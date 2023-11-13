from swarkn.helpers import swallow_exception
def test_swallow_exception():
    with swallow_exception():
        raise RuntimeError('this is a test')
