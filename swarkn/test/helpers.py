from swarkn.helpers import swallow_exception, timer
def test_context_managers():
    with swallow_exception():
        raise RuntimeError('this is a test')

    with timer():
        x = 1

    with timer(None, logger, 'debug'):
        x = 1
