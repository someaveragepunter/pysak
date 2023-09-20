import sys
from importlib.util import spec_from_file_location, module_from_spec
from importlib import import_module


def load_module(path: str, module_name="module.name"):
    """

    path = '/home/test'
    path = 'c:/users/user/test.py'
    """
    if path.endswith('.py'):
        spec = spec_from_file_location(module_name, path)
        module = module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    return import_module(path)