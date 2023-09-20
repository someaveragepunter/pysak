from swarkn.imports import load_module
from swarkn.helpers import run_func
def test_load_module():
    path = 'swarkn.db.sql'
    path = r'C:\Users\user\PycharmProjects\pysak\swarkn\db\sql.py'
    module = load_module(path)
    res = module.dict2predicate_sql()

    run_func(module.dict2predicate_sql, predicates={'a': 1}, andor='OR')
