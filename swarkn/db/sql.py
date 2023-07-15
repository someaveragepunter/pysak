import re

def dict2predicate_sql(predicates: dict, andor='AND') -> str:
    res = []
    for k, v in predicates.items():
        value = v
        if isinstance(v, (tuple, set, list)):
            operator = 'in'
            value = re.sub('\]|\}', ')',
                re.sub('\[|\{', '(', str(v))
            )
        elif isinstance(v, str):
            operator = 'like'
            value = f"'{v}'"
        elif v is None:
            operator = 'is'
            value = 'Null'
        else:
            operator = '='

        if k.startswith('~'):
            operator = '!=' if operator == '=' else \
                       'is not' if operator == 'is' else \
                       f'not {operator}'

        res.append(f"{k.replace('~', '')} {operator} {value}")
    return f' {andor} '.join(res)

