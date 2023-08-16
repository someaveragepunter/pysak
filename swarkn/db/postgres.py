import logging
import json
import pandas as pd
from contextlib import contextmanager
from functools import partial
from typing import Iterable, Union, List, Dict
from psycopg2.extras import execute_values, Json
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.sql import SQL, Identifier, Composed, Literal, Composable

from pythonlib.utils.helpers import get_env, json_serial, wraplist
from pythonlib.utils.pandas_helpers import filter_dict

SQL_ITERABLES = (list, tuple, set, frozenset)


class ThreadedConnectionPoolAug(ThreadedConnectionPool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def cursor(self):
        try:
            _conn = self.getconn()
            with _conn as real_conn:
                cursor = real_conn.cursor()
                yield cursor
                self.logger.info(f'{cursor.rowcount} rows affected')
                real_conn.commit()
        except:
            raise
        finally:
            _conn.rollback()
            self.putconn(_conn)

def cols_from_iterable(cols: Iterable, template='{}'):
    return SQL(', ').join([SQL(template).format(Identifier(col)) for col in cols])

def predicates_from_dict(predicates: dict, operator=SQL(' AND ')) -> SQL:
    """

    tries to cleverly infer predicates from a dict
    handles LIKE, IN and =
    """
    res = Composed([SQL(' WHERE '),
                    operator.join([
                        SQL(' ').join([
                            Identifier(key),
                            SQL(sql_operator(value)),
                            Literal(tuple(value) if isinstance(value, SQL_ITERABLES) else value),
                        ]) for key, value in predicates.items()
                    ])
                    ]) if predicates else SQL('')
    return res


def sql_operator(value) -> str:
    """

    Try to cleverly infer the operator based on literal
    """
    res = 'LIKE' if isinstance(value, str) and '%' in value \
        else 'IS' if value is None \
        else 'IN' if isinstance(value, SQL_ITERABLES) \
        else '='
    return res


def _cast_inputs(list_of_tuples: list):
    res = [[
        Json(elem, dumps=partial(json.dumps, default=json_serial)) if isinstance(elem, dict) else elem
        for elem in tup
    ] for tup in list_of_tuples]
    return res


class DBHelper:
    MAX_POOLSIZE = 8
    MIN_POOLSIZE = 1

    def __init__(self, **login_params):
        self.pool = ThreadedConnectionPoolAug(self.MIN_POOLSIZE, self.MAX_POOLSIZE, **login_params)
        self.logger = logging.getLogger(__name__)

    def execute(self, sql: Composable):
        with self.pool.cursor() as cursor:
            self.logger.info(f'{sql.as_string(cursor)}')
            cursor.execute(sql)
            return cursor.rowcount


    def insert_json(self, table: str, values, schema='public', **kwargs):
        """

        :param values: dict or list of dicts
        :param kwargs: will be added to table
        """
        list_of_values = wraplist(values)
        jsonified_values = [(
            dict(value, **kwargs),  # this is single element tuple
        ) for value in list_of_values]
        latestid = self.insert(table, jsonified_values, schema, returning='id')
        return latestid

    def insert(self, table: str, values, schema='public', returning: str = None,
               cols: Union[list, tuple] = None, #uses table col order by default
               upsertcols: Iterable = None,
        ):
        """

        :param values: list of tuples. or single tuple.
        :param upsertcols: set of keys in "values"
        """
        list_of_values = _cast_inputs(wraplist(values))
        colsql = SQL("({})").format(cols_from_iterable(cols)) if cols else ''
        if upsertcols:
            nonpks = set(cols) - set(upsertcols)
            conflict = SQL("ON CONFLICT({}) DO UPDATE SET {};").format(
                cols_from_iterable(upsertcols),
                cols_from_iterable(nonpks, '{0} = EXCLUDED.{0}'),
            )
        else:
            conflict = None

        insert_str = Composed(filter(None, [
            SQL("insert into {}.{} ").format(Identifier(schema), Identifier(table)),
            colsql,
            SQL(" values %s "),
            conflict,
            SQL("RETURNING {}").format(Identifier(returning)) if returning else None
        ]))

        with self.pool.cursor() as cursor:
            self.logger.info(f'{insert_str.as_string(cursor)} ({len(list_of_values)} rows)')
            execute_values(cursor, insert_str, list_of_values)
            if returning:
                ids = cursor.fetchall()
                return ids[0][0] #latest id

    def update(self, table: str,
               col_values: dict,
               schema='public',
               predicates={}
               ):
        # col_values = {'1':  2, '3', 4}
        # table = 'test'
        update_str = Composed([
            SQL("UPDATE {}.{} SET ").format(Identifier(schema), Identifier(table)),
            SQL(", ").join(SQL('{}={}').format(Identifier(key), Literal(val)) for key, val in col_values.items()),
            predicates_from_dict(predicates),
        ])
        self.logger.info(f'updating {len(col_values)} cols')
        rowcount = self.execute(update_str)
        return rowcount

    def delete(self, table: str,
               schema='public',
               predicates={}
               ):
        sqlstr = Composed([
            SQL("delete from {}.{} ").format(Identifier(schema), Identifier(table)),
            predicates_from_dict(predicates),
        ])
        rowcount = self.execute(sqlstr)
        return rowcount


    def get_table(self, table: str,
                  schema='public',
                  limit=99999999,
                  predicates={},
                  cols=None,
                  session_vars={}) -> pd.DataFrame:
        colstr = SQL(',').join(map(Identifier, cols)) if cols else SQL('*')
        query = Composed([
            SQL("set {} = {}; ").format(Identifier(var), Literal(val))
        for var, val in session_vars.items()] +
        [
            SQL("select {} from {}.{} ").format(colstr, Identifier(schema), Identifier(table)),
            predicates_from_dict(predicates),
            SQL(" limit {}").format(Literal(limit))
        ])
        with self.pool.cursor() as cursor:
            self.logger.info(query.as_string(cursor))
            df = pd.read_sql(query, cursor.connection)
        return df

    def get_json_table(self, *args, predicates=(), cols=None, **kwargs) -> pd.DataFrame:
        """

        expands single columnn jsonb table into individual columns
         :param args: see self.get_table()
        """
        df = self.get_table(*args, **kwargs)
        dfj = pd.concat([
            df[['id']],
            pd.io.json.json_normalize(df['jblob'])
        ], axis=1)
        df_filtered = filter_dict(dfj, predicates)
        return df_filtered[cols] if cols else df_filtered

    def get_table_as_dict(self, *args, json_table=False, **kwargs) -> List[Dict]:
        """

        :param args: see self.get_table()
        """
        func = self.get_json_table if json_table else self.get_table
        df = func(*args, **kwargs)
        res = json.loads(df.to_json(orient='records')) #cleans up Nans and other non json constructs
        return res


if __name__ == '__main__':
    from datetime import date

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    dbhelper = DBHelper()
    #    latestid = dbhelper.insert_json('json', [{'a': 1}], 'test')
    #   latestid = dbhelper.insert('json', [(Json({'dsfdsf': 1}), 206)], 'test')
    dbhelper.update('json', [('id', 999)], 'test', {'id': 1000})
    df = dbhelper.get_table('json', 'test')
    print(df)

    preds = {
        "hello": "world",
        "hello1": "world%",
        "hello2": 1,
        "hello3": [11, 3, 4],
        "hello4": ('dsfdsf',),
        "hello5": date(2018, 1, 1),
    }
    predstr = predicates_from_dict(preds)

    self = dbhelper
    with dbhelper.pool.cursor() as cursor:
        cursor.execute(
            '''
CREATE OR REPLACE VIEW public.v_users AS
    SELECT t.*
        , kyc_gbg and coalesce(kyc_pep, true) and coalesce(kyc_override, true) as kyc_pass
    from raw.users t
            '''
        )
        res = cursor.fetchall()
        res

        self.logger.info(query.as_string(conn))

    res = dbhelper.get_table('users', 'raw', predicates={'id': 4})
    res['jblob'][0]