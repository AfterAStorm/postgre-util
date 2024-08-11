# afterastorm #
# postgre wrapper w/ psycopg2

from typing import Generic, TypeVar
import psycopg2 as pg

__all__ = [ # what the import * fetches
    'Where',
    'Database',
    'ALL',
    'ONE'
]


class Where:
    def __init__(self, string: str=None): # string in-case you already make a statement, adds WHERE for you
        self.conditions = []
        self.string = ' WHERE ' + string if string != None else ''
    
    # logic #
    # anything suffixed with "l" is due to python... cant name a function "and" or "or" or "in" >:(
    
    def andl(self):
        '''AND
        '''
        self.conditions.append(('AND',))
        return self
    
    def orl(self):
        '''OR
        '''
        self.conditions.append(('OR',))
        return self
    
    def notl(self):
        '''NOT
        '''
        self.conditions.append(('NOT',))
        return self
    
    # operators #
    
    def equals(self, what: str, value: any):
        '''=
        '''
        self.conditions.append(('=', what, value))
        return self
    
    def not_equals(self, what: str, value: any):
        '''!=
        '''
        self.conditions.append(('!=', what, value)) # or also <> ?
        return self

    def greater(self, what: str, value: any):
        '''>
        '''
        self.conditions.append(('>', what, value))
        return self

    def lesser(self, what: str, value: any):
        '''<
        '''
        self.conditions.append(('<', what, value))
        return self

    def greater_equal(self, what: str, value: any):
        '''>=
        '''
        self.conditions.append(('>=', what, value))
        return self

    def lesser_equal(self, what: str, value: any):
        '''<=
        '''
        self.conditions.append(('<=', what, value))
        return self
    
    def like(self, what: str, like: str):
        '''LIKE
        '''
        self.conditions.append((' LIKE ', what, like))
        return self
    
    def ilike(self, what: str, like: str): # (case insensitive)
        '''ILIKE
        '''
        self.conditions.append((' ILIKE ', what, like))
        return self
    
    def inl(self, what: str, range: tuple):
        '''IN
        '''
        self.conditions.append((' IN ', what, range))
        #self.conditions.append(('(',))
        #self.conditions.append((','.join(range),))
        #self.conditions.append((')',))
        return self
    
    def between(self, what: str, range: tuple):
        '''BETWEEN X AND Y
        '''
        self.conditions.append((' BETWEEN', what))
        self.conditions.append((range[0],))
        self.conditions.append(('AND',))
        self.conditions.append((range[1],))
        return self
    
    def is_null(self, what: str):
        '''IS NULL
        '''
        self.conditions.append((' IS NULL', what))
        return self
    
    def is_not_null(self, what: str):
        '''IS NOT NULL
        '''
        self.conditions.append((' IS NOT NULL', what))
        return self
    
    def build(self, cursor: pg.extensions.cursor) -> str: # only time im making a one liner not one line, it's "easier" to "read" (decipher ;-;)
        if len(self.conditions) == 0:
            return self.string
        formatting = ' '.join([f'{cond[1]}{cond[0]}%s' if len(cond) == 3 else (f'{cond[0]}' if len(cond) == 1 else f'{cond[1]}{cond[0]}') for cond in self.conditions])
        vals = [x for x in map(lambda v: v[2] if len(v) == 3 else None, self.conditions) if x is not None]
        return ' WHERE ' + cursor.mogrify(formatting, vals).decode()#[cond for tup in self.conditions for cond in tup[1:]]

class InsertQuery:
    def __init__(self, table: str, keys: list, returning: any) -> None:
        self.query = 'INSERT INTO %s (%s) VALUES ' % (table, ','.join(keys))
        self.template = '(%s)' % (','.join(['%s' for _ in range(len(keys))]))
        self.values = []
        self.total = 0
        self.length = len(keys)
        self.suffix = ' RETURNING %s' % (','.join(self.returning) if isinstance(returning, list) else returning) if returning != None else ''
    
    def add(self, values: list):
        if len(values) != self.length: # validation
            raise ValueError('Invalid number of values! Needed %d, got %d.' % (self.length, len(values)))
        self.values.extend(values)
        self.total += 1
    
    def build(self, cursor):
        return self.query + ','.join([self.template for _ in range(self.total)]) + self.suffix, self.values

class UpdateQuery:
    def __init__(self, table: str, where: Where, returning: any) -> None:
        self.query = 'UPDATE %s SET ' % table
        self.template = '%s='
        self.values = []
        self.total = 0
        self.where = where if where != None else Where()
        self.suffix = ' RETURNING %s' % (','.join(self.returning) if isinstance(returning, list) else returning) if returning != None else ''
    
    def add(self, key: any, value: any):
        self.values.append((key, value))
        self.total += 1
    
    def build(self, cursor):
        return self.query + ','.join([(self.template % (self.values[i][0])) + '%s' for i in range(self.total)]) + self.where.build(cursor) + self.suffix, list(map(lambda v : v[1], self.values))

class DeleteQuery:
    def __init__(self, table: str, where: Where, returning: any) -> None:
        self.query = 'DELETE FROM %s' % table
        self.where = where if where != None else Where()
        self.suffix = ' RETURNING %s' % (','.join(self.returning) if isinstance(returning, list) else returning) if returning != None else ''
    
    def add(self, *args, **kwargs):
        raise NotImplementedError('Delete query doesn\'t require any additions!')
    
    def build(self, cursor):
        return self.query + self.where.build(cursor) + self.suffix, None

class CustomQuery:
    def __init__(self, query: str, values: list):
        self.query = query
        self.values = values
    
    def add(self, *args, **kwargs):
        raise NotImplementedError('Custom query doesn\'t require any additions!')
    
    def build(self, cursor):
        return self.query, self.values

Query = TypeVar('Query')

ALL = -1
ONE = 1

class Transaction(Generic[Query]):
    def __init__(self, cursor: pg.extensions.cursor, query: Query, commitOnExecute=True):
        self.cursor = cursor
        self.query: Query = query
        self.autocommit = commitOnExecute
    
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.execute()
    
    def add(self, *args, **kwargs):
        '''Add to the query
        '''
        print('query:', self.query)
        self.query.add(*args, **kwargs)
    
    def execute(self):
        '''Execute the query
        '''
        execute = self.query.build(self.cursor)
        print('executing', execute)
        self.cursor.execute(*execute)
        if self.autocommit:
            self.commit()
    
    def fetch(self, count: int=-1, group_by: int=None, index: bool=True) -> any:
        '''Fetch rows
        Always returns a list or dict (of rows) depending on group_by

        Parameters
        ----------
        count : int, optional
            How many rows (<=-1 is all), by default -1
        group_by : str, optional
            Group by column, by default None
        index : bool, optional
            Return dictionaries instead, where keys are column names, by default True
        '''
        count = int(count) # make sure it's an int
        if count <= -1:
            rows = self.cursor.fetchall()
        elif count == 1:
            rows = [self.cursor.fetchone()]
        else:
            rows = self.cursor.fetchmany(count)
        # ((a, b, c), (a, b, c))
        if group_by != None: # group by column, so {'name': {...}, 'name2': [...]}
            out = {}
            columns = self.cursor.description
            if isinstance(group_by, str):
                index = next(i for i, v in enumerate(columns) if v.name == group_by)
            else:
                index = int(index)
            for row in rows:
                out[row[index]] = dict([(columns[index].name, row[index]) for index in range(len(row))]) if index else row
            return out
        else: # just return a list, so [{...}, [...]]
            if index:
                columns = self.cursor.description
                rows = [dict([(columns[index].name, row[index]) for index in range(len(row))]) for row in rows]
            return rows
    
    def commit(self):
        self.cursor.connection.commit()
        
    def close(self):
        if not self.cursor.closed:
            self.cursor.close()
    
    def __del__(self):
        self.close()

class Database:
    def __init__(self, dbname: str=None, user: str=None, password: str=None, host: str='localhost', port: int=5432, autoCommit: bool=True):
        '''Create a database

        Parameters
        ----------
        dbname : str
            The database name
        user : str
            The user
        password : str, optional
            The password, by default None
        host : str, optional
            The host, by default 'localhost'
        port : int, optional
            The port, by default 5432
        autoCommit : bool, optional
            Should auto commit after a transaction is built and executed
        '''
        self.credentials = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port,
        }
        self.connection: pg.extensions.connection = None
        self.autoCommit = autoCommit
        self.connect()

    def connect(self):
        '''Connect to the database with credentials given in the constructor
        '''
        self.connection = pg.connect(**self.credentials)
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if not self.connection.closed:
            self.connection.close()
    
    def insert(self, table: str, keys: list, returning: any=None) -> Transaction[InsertQuery]:
        '''Insert >=1 rows

        Parameters
        ----------
        table : str
            The table being inserted into
        keys : list
            The columns being defined
        returning : any, optional
            The columns being returned, by default None

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        transaction = Transaction(self.connection.cursor(), InsertQuery(table, keys, returning), self.autoCommit)
        return transaction
    
    def insertOne(self, table: str, keys: list, values: list, returning: any=None) -> Transaction[InsertQuery]:
        '''Insert 1 row

        Parameters
        ----------
        table : str
            The table being inserted into
        keys : list
            The columns being defined
        values : list
            The column values
        returning : any, optional
            THe columns being returned, by default None

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        with self.insert(table, keys, returning) as t:
            t.add(values)
        return t
    
    def insertDict(self, table: str, set: dict, returning: any=None) -> Transaction[InsertQuery]:
        '''Insert 1 row

        Parameters
        ----------
        table : str
            The table being inserted into
        set : dict
            The {'column': value} dictionary
        returning : any, optional
            The columns being returned, by default None

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        with self.insert(table, list(set.keys()), returning) as t:
            t.add(list(set.values()))
        return t
    
    def update(self, table: str, where: Where, returning: any=None) -> Transaction[UpdateQuery]:
        '''Update rows

        Parameters
        ----------
        table : str
            The table being updated
        where : Where
            The where clause
        returning : any, optional
            The columns being returned, by default None

        Returns
        -------
        Transaction[UpdateQuery]
            The transaction
        '''
        return Transaction(self.connection.cursor(), UpdateQuery(table, where, returning), self.autoCommit)
    
    def updateDict(self, table: str, set: dict, where: Where, returning: any=None) -> Transaction[UpdateQuery]:
        '''Update rows

        Parameters
        ----------
        table : str
            The table being updated
        set : dict
            The {'column': value} dictionary
        where : Where
            The where clause
        returning : any, optional
            The columns being returned, by default None

        Returns
        -------
        Transaction[UpdateQuery]
            The transaction
        '''
        with self.update(table, where, returning) as t:
            for k, v in set.items():
                t.add(k, v)
        return t
    
    def delete(self, table: str, where: Where, returning: any=None) -> Transaction[DeleteQuery]:
        '''Delete rows

        Parameters
        ----------
        table : str
            The table being deleted from
        where : Where
            The where clause
        returning : any, optional
            The columns being returned, by default None

        Returns
        -------
        Transaction[DeleteQuery]
            The transaction, executed immediately
        '''
        t = Transaction(self.connection.cursor(), DeleteQuery(table, where, returning), self.autoCommit)
        with t:
            pass # to be consistent, technically we could just call .execute() or whatever
        return t
    
    def custom(self, sql: str, values: list) -> Transaction[CustomQuery]:
        '''Execute a custom SQL statement

        Parameters
        ----------
        sql : str
            The SQL
        values : list
            The values (%s)

        Returns
        -------
        Transaction[CustomQuery]
            The transaction, executed immediately
        '''
        t = Transaction(self.connection.cursor(), CustomQuery(sql, values), self.autoCommit)
        with t:
            pass # same as above
        return t