# afterastorm #
# postgre wrapper w/ psycopg2

# if you're curious, the docstring format is numpy's

from typing import Generic, TypeVar
import psycopg2 as pg

__all__ = [ # what the import * fetches
    'Where',
    'Options',
    'Database',
    'ALL',
    'ONE'
]


class Where:
    def __init__(self, string: str=None): # string in-case you already make a statement, adds WHERE for you
        self.conditions = []
        self.string = ' WHERE ' + string.strip().lstrip('WHERE') if string != None else '' # the lstrip assumes the WHERE is capital (if it even exists), if it isn't, then someone isn't following stylish syntax!
    
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
        self.conditions.append((' IN ', what, tuple(range))) # cast to tuple to make sure it's not an iterator
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

def _parseWhere(clause: any):
    if isinstance(clause, Where):
        return clause
    elif isinstance(clause, str):
        return Where(clause)
    elif clause == None:
        return Where()
    raise TypeError('Where clause must be Where, str, or None')

def _parseOptions(options: any):
    if isinstance(options, Options):
        return options
    elif isinstance(options, str):
        return Options(options)
    elif options == None:
        return Options()
    raise TypeError('Options must be Options, str, or None')

def _parseColumns(columnOrColumns: str, allowAll=True):
    if columnOrColumns == ALL or columnOrColumns == '*':
        if not allowAll:
            raise ValueError('Cannot use all columns (aka "*")')
        return '*'
    elif isinstance(columnOrColumns, list) or isinstance(columnOrColumns, tuple):
        return ','.join(columnOrColumns)
    elif isinstance(columnOrColumns, str):
        return columnOrColumns
    raise TypeError('Column(s) must be either a str, "*", list of str, or tuple of str')

class Options:
    ORDERING = { # higher values are placed more rightwards, so LIMIT will alawys be to the left of RETURNING (LIMIT + RETURNING is never possible anyway!)
        'RETURNING': 30,
        'LIMIT': 20,
        'ORDER BY': 10,
    }
    
    def __init__(self, string: str=None): # string in-case you already make a statement
        self.options = []
        self.string = ' ' + string.strip() if string != None else ''
    
    def limit(self, amount: int):
        '''LIMIT X

        Parameters
        ----------
        amount : int
            Maximum # of rows
        '''
        self.options.append((Options.ORDERING['LIMIT'], f'LIMIT {amount}'))
        return self
    
    def orderby(self, columnOrColumns: any):
        '''ORDER BY column(s)

        Parameters
        ----------
        columnOrColumns : str
            Column as a str (or optionally, a list of columns as a list, tuple, or string split with commas) (* not allowed)
        '''
        self.options.append((Options.ORDERING['ORDER BY'], f'ORDER BY {_parseColumns(columnOrColumns, allowAll=False)}')) # i'm not sure why you can use multiple columns ;-;
        return self
    
    def returning(self, columnOrColumns: any):
        '''RETURNING column(s)

        Parameters
        ----------
        columnOrColumns : str
            Column as a str (or optionally, a list of columns as a list, tuple, or string split with commas)
        '''
        self.options.append((Options.ORDERING['RETURNING'], f'RETURNING {_parseColumns(columnOrColumns)}'))
        return self
    
    def custom(self, priority: int, statement: str):
        '''Define a custom option

        Parameters
        ----------
        priority : int
            The priority, use ```Options.ORDERING``` as reference (higher numbers are more rightwards)
        statement : str
            The actual statement, ex: "RETURNING id"
        '''
        self.options.append((priority, statement))
        return self
    
    def build(self, cursor: pg.extensions.cursor) -> str: # only time im making a one liner not one line, it's "easier" to "read" (decipher ;-;)
        if len(self.options) == 0:
            return self.string
        self.options.sort(key=lambda x: x[0]) # sort in ascending order, so ORDER BY x LIMIT y
        return ' ' + ' '.join(map(lambda x: x[1], self.options))

class InsertQuery:
    def __init__(self, table: str, keys: list, options: Options) -> None:
        self.query = 'INSERT INTO %s (%s) VALUES ' % (table, ','.join(keys))
        self.template = '(%s)' % (','.join(['%s' for _ in range(len(keys))]))
        self.values = []
        self.total = 0
        self.length = len(keys)
        self.options = options
    
    def add(self, values: list):
        if len(values) != self.length: # validation
            raise ValueError('Invalid number of values! Needed %d, got %d.' % (self.length, len(values)))
        self.values.extend(values)
        self.total += 1
    
    def build(self, cursor):
        return self.query + ','.join([self.template for _ in range(self.total)]) + self.options.build(cursor), self.values

class UpdateQuery:
    def __init__(self, table: str, where: Where, options: Options) -> None:
        self.query = 'UPDATE %s SET ' % table
        self.template = '%s='
        self.values = []
        self.total = 0
        self.where = where
        self.options = options
    
    def add(self, key: any, value: any):
        self.values.append((key, value))
        self.total += 1
    
    def build(self, cursor):
        return self.query + ','.join([(self.template % (self.values[i][0])) + '%s' for i in range(self.total)]) + self.where.build(cursor) + self.options.build(cursor), list(map(lambda v : v[1], self.values))

class DeleteQuery:
    def __init__(self, table: str, where: Where, options: Options) -> None:
        self.query = 'DELETE FROM %s' % table
        self.where = where
        self.options = options
    
    def add(self, *args, **kwargs):
        raise NotImplementedError('Delete query doesn\'t require any additions!')
    
    def build(self, cursor):
        return self.query + self.where.build(cursor) + self.options.build(cursor), None

class SelectQuery:
    def __init__(self, table: str, what: str, where: Where, options: Options) -> None:
        self.query = 'SELECT %s FROM %s' % (what, table)
        self.where = where
        self.options = options
    
    def add(self, *args, **kwargs):
        raise NotImplementedError('Select query doesn\'t require any additions!')
    
    def build(self, cursor):
        return self.query + self.where.build(cursor) + self.options.build(cursor), None

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
        self.query.add(*args, **kwargs)
    
    def execute(self):
        '''Execute the query
        '''
        execute = self.query.build(self.cursor)
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
        
        Notes
        --------
        Assuming quotes are in the right place and using this table:
        
        ```
        id | name | desc
        1  | a    | object a
        2  | b    | object b
        ```
        
        Group By returns a dictionary where the keys are the column specified, ex:
        
        group_by id (by default, it's just a list as shown below)
        ```
        {
            1: {
                name: a
                desc: object a
            }
            2: {
                name: b
                desc: object b
            }
        }
        ```
        
        Index returns the objects as dictionaries where the keys are column names, ex:
        (same table above)
        
        index False
        ```
        [
            [1, a, object a]
            [2, b, object b]
        ]
        ```
        
        index True (default)
        ```
        [
            {
                id: 1,
                name: a,
                desc: object a
            },
            ...
        ]
        ```
        '''
        count = int(count) # make sure it's an int
        if count <= -1:
            rows = self.cursor.fetchall()
        elif count == 0: # why not support this? :p
            return {} if group_by != None else []
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
    def __init__(self, dbname: str=None, user: str=None, password: str=None, host: str='localhost', schema: str=None, port: int=5432, autoCommit: bool=True):
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
        schema : str, optional
            The schema, prefixes table names with "schema."
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
        self.schema = None
        self.connection: pg.extensions.connection = None
        self.autoCommit = autoCommit
        self.connect()

    def connect(self):
        '''Connect to the database with credentials given in the constructor
        '''
        self.connection = pg.connect(**self.credentials)
    
    def commit(self):
        '''Commit the database changes
        '''
        self.commit()
    
    def _parseTable(self, table: str):
        if not isinstance(table, str):
            raise TypeError('Table must be a str')
        if self.schema != None:
            return self.schema + '.' + table
        else:
            return table
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if not self.connection.closed:
            self.connection.close()
    
    def insert(self, table: str, keys: list, options: Options=None) -> Transaction[InsertQuery]:
        '''Insert >=1 rows

        Parameters
        ----------
        table : str
            The table being inserted into
        keys : list
            The columns being defined
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        transaction = Transaction(self.connection.cursor(), InsertQuery(self._parseTable(table), keys, _parseOptions(options)), self.autoCommit)
        return transaction
    
    def insertOne(self, table: str, keys: list, values: list, options: Options=None) -> Transaction[InsertQuery]:
        '''Insert 1 row

        Parameters
        ----------
        table : str
            The table being inserted into
        keys : list
            The columns being defined
        values : list
            The column values
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        with self.insert(table, keys, _parseOptions(options)) as t:
            t.add(values)
        return t
    
    def insertDict(self, table: str, set: dict, options: Options=None) -> Transaction[InsertQuery]:
        '''Insert 1 row

        Parameters
        ----------
        table : str
            The table being inserted into
        set : dict
            The {'column': value} dictionary
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[InsertQuery]
            The transaction
        '''
        with self.insert(table, list(set.keys()), _parseOptions(options)) as t:
            t.add(list(set.values()))
        return t
    
    def update(self, table: str, where: Where=None, options: Options=None) -> Transaction[UpdateQuery]:
        '''Update rows

        Parameters
        ----------
        table : str
            The table being updated
        where : Where
            The where clause
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[UpdateQuery]
            The transaction
        '''
        return Transaction(self.connection.cursor(), UpdateQuery(self._parseTable(table), _parseWhere(where), _parseOptions(options)), self.autoCommit)
    
    def updateDict(self, table: str, set: dict, where: Where=None, options: Options=None) -> Transaction[UpdateQuery]:
        '''Update rows

        Parameters
        ----------
        table : str
            The table being updated
        set : dict
            The {'column': value} dictionary
        where : Where
            The where clause
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[UpdateQuery]
            The transaction
        '''
        with self.update(table, _parseWhere(where), _parseOptions(options)) as t:
            for k, v in set.items():
                t.add(k, v)
        return t
    
    def delete(self, table: str, where: Where=None, options: Options=None) -> Transaction[DeleteQuery]:
        '''Delete rows

        Parameters
        ----------
        table : str
            The table being deleted from
        where : Where
            The where clause
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[DeleteQuery]
            The transaction, executed immediately
        '''
        t = Transaction(self.connection.cursor(), DeleteQuery(self._parseTable(table), _parseWhere(where), _parseOptions(options)), self.autoCommit)
        with t:
            pass # to be consistent, technically we could just call .execute() or whatever
        return t
    
    def select(self, table: str, columns: str, where: Where=None, options: Options=None) -> Transaction[SelectQuery]:
        '''Select rows

        Parameters
        ----------
        table : str
            The table being selected
        columns : str
            What columns are we selecting,
            columns as a str (or optionally, a list of columns as a list, tuple, or string split with commas)
        where : Where
            The where clause
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[SelectQuery]
            The transaction, executed immediately
        '''
        t = Transaction(self.connection.cursor(), SelectQuery(self._parseTable(table), columns, _parseWhere(where), _parseOptions(options)), self.autoCommit)
        with t:
            pass
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
    
    # extras / utilities #
    
    def insertOrUpdate(self, table: str, keys: list, values: list, columns: any=None, options: Options=None) -> Transaction[CustomQuery]:
        '''Insert or Update one row

        Parameters
        ----------
        table : str
            The table being inserted into
        keys : list
            The columns being defined
        values : list
            The column values
        columns : str
            Columns as a str that can conflict (or optionally, a list of columns as a list, tuple, or string split with commas) (* not allowed)
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[CustomQuery]
            The transaction, executed immediately
        '''
        cursor = self.connection.cursor()
        table = self._parseTable(table)
        options = _parseOptions(options) # parse all variables since they are used multiple times!
        columns = _parseColumns(columns, allowAll=False)
        insert = InsertQuery(table, keys, options) # options are parsed themselves by the queries, very convienent!
        insert.add(values) # <-- will handle len(keys) != len(values) for us :D
        update = UpdateQuery('', Where(), options)
        for i in range(len(keys)):
            update.add(keys[i], values[i])
        insertQuery, insertValues = insert.build(cursor) # build both statements to compile them later
        updateQuery, updateValues = update.build(cursor)
        # optionsQuery = options.build(cursor) # jk, queries do it for us! ~~ has left padding space, that's why the below format string looks weird :D
        # you can't do RETURNING with this... isn't SQL just so fun? (╯°□°）╯︵ ┻━┻
        # well you can... just look at stackoverflow and use a .custom() statement if you care so much
        
        t = Transaction(cursor, CustomQuery(f'{insertQuery} ON CONFLICT ({columns}) DO {updateQuery}', [*insertValues, *updateValues]), self.autoCommit)
        with t:
            pass
        return t
        
    
    def insertOrUpdateDict(self, table: str, set: dict, options: Options=None) -> Transaction[CustomQuery]:
        '''Insert or Update one row

        Parameters
        ----------
        table : str
            The table being inserted into or updated
        set : dict
            The {'column': value} dictionary
        options : Options, optional
            Special options appended at the end of a statement, such as RETURNING or LIMIT

        Returns
        -------
        Transaction[CustomQuery]
            The transaction
        '''
        return self.insertOrUpdate(table, list(set.keys()), list(set.values()), options)