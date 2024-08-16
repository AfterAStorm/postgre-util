# postgre-util #

Browse the files for the full filing experience.

## postgre.py ##

Wrapper for a Postgre DB using psycopg2 as the backend

### Examples ###

```python
from postgre import Database, Where, Options, ALL, ONE

db = Database(
    dbname='database name',
    user='database role or user',
    password='database role or user password',
    host='database host', # default "localhost"
    #port=5432, # defaults to postgre's default port
    autoCommit=True#, # <-- defaults to True, should it autocommit after executing any statement?
    #schema='public' # defaults to None, which is "public" anyway
                     # \> also just change .schema property if need-be; or just prefix tables
                     # \> with the schema, ex: docs.doc_meta
)

# # #== inserting ==# # #
#
# def insert(self, table: str, keys: list, options: Options=None) -> Transaction[InsertQuery]
# def insertOne(self, table: str, keys: list, values: list, options: Options=None) -> Transaction[InsertQuery]
# def insertDict(self, table: str, set: dict, options: Options=None) -> Transaction[InsertQuery]
#
with db.insert('users', ['name', 'role'], Options().returning('id')) as transaction:
    transaction.add(['system', 0])
new_user = transaction.fetch(ONE)[0] # always returns a list (or dict if fetch(group_by=...)), even if only fetching 1
                            # \> ONE is just 1
new_user_id = user['id'] # by default (if fetch(index=True)), values are returned as dicts with column names
print('Created user', new_user_id)
#
# # #== updating ==# # #
#
# def update(self, table: str, where: Where=None, options: Options=None) -> Transaction[UpdateQuery]
# def updateDict(self, table: str, set: dict, where: Where=None, options: Options=None) -> Transaction[UpdateQuery]
#
with db.update('users', Where().equals('id', new_user_id), Options().returning(['id', 'name', 'role'])) as update:
    update.add('role', 1)
    # update.add('...', ...)
updated_users = update.fetch(ALL) # ALL is just -1
for user in updated_users:
    print('Updated user', user['id'], 'with name', user['name'], 'to role', user['role'])
#
# # #== deleting ==# # #
#
# def delete(self, table: str, where: Where=None, options: Options=None) -> Transaction[DeleteQuery]
#
delete = db.delete('users', Where().equals('id', new_user_id), Options().returning('id,name'))
         # \> delete is immediately executed, as you don't need to ".add()" anything
deleted_users = delete.fetch(ALL)
for user in deleted_users:
    print('Deleted user', user['id'], 'with name', user['name'])
#
# # #== selecting ==# # #
# def select(self, table: str, columns: str, where: Where=None, options: Options=None) -> Transaction[SelectQuery]
#
select = db.select('users', ALL, Where().greater('id', 0))
         # \> much like delete, this is executed immediately
         # \> ALL works as a column, where it just turns into '*'
         # \> values allowed for columns are: -1 (ALL) -> '*', '*', 'column', 'columna,columnb', ['columna', ...], ('columna', ...)
for user in select.fetch(ALL):
    print('User', user['id']) # User 1
                              # name --> system
                              # ...etc
    for key, value in user.items():
        if key == 'id':
            continue
        print(key, '-->', value)
#
# # #== miscellaneous ==# # #
# def custom(self, sql: str, values: list) -> Transaction[CustomQuery]
#
db.custom('SELECT * FROM users WHERE name=%s', ['name'])
#
# # #== utility ==# # #
# def insertOrUpdate(self, table: str, keys: list, values: list, columns: any=None, options: Options=None) -> Transaction[CustomQuery]
# def insertOrUpdateDict(self, table: str, set: dict, columns: any=None, options: Options=None) -> Transaction[CustomQuery]
#
db.insertOrUpdate('users', {
    'name': 'foo',
    'role': 96,
}, 'name') # docstrings explain the use
#
```