
# read .env #
from configparser import ConfigParser

parser = ConfigParser()
with open('.env', 'r') as f:
    parser.read_file(f)

# test #
from postgre import Database, Where, Options, ALL

db = Database(
    dbname=parser.get('db', 'dbnm'),
    user=parser.get('db', 'user'),
    password=parser.get('db', 'pass'),
    host=parser.get('db', 'host'),
    #port=5432, # defaults to postgre's default port
    autoCommit=True#, # <-- defaults to True
    #schema='public' # defaults to nothing, which is "public" anyway
)

with db.insert('posts', ['title', 'contents', 'poster'], Options().returning('id')) as insert:
    insert.add(('title', 'contents', 621))
INSERTS = insert.fetch(-1)
print('INSERT INTO:', INSERTS)
# if autoCommit wasn't True, you'd have to do
# trans.commit() -- something to note:
#                   Transaction.commit() will theoretically commit EVERY change, even from other active transactions
#                   if this isn't desirable, make multiple database connections (?)

# all this does is select "3", while showing off the builder
where = Where().equals('id', 3).andl()  \
.is_not_null('id').andl()               \
.lesser_equal('id', 3)                  \
    .orl()                              \
.between('id', (10, 20)).andl()         \
.inl('id', (3,))
     
with db.update(
    'posts', # table
    where, # where
    Options().returning('id') # options
    ) as trans:
    trans.add('title', 'new title') # add values
print('UPDATE:', trans.fetch(-1))

# also updateDict for one row

print('DELETE:', db.delete(
    'posts',
    Where().inl('id', map(lambda x : x['id'], INSERTS)),
    Options().returning('id')
).fetch(-1))

import random

print('INSERT/UPDATE:', db.insertOrUpdate(
    'posts', # table
    ['id', 'title', 'contents', 'poster'], # keys (also insertOrUpdateDict for a {key: value} instead of two lists... although it just turns .keys() and .values() into lists anyway and ports it to this method :p)
    [3, 'rawr', 'xd', random.randint(111, 999)], # values
    'id', # columns, we have to define them for ON CONFLICT (columns...), and can't use a wildcard D:
          # it can be a list of columns if needed, either as "a,b,c" or ["a", "b", "c"]
          #                                                \> this goes for any "column" requirement, unless stated otherwise
    None # options, you can't do RETURNING on ON CONFLICT sadly, without some funky stuff at least
)) 

print('SELECT:', db.select(
    'posts', # table
    '*', # columns we are selecting
    None,#Where().equals('id', 1), # where
    Options().orderby('id') # options
).fetch(ALL)) # ALL is the same as -1