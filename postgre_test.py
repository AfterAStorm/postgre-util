
# read .env #
from configparser import ConfigParser

parser = ConfigParser()
with open('.env', 'r') as f:
    parser.read_file(f)

# test #
from postgre import Database, Where

db = Database(
    dbname=parser.get('db', 'dbnm'),
    user=parser.get('db', 'user'),
    password=parser.get('db', 'pass'),
    host=parser.get('db', 'host'),
    autoCommit=True # <-- defaults to True
)

with db.insert('posts', ['title', 'contents', 'poster'], 'id') as trans:
    trans.add(('title', 'contents', 621))
print('INSERT INTO:', trans.fetch(-1))
# if autoCommit wasn't True, you'd have to do
# trans.commit() -- something to note:
#                   Transaction.commit() will theoretically commit EVERY change, even from other active transactions
#                   if this isn't desirable, make multiple database connections (?)

where = Where().equals('id', 3).andl()  \
.is_not_null('id').andl()               \
.lesser_equal('id', 3)                  \
    .orl()                              \
.between('id', (10, 20)).andl()         \
.inl('id', (3,))
     
with db.update('posts', where, 'id') as trans:
    trans.add('title', 'new title')
print('UPDATE:', trans.fetch(-1))

with db.delete('posts', Where().greater('id', 3), 'id') as trans:
    pass
print('DELETE:', trans.fetch(-1))