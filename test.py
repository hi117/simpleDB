from simpleDB import *
from os.path import exists
from os import remove

if exists('test.db'):
    remove('test.db')

# open new db
print('testing opening...')
db = DB()
db.open('test.db')

input()

# write some values
print('testing writing...')
db.write(b'hello', b'world')
db.write(b'foo', b'bar')

input()

# read some
print('testing reading...')
print(db.read(b'hello'))
print(db.read(b'foo'))

input()

#close
print('testing closing...')
db.close()

input()

# open an existing database
print('testing re-opening...')
db.open('test.db')

input()

# read old values
print('testing re-reading...')
print(db.read(b'hello'))
print(db.read(b'foo'))

input()

# write a new value
print('testing new writing...')
db.write(b'tachi',b'bana')

input()

# read all the values to check
print('final read test...')
print(db.read(b'hello'))
print(db.read(b'foo'))
print(db.read(b'tachi'))

input()

# close the db
print('final close...')
db.close()
