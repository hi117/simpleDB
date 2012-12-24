from simpleDB import *
from os.path import exists
from os import remove

if exists('test.db'):
    remove('test.db')

# open new db
print('testing opening...')
db = DB()
db.open('test.db')

# write some values
print('testing writing...')
db.write(b'hello', b'world')
db.write(b'foo', b'bar')

# read some
print('testing reading...')
print(db.read(b'hello'))
print(db.read(b'foo'))

# add and remove
print('testing remove...')
db.remove(b'hello')
db.write(b'bla', b'bla')

#close
print('testing closing...')
db.close()

# open an existing database
print('testing re-opening...')
db.open('test.db')

# read old values
print('testing re-reading...')
print(db.read(b'bla'))
print(db.read(b'foo'))

# write a new value
print('testing new writing...')
db.write(b'tachi',b'bana')

input()

# read all the values to check
print('final read test...')
print(db.read(b'bla'))
print(db.read(b'foo'))
print(db.read(b'tachi'))

input()

# close the db
print('final close...')
db.close()
