from simpleDB import *
from os.path import exists, isdir, isfile
from shutil import rmtree
from os import remove

types = ['stdFile', 'dirDB']

for i in types:
    print('Testing: ' + i)

    if exists('test.db'):
        if isfile('test.db'): remove('test.db')
        elif isdir('test.db'): rmtree('test.db')
    if exists('test.db.temp'):
        if isfile('test.db.temp'): remove('test.db.temp')
        elif isdir('test.db.temp'): remove('test.db.temp')

    # open new db
    print('testing opening...')
    db = DB()
    db.open('test.db', i)

    # set some values
    print('testing writing...')
    db.set(b'hello', b'world')
    db.set(b'foo', b'bar')

    # get some
    print('testing geting...')
    print(db.get(b'hello'))
    print(db.get(b'foo'))

    # add and remove
    print('testing remove...')
    db.remove(b'hello')
    db.set(b'bla', b'bla')

    #close
    print('testing closing...')
    db.close()

    # open an existing database
    print('testing re-opening...')
    db.open('test.db', i)

    # get old values
    print('testing re-geting...')
    print(db.get(b'bla'))
    print(db.get(b'foo'))

    # set a new value
    print('testing new writing...')
    db.set(b'tachi',b'bana')

    # defrag
    print('testing defrag...')
    db.defrag()

    # get all the values to check
    print('final get test...')
    print(db.get(b'bla'))
    print(db.get(b'foo'))
    print(db.get(b'tachi'))

    # close the db
    print('final close...')
    db.close()
