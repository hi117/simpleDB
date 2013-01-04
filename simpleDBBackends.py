'''
This file implements all the backends in simpleDB.
each class is put into a list for enumeration.
each class replicates the open, put, set interface as simpleDB as the
implementation requires along with a typeCheck method which returns true
if the path given is an implementation of the given database type.
'''

import pickle
from os.path import exists, isdir, isfile
from os import rename, remove
import types
import inspect

class stdFile:

    magic = b'\xc8\xbc\x16\xdc'
    dict = None
    gaps = None

    def open(self, path):
        '''
        Opens a database.
        '''
        # open the db
        if exists(path):
            self.file = open(path, "r+b",0)
        else:
            self.file = open(path, "w+b",0)
            self.file.write(self.magic + pickle.dumps({}))

        # search for magic number 1KB at a time
        n = 1
        a = b''
        self.file.seek(0,2)
        if self.file.tell() > 1024:
            while not magic in a:
                self.file.seek(n * -1024, 2)
                a += self.file.read(1024)
        else:
            self.file.seek(0)
            a = self.file.read(1024)

        # Truncate to the magic
        while not a[:4] == self.magic:
            a = a[1:]
        a = a[4:]

        # load the dict
        self.dict = pickle.loads(a)
        self.genGaps()

    def get(self, key):
        '''
        Reads a value from the database.
        '''
        try:
            self.file.seek(self.dict[key][0])
            return self.file.read(self.dict[key][1])
        except Exception as e:
            self.dumpDict()
            raise e

    def set(self, key, value):
        '''
        Sets the key to the value.
        '''
        try:
            if key in self.dict:
                if len(value) < self.dict[key][1]:
                    self.dict[key][1] = len(value)
                    self.file.seek(self.dict[key][0])
                    self.file.write(value)
                    return

            # find a starting index and save it to the dict
            index = self.findGap(value)
            self.dict[key] = [index, len(value)]

            # write the dict
            self.file.seek(self.findDictGap())
            self.file.write(self.magic + pickle.dumps(self.dict))

            # truncate the file to the cursor
            self.file.truncate()

            # write the value
            self.file.seek(self.dict[key][0])
            self.file.write(value)
        except Exception as e:
            self.dumpDict()
            raise e

    def remove(self, key):
        '''
        This function removes a key from the database.
        A remove function simply removes it from the dict and the gaps.
        '''
        try:
            index = self.dict[key][0]
            self.dict.pop(key)
            i = None
            for n, gap in enumerate(self.gaps):
                if gap[0] == index:
                    i = n
                    break
            self.gaps.pop(i)
            # write the new dict and trunc
            self.file.seek(self.findDictGap())
            self.file.write(self.magic + pickle.dumps(self.dict))
            self.file.truncate()
        except Exception as e:
            self.dumpDict()
            raise e

    def defrag(self):
        '''
        This function defrags the open database.
        A defrag is just a copy to a temporary where the temporary eventually
        replaces the current database.
        '''
        try:
            # create the temporary database
            name = self.file.name
            if exists(name + '.temp'):
                print('Error defragging, previous defrag interrupted.')
                return False
            tempDB = stdFile()
            tempDB.open(name + '.temp')

            # clone the current databast
            for key in self.dict.keys():
                tempDB.set(key, self.get(key))

            # close them both and replace the old with the temp
            tempDB.close()
            self.close()
            remove(name)
            rename(name + '.temp', name)
            self.open(name)
        except Exception as e:
            self.dumpDict()
            raise e

    def check(self, item):
        '''
        Returns true if item is in the database, false otherwise.
        '''
        return item in self.dict

    def dumpDict(self, path = None):
        if path == None:
            path = self.file.name + '.dump'
        with open(path, 'wb') as dump:
            pickle.dump(self.dict, dump)

    def genGaps(self):
        '''
        Generates a gap tracking datastructre on load.
        '''
        vals = list(self.dict.values())
        self.gaps = []
        if len(vals) == 0: return
        gaps = [vals.pop()]
        for val in vals:
            # insert it into the list in order of start pos since we cant guarintee order
            if val[0] > gaps[-1][0]:
                gaps.append(val)
                continue

            for i, gap in enumerate(gaps):
                if gap[0] > val[0]:
                    break
            gaps.insert(i, val)

        self.gaps = gaps

    def findGap(self, value):
        '''
        This function returns a starting index that is safe to write to given a value.
        It also assumes that it will be written to and adds it to the gap list.
        '''
        if len(self.gaps) == 0:
            self.gaps.append([0, len(value)])
            return 0

        if len(self.gaps) == 1:
            self.gaps.append([self.gaps[0][0] + self.gaps[0][1], len(value)])
            return self.gaps[0][0] + self.gaps[0][1]

        for i, gap in enumerate(self.gaps):
            if i == 0:
                continue
            if (gap[0] - (self.gaps[i - 1][0] + self.gaps[i - 1][1])) > len(value):
                a = [self.gaps[i - 1][0] + self.gaps[i - 1][1], len(value)]
                self.gaps.insert(i, a)
                return a[0]
        # it doesnt fit into any gaps
        a = [self.gaps[-1][0] + self.gaps[-1][1], len(value)]
        self.gaps.append(a)
        return a[0]

    def findDictGap(self):
        '''
        This function returns an index that is safe to write the dict to.
        '''
        if len(self.gaps) == 0: return 0
        return self.gaps[-1][0] + self.gaps[-1][1]

    def close(self):
        try:
            self.file.close()
            self.dict = None
            self.gaps = None
        except Exception as e:
            self.dumpDict()
            raise e

    def checkType(self, path):
        '''
        This function returns true if the path given is a database of the given type.
        if the path does not exist, return false.
        '''
        if not exists(path):
            if path[-3:] == '.db':
                return True
            return False
        if not isfile(path):
            return False
        # if it is a file, then run open() on it (since it does not write it is safe)
        # and check if self.dict is set.
        self.open(path)
        if self.dict:
            self.close()
            return True
        else:
            self.close()
            return False
    def __len__(self):
        return len(self.dict)

def getDB(path):
    '''
    This function take a path and loops over all modules until one returns true for the path.
    If none return true, return None.
    '''
    for i in modules:
        if i.checkType(path):
            return i
    return None

'''
start the generation of the modules list used by getDB
'''
modules = []

# get a list of values from locals()
vals = list(locals().values())

ClassType = type(types.new_class('a'))

# maps needed function names to argument counts
needed = {
        'open': 2,
        'get': 2,
        'set': 3,
        'remove': 2,
        'defrag': 1,
        'check': 2,
        'dumpDict': 2,
        'close': 1,
        '__len__': 1
        }

for i in vals:
    if not type(i) == ClassType:
        continue
    cvals = i.__dict__
    add = True
    for j in needed:
        if not j in cvals:
            add = False
            break
        if not type(cvals[j]) == types.FunctionType:
            add = False
            break
        if not len(inspect.getfullargspec(cvals[j]).args) == needed[j]:
            add = False
            break
        # if it made it this far, then its a function of the same name that
        # takes the same number of arguments, its safe to assume its OK
    if add:
        modules.append(i())
