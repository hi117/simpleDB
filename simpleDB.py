import pickle
from os.path import exists
from os import rename, remove
import simpleDBBackends

class DB:
    def open(self, path, Type = None):
        '''
        Opens a database.
        '''
        if not type:
            # first search for the database type
            self.DB = simpleDBBackends.getDB(path)
        else:
            self.DB = simpleDBBackends.getDBFromType(Type)
        self.DB.open(path)

    def get(self, key):
        '''
        Reads a value from the database.
        '''
        return self.DB.get(key)

    def set(self, key, value):
        '''
        Sets the key to the value.
        '''
        self.DB.set(key, value)

    def remove(self, key):
        '''
        This function removes a key from the database.
        A remove function simply removes it from the dict and the gaps.
        '''
        self.DB.remove(key)

    def defrag(self):
        '''
        This function defrags the open database.
        A defrag is just a copy to a temporary where the temporary eventually 
        replaces the current database.
        '''
        self.DB.defrag()

    def check(self, item):
        '''
        Returns true if item is in the database, false otherwise.
        '''
        return self.DB.check(item)

    def dumpDict(self, path = None):
        self.DB.dumpDict(path)

    def close(self):
        self.DB.close()

    def __len__(self):
        return self.DB.__len__()
    
    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.remove(key)

    def __contains__(self, item):
        self.check(item)
