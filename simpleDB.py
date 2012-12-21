import pickle
from os import exists

class DB:
    def open(self, path):
        '''
        Opens a database.
        '''
        # open the db
        if exists(path):
            self.file = open(path, "r+b")
        else:
            self.file = open(path, "w+b")
            self.file.write(pickle.dumps({}))

        # search for magic number 1KB at a time
        n = 1
        a = b''
        while not b'\x80\x03' in a:
            self.file.seek(n * -1024, 2)
            a = self.file.read(1024) + a

        # Truncate to the magic
        while not a[:2] == b'\x80\x03':
            a.pop(0)

        # load the dict
        self.dict = pickle.loads(a)

    def read(self, key):
        '''
        Reads a value from the database.
        '''
        self.file.seek(self.dict[key][0])
        return self.file.read(self.dict[key][1])
    def write(self, key, value):
        '''
        Sets the key to the value.
        '''
        if key in self.dict:
            if len(value) < self.dict[key][1]:
                self.dict[key][1] = len(value)
                self.file.seek(self.dict[key][0])
                self.file.write(value)
                return

        # TODO: implement gap finding
        self.file.seek(0, 2)
        self.dict[key] = [self.file.tell(), len(value)]
        self.file.seek(len(value) + 1, 2)
        self.file.write(pickle.dumps(self.dict))
        self.file.seek(self.dict[key][0])
        self.file.write(value)
