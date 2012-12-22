import pickle
from os.path import exists

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
        self.file.seek(0,2)
        if self.file.tell() > 1024:
            while not b'\x80\x03' in a:
                self.file.seek(n * -1024, 2)
                a = self.file.read(1024) + a
        else:
            self.file.seek(0)
            a = self.file.read(1024)

        # Truncate to the magic
        while not a[:2] == b'\x80\x03':
            a = a[1:]

        # load the dict
        self.dict = pickle.loads(a)
        print(self.dict)

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
        
        # write the dict
        self.file.seek(len(value) + 1, 2)
        self.file.write(pickle.dumps(self.dict))

        # write the value
        self.file.seek(self.dict[key][0])
        self.file.write(value)

    def close(self):
        self.file.close()
        self.dict = None
