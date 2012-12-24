import pickle
from os.path import exists

magic = b'\xc8\xbc\x16\xdc'

class DB:
    def open(self, path):
        '''
        Opens a database.
        '''
        # open the db
        if exists(path):
            self.file = open(path, "r+b",0)
        else:
            self.file = open(path, "w+b",0)
            self.file.write(magic + pickle.dumps({}))

        # search for magic number 1KB at a time
        n = 1
        a = b''
        self.file.seek(0,2)
        if self.file.tell() > 1024:
            while not magic in a:
                self.file.seek(n * -1024, 2)
                a = self.file.read(1024) + a
        else:
            self.file.seek(0)
            a = self.file.read(1024)

        # Truncate to the magic
        while not a[:4] == magic:
            a = a[1:]
        a = a[4:]

        # load the dict
        self.dict = pickle.loads(a)
        self.genGaps()
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

        # find a starting index and save it to the dict
        index = self.findGap(value)
        self.dict[key] = [index, len(value)]
        
        # write the dict
        self.file.seek(self.findDictGap())
        self.file.write(magic + pickle.dumps(self.dict))

        print('pausing...')
        print(self.gaps)
        input()

        # write the value
        self.file.seek(self.dict[key][0])
        self.file.write(value)

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

        print(gaps)
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
                print(a)
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
        self.file.close()
        self.dict = None
