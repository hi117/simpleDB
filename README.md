This implements a really simple k,v database.
The database consists with values concatanated to eachother.
At the end is a pickled dictionary mapping keys to an offset,size pair.
This allows for reading of the data through a seek() and read().
The pickled data's start can be found through its magic number \x80\x03.

Writing to New:
    To write, a value's size is first determined.
    Then we seek to end + size + 1.
    Then we add the new key,offset,size to the in-memory mapping dictionary.
    Then we write the dictionary to the end of the database.
    We do this so that if there is an error in the writing process, the database is rcoverable.
    In case of any error, the dictionary is dumped so recovery is possible.
    We then seek to the offset specified in the dictionary and write the value to the database.
Reading:
    Seek to dict[keu][offset]
    Read(dict[key][size])
    return
Removing:
    remove the key from the dictionary.
Wirting to Existing:
    If the new size is equal smaller, simply seek to existing and overwrite.
    Also change dict[key][size] to reflect the new size
    Else treat as a new key
Defrag:
    Online:
        Online defragging is possible through inserting new values into gaps createted
        when removing data.  This is sometimes hard to accomplish and not very effective.
    Offline:
        Completely rebuilds the database for maximum defragging.
Opening:
    Open the file.
    Look backwards for the magic number.
    Read the dict to memory.
This database is based off of kytotcabinet and as such, follows its API, 
though the implementation is much different.
