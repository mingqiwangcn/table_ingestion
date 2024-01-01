import util
import hashlib

class Bin:
    def __init__(self):
        self.item_lst = []

    def add(self, item):
        self.item_lst.append(item)
    
class BinTable:
    def __init__(self, tag, size)
        self.tag = tag
        self.size = size
        self.bin_array = [None] * size

    def add(key, item):
        digest = hashlib.sha256(key.encode()).digest()
        slot = int.from_bytes(digest, 'big') % self.size
        if self.bin_array[slot] is None
            self.bin_array[slot] = new Bin()
        bin_entry = self.bin_array[slot]
        bin_entry.add(item)

def choose_hash_table_size(num_items):
    size = int(num_items * 4 / 3) # load factor aournd 0.75
    while not util.is_prime(size):
        size += 1
    return size
