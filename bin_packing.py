class Item:
    def __init__(self, key, size):
        self.key = key
        self.size = size

class Bin:
    def __init__(self, space):
        self.space = space
        self.item_lst = []

    def can_fit(self, item):
        return item.size <= self.space

    def add(self, item):
        if item.size > self.space:
            raise ValueError('Not Fit')
        self.item_lst.append(item)
        self.space -= item.size
    
#Use First-FIt (FF) algorithm
def bin_packing(item_lst, bin_capacity):
    bin_lst = []
    for item in item_lst:
        first_fit_bin = None
        for bin_entry in bin_lst:
            if bin_entry.can_fit(item):
                first_fit_bin = bin_entry
                break
        if first_fit_bin is None:
            first_fit_bin = Bin(bin_capacity)
            bin_lst.append(first_fit_bin)
        first_fit_bin.add(item)
    return bin_lst
