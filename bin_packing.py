class Bin:
    def __init__(self, space):
        self.space = space
        self.item_lst = []

    def can_fit(self, item):
        return item[1] <= self.space

    def add(self, item):
        item_size = item[1]
        if item_size > self.space:
            raise ValueError('Not Fit')
        self.item_lst.append(item)
        self.space -= item_size
    
#Use First-FIt (FF) algorithm
def bin_pack(item_lst, bin_capacity):
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
        try: 
            first_fit_bin.add(item)
        except:
            import pdb; pdb.set_trace()
            print()

    return bin_lst
