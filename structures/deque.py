from collections import defaultdict, OrderedDict
from typing import Dict, Tuple
class Deque(object):
    'Fast searchable queue'

    

    def __init__(self):
        self.od = OrderedDict()

    def appendleft(self, k):
        if k in self.od:
            del self.od[k]
        self.od[k] = None

    def pop(self):
        return self.od.popitem(0)[0]

    def remove(self, k):
        del self.od[k]

    def __len__(self):
        return len(self.od)

    def __contains__(self, k):
        return k in self.od

    def __iter__(self):
        return reversed(self.od)

    def __repr__(self):
        return 'Deque(%r)' % (list(self),)
