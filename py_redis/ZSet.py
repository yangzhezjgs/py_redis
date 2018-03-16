from collections import OrderedDict

class SortedSet:
    def __init__(self):
        self.nodes = {}
        self.Soredlist = OrderedDict()
        self.count = 0

    def insert(self, key, value):
        self.nodes[key] = value
        self.update(self.nodes)
        self.count += 1

    def delete(self, key):
        self.nodes.pop(key)
        self.update(self.nodes)
        self.count -= 1

    def search(self, key):
        return self.nodes[key]

    def update(self, nodes):
        self.Soredlist = OrderedDict(sorted(nodes.items(),key=lambda x:x[1]))


class ZSet:
    def __init__(self):
        self.zset = SortedSet()

    @property
    def content(self):
        return self.zset.Soredlist 

    def zadd(self, value, score):
        return self.zset.insert(value, score)

    def zrem(self, value):
        return self.zset.delete(value)

    def __repr__(self):
        return '%s(%r)' %(self.__class__.__name__, list(self.content.items()))

