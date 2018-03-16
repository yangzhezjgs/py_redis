import pickle

from bases import DataBase
from ZSet import ZSet

class ZSetStore(DataBase):
    data_type = ZSet

    def zadd(self, key, score, value):
        self.create_key(key)
        self.data[key].zadd(value, score)
 
    def load(self, nodes):
        for k,v in nodes.items():
            for value, score in v.items():
                self.zadd(k, score, value)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v.zset.nodes
        return nodes

class SetStore(DataBase):
    data_type = set

    def sadd(self,key,value):
        self.create_key(key)
        self.data[key].add(value)

    def load(self, nodes):
        for k, v in nodes.items():
            for x in v:
                self.sadd(k, x)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes


class StrStore(DataBase):
    data_type = str

    def set(self, key, value):
        self.create_key(key)
        self.data[key] = value

    def load(self, nodes):
        for k, v in nodes.items():
            self.set(k ,v)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes

        

class HashStore(DataBase):
    data_type = dict

    def hset(self, key, field, value ):
        self.create_key(key)
        self.data[key][field] = value

    def load(self, nodes):
        for k, v in nodes.items():
            for field, value in v.items():
                self.hset(k, field, value)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes

        

class ListStore(DataBase):
    data_type = list

    def lpush(self, key, value):
        self.create_key(key)
        self.data[key].insert(0, value)
        
    def load(self, nodes):
        for k, v in nodes.items():
            for x in v:
                self.lpush(k,x)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes
