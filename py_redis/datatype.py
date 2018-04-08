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

    def register_command(self):
        commands = {}
        commands['ZADD'] = self.zadd
        return commands


class SetStore(DataBase):
    data_type = set

    def sadd(self, key, value):
        self.create_key(key)
        self.data[key].add(value)
    
    def smembers(self, key):
        return str(self.data[key])

    def load(self, nodes):
        for k, v in nodes.items():
            for x in v:
                self.sadd(k, x)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes
    
    def register_command(self):
        commands = {}
        commands['SADD'] = self.sadd
        commands['SMEMBERS'] = self.smembers
        return commands


class StrStore(DataBase):
    data_type = str

    def set(self, key, value):
        self.create_key(key)
        self.data[key] = value

    def get(self, key):
        return self.data[key]

    def load(self, nodes):
        for k, v in nodes.items():
            self.set(k ,v)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes

    def register_command(self):
        commands = {}
        commands['SET'] = self.set
        commands['GET'] = self.get
        return commands

        

class HashStore(DataBase):
    data_type = dict

    def hset(self, key, field, value ):
        self.create_key(key)
        self.data[key][field] = value

    def hget(self, key, field):
        return self.data[key][field]

    def load(self, nodes):
        for k, v in nodes.items():
            for field, value in v.items():
                self.hset(k, field, value)

    def dump(self):
        nodes = {}
        for k, v in self.data.items():
            nodes[k] = v
        return nodes
    
    def register_command(self):
        commands = {}
        commands['HSET'] = self.hset
        commands['HGET'] = self.hget
        return commands

        

class ListStore(DataBase):
    data_type = list

    def lpop(self, key):
        return self.data[key].pop(0)

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
    
    def register_command(self):
        commands = {}
        commands['LPUSH'] = self.lpush
        commands['LPOP'] = self.lpop
        return commands
