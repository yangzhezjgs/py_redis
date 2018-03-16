import pickle

from socket import socket
from datatype import *

class RedisServer:
    def __init__(self, host='127.0.0.1', port=8889):
        self.datas = {
                'ZSET': ZSetStore(),
                'STR': StrStore(),
                'SET': SetStore(),
                'HASH': HashStore(),
                'LIST': ListStore()
                }
        self.ZSet = self.datas['ZSET']
        self.Set = self.datas['SET']
        self.Str = self.datas['STR']
        self.Hash = self.datas['HASH']
        self.List = self.datas['LIST']
        self.host = host
        self.port = port
        self.commands_map = {'SET': self.Str.set}

    def load(self):
        with open('redis.db', 'rb') as f:
                datas = pickle.load(f)
        for k in self.datas:
            self.datas[k].load(datas[k])


    def dump(self):
        datas = {}
        for k in self.datas:
            datas[k] = self.datas[k].dump()
        with open('redis.db', 'wb') as f:
                pickle.dump(datas, f)

    def run(self):
        self.process_request()


    def execute_command(self, command):
        commands = command.split('\r\n')
        method, key, value = commands[2],commands[4],commands[6]
        self.commands_map[method](key, value)


    def process_request(self):
        server = socket()
        server.bind((self.host, self.port))
        server.listen(10)
        while True:
            conn, addr = server.accept()
            accept_data = conn.recv(1024)
            command = str(accept_data, encoding="utf8")
            self.execute_command(command)
            #conn.send()
            print(self.Str.data)
            conn.close()

if __name__ == '__main__':
    redis = RedisServer()
    redis.run()
