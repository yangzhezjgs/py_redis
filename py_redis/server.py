import pickle

from socket import *
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
        self.commands_map = {}

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
        self.register_commands()
        self.process_request()
    
    def register_commands(self):
        d = self.datas['ZSET'].register_command()
        self.commands_map.update(d)
        s = self.datas['STR'].register_command()
        self.commands_map.update(s)


    def execute_command(self, command):
        commands = command.split('\r\n')
        method, key, value = commands[2],commands[4],commands[6]
        self.commands_map[method](key, value)


    def process_request(self):
        server = socket()
        server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(10)
        while True:
            conn, addr = server.accept()
            accept_data = conn.recv(1024)
            command = str(accept_data, encoding="utf8")
            self.execute_command(command)
            #conn.send()
            print(self.Str.data)
            conn.sendall("ok".encode())
            conn.close()

if __name__ == '__main__':
    redis = RedisServer()
    redis.run()
