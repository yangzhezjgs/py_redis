import socket
import pickle
import selectors

from datatype import *
from logger import logger

class RedisServer:
    def __init__(self, selector, sock, host='127.0.0.1', port=8880):
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
        self.selector = selector
        self.sock = sock
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
        self.load()
        self.process_request()
    
    def register_commands(self):
        for k in self.datas:
            command_map = self.datas[k].register_command()
            self.commands_map.update(command_map)

    def execute_command(self, command):
        logger.info("execute %s", ''.join(command.split('\r\n')))
        commands = command.split('\r\n')
        rows = int(commands[0][1])
        if rows == 3:
            method, key, value = commands[2],commands[4],commands[6]
            self.commands_map[method](key, value)
        elif rows == 4:
            method, key, value, value2 = commands[2],commands[4],commands[6],commands[8]
            self.commands_map[method](key, value, value2)

    def process_request(self):
        logger.info("listen  to %s:%s"%(self.host, self.port))
        self.sock.bind((self.host, self.port))
        self.sock.listen(1000)
        self.sock.setblocking(False)
        self.selector.register(sock, selectors.EVENT_READ, self.accept)
        while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def accept(self, sock, mask):
        conn, addr = sock.accept()
        logger.info("accepted conn from %s", addr)
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        data = conn.recv(1024)
        command = str(data, encoding="utf8")
        if command != 'exit':
            self.execute_command(command)
            self.dump()
            conn.send('ok'.encode('utf8'))
        elif command == 'exit':
            print('closing',conn)
            self.selector.unregister(conn)
            conn.close()

if __name__ == '__main__':
    selector = selectors.DefaultSelector()
    sock = socket.socket()
    redis = RedisServer(selector, sock)
    redis.run()
