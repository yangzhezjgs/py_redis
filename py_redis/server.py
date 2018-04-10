import os
import socket
import pickle
import selectors
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock

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
        self.host = host
        self.port = port
        self.selector = selector
        self.sock = sock
        self.commands_map = {}
        self.pool = ThreadPool(processes=4)
        self.lock = Lock()

    def load(self):
        if os.path.exists('redis.db'):
            with open('redis.db', 'rb') as f:
                    datas = pickle.load(f)
            for k in self.datas:
                self.datas[k].load(datas[k])
        else:
            self.dump()

    def dump(self):
        with self.lock:
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
        commands = command.split('\r\n')
        rows = int(commands[0][1])
        method = commands[2].upper()

        if rows == 2:
            method , key = method, commands[4]
            logger.info("execute %s", ' '.join([method, key]))
            try:
                message = self.commands_map[method](key)
            except Exception:
                logger.error("execute %s", ' '.join([method, key]))
                return 'Error'
            return message
        elif rows == 3:
            method, key, value = method, commands[4], commands[6]
            logger.info("execute %s", ' '.join([method, key, value]))
            try:
                message = self.commands_map[method](key, value)
                if message == None:
                    message = 'OK'
            except Exception:
                logger.error("execute %s", ' '.join([method, key, value]))
                return 'Error'
            return message
        elif rows == 4:
            method, key, value, value2 = method, commands[4], commands[6], commands[8]
            logger.info("execute %s", ' '.join([method, key, value, value2]))
            try:
                message = self.commands_map[method](key, value, value2)
                if message == None:
                    message = 'OK'
            except Exception:
                logger.error("execute %s", ' '.join([method, key, value, value2]))
                return 'Error'
            return message
        else:
            logger.error("execute %s", ''.join(commands))
            return 'Error'

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
            message = self.pool.apply(self.execute_command, (command,))
            self.dump()
            conn.send(message.encode('utf8'))
        elif command == 'exit':
            print('closing',conn)
            self.selector.unregister(conn)
            conn.close()

if __name__ == '__main__':
    selector = selectors.DefaultSelector()
    sock = socket.socket()
    redis = RedisServer(selector, sock)
    redis.run()
