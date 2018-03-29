import sys
from socket import *

RUN = True
STOP = False

class Shell:
    def display_cmd_prompt(self):
        sys.stdout.write('py-redis >')
        sys.stdout.flush()

    def get_cmd(self):
        cmd = sys.stdin.readline()
        tokens = cmd.split()
        return tokens

class Client:
    def __init__(self):
        self.client=socket()
        self.client.connect(('127.0.0.1',8889))
        self.shell = Shell()
        self.status = RUN

    def generate_requests(self, tokens):
        cmds = []
        if tokens == ['exit']:
            self.status = STOP
            return ''

        for t in tokens:
            cmds.append("$%s\r\n%s\r\n" % (len(t), t))
        s = "*%s\r\n%s" % (len(tokens), "".join(cmds))
        return s

    def run(self):
        while self.status == RUN:
            self.shell.display_cmd_prompt()
            tokens = self.shell.get_cmd()
            request = self.generate_requests(tokens)
            if request == '':
                continue
            self.client.sendall(request.encode('utf8'))
            response = self.client.recv(1024).decode()
            print(response)
        self.client.close()
'''
client=socket()
client.connect(('127.0.0.1',8889))
while True:
    sys.stdout.write('py-redis >')
    sys.stdout.flush()

    cmd = sys.stdin.readline()
    tokens = cmd.split()
    cmds = []
    for t in tokens:
        cmds.append("$%s\r\n%s\r\n" % (len(t), t))
    s = "*%s\r\n%s" % (len(tokens), "".join(cmds))
    client.sendall(s.encode('utf8'))
    data = client.recv(1024).decode()
    print(data)

client.close()
'''
if __name__ == '__main__':
    client = Client()
    client.run()
