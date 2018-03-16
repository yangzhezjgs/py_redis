import sys
from socket import *

client=socket()
client.connect(('127.0.0.1',8889))
while True:
    sys.stdout.write('>')
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
