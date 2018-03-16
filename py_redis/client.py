from socket import *
client=socket(AF_INET, SOCK_STREAM)
client.connect(('127.0.0.1',8889))
s = '*3\r\n$3\r\nSET\r\n$3\r\nssddd\r\n$4\r\nlong\r\n'
a = client.send(s.encode('utf8'))
data = client.recv(1024).decode()
print(data)
