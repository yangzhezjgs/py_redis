from socket import *
client=socket(AF_INET, SOCK_STREAM)
client.connect(('127.0.0.1',8889))
s = '*3\r\n$3\r\nSET\r\n$3\r\nc\r\n$4\r\nlddd\r\n'
a = client.send(s.encode('utf8'))
print(a)
