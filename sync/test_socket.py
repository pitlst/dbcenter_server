import socket
# 创建服务器端套接字
sk = socket.socket()
# 设置给定套接字选项的值。
# sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# 把地址绑定到套接字
sk.bind(('localhost', 10089))
# 监听链接
sk.listen()
# 接受客户端链接
conn, addr = sk.accept()
while True:
    # 接收客户端信息
    ret = conn.recv(1024)
    # 打印客户端信息
    print(ret.decode('utf-8'))
# 关闭客户端链接
conn.close()
# 关闭服务器套接字
sk.close()  