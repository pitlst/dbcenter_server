# import socket
# # 创建客户端套接字
# sk = socket.socket()           
# # 尝试连接服务器
# sk.connect(('localhost',10089))
# print("开始发送")
# while True:
#     # 信息发送
#     info = "hello world"
#     sk.send(bytes(info,encoding='utf-8'))
# # 关闭客户端套接字
# sk.close()


import socket
# 创建客户端套接字
sk = socket.socket()           
# 尝试连接服务器
sk.connect(('localhost',10089))
print("开始发送")
while True:
    # 信息发送
    info = input()
    sk.send(bytes(info + ";",encoding='utf-8'))
# 关闭客户端套接字
sk.close()