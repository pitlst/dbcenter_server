import toml
import socket
import time
import datetime
from sync.node.base import node_base  # 用于类型标注
from general.config import SYNC_CONFIG
from general.logger import node_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

'''
这里主要想实现的一个想法是，对于没有前后依赖的sql同步节点
在程序触发时根据日志检查上一次的同步数据量
并根据一个系数来计算这一次触发时该节点是否要执行
然后通过IPC来通知c++进行更新
对于有前后依赖的sql节点应当放到c++的处理中执行

实际计算同步时间的方法是在节点的定义里，可以重写
'''

SOCKET_DEBUG = SYNC_CONFIG["socket_debug"]
SOCKET_IP = SYNC_CONFIG["socket_ip"]
SOCKET_PORT = SYNC_CONFIG["socket_port"]


class scheduler:
    '''调度器，用于调度sql节点的执行'''
    def __init__(self, node_list: list[node_base]):
        self.node_list = node_list
        self.socket_server = socket.socket()
        self.socket_server.bind((SOCKET_IP, SOCKET_PORT))
        self.LOG = node_logger("scheduler")
        if SOCKET_DEBUG:
            self.LOG.info("服务端已开始监听，正在等待客户端连接...")
            try:
                self.conn, address = self.socket_server.accept()
            except Exception as me:
                self.LOG.error(me)
            self.LOG.info(f"接收到了客户端的连接，客户端的信息：{address}")
    
    def __del__(self):
        self.conn.close()
        self.socket_server.close()

    def run_node(self) -> None:
        '''真正执行节点的地方'''
        with ThreadPoolExecutor() as tpool:
            # 死循环，不退出
            while True:
                total_tasks = []
                run_node = self.get_node_run()
                if len(run_node) == 0:
                    # 没任务运行就等10秒，不要让cpu一直卡在检查
                    self.LOG.info("没有任务运行，等待10秒")
                    time.sleep(10)
                else:
                    # 只有在本次获取的任务都执行完成后下一次执行才会开始
                    for ch in run_node:
                        total_tasks.append(tpool.submit(ch.run))
                    for future in as_completed(total_tasks):
                        self.notify(future.result())
                
    def get_node_run(self) -> list[node_base]:
        '''获取当前需要执行的节点'''
        temp_node = []
        temp_now = datetime.datetime.now()
        for _node in self.node_list:
            if _node.last_time <= temp_now:
                temp_node.append(_node)
        return temp_node

    def notify(self, name: str)-> None:
        '''
        在节点运行完成之后
        使用socket通知处理程序
        '''
        if SOCKET_DEBUG:
            msg = "节点：" + name + "已经执行完成。"
            self.LOG.info(msg)
            self.conn.send(msg.encode("UTF-8"))